"""
异步图片下载器测试
"""

import pytest
import asyncio
import tempfile
import os
import aiohttp
from unittest.mock import AsyncMock, patch, MagicMock

from src.core.download.async_downloader import AsyncImageDownloader
from src.core.database.base import initialize_database


class TestAsyncImageDownloader:
    """异步图片下载器测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_downloader(self):
        """设置测试下载器"""
        # 创建临时数据库
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        
        # 初始化数据库
        self.db_manager = initialize_database(self.temp_db.name)
        
        # 创建下载器
        self.downloader = AsyncImageDownloader(max_concurrent=2, timeout=5)
        
        yield
        
        # 清理
        try:
            if hasattr(self.db_manager, 'engine'):
                self.db_manager.engine.dispose()
            os.unlink(self.temp_db.name)
        except (PermissionError, FileNotFoundError):
            pass
    
    @pytest.mark.asyncio
    async def test_downloader_start_stop(self):
        """测试下载器启动和停止"""
        # 测试启动
        await self.downloader.start()
        assert self.downloader.running == True
        assert self.downloader.session is not None
        assert len(self.downloader.workers) == 2
        
        # 测试停止
        await self.downloader.stop()
        assert self.downloader.running == False
        assert self.downloader.session is None
    
    @pytest.mark.asyncio
    async def test_schedule_download(self):
        """测试调度下载任务"""
        await self.downloader.start()
        
        # 调度一个下载任务
        task_data = {
            'task_id': 1,
            'image_url': 'https://example.com/test.jpg',
            'output_path': '/tmp/test.jpg'
        }
        
        self.downloader.schedule_download(task_data)
        
        # 验证统计信息
        stats = self.downloader.get_stats()
        assert stats['total_tasks'] == 1
        
        await self.downloader.stop()
    
    @pytest.mark.asyncio
    async def test_batch_schedule_downloads(self):
        """测试批量调度下载任务"""
        await self.downloader.start()
        
        # 批量调度下载任务
        tasks = [
            {
                'task_id': i,
                'image_url': f'https://example.com/test{i}.jpg',
                'output_path': f'/tmp/test{i}.jpg'
            }
            for i in range(5)
        ]
        
        self.downloader.schedule_downloads_batch(tasks)
        
        # 验证统计信息
        stats = self.downloader.get_stats()
        assert stats['total_tasks'] == 5
        
        await self.downloader.stop()
    
    @pytest.mark.asyncio
    async def test_download_worker_lifecycle(self):
        """测试下载工作协程生命周期"""
        await self.downloader.start()
        
        # 验证工作协程已启动
        assert len(self.downloader.workers) == 2
        for worker in self.downloader.workers:
            assert not worker.done()
        
        await self.downloader.stop()
        
        # 验证工作协程已停止
        for worker in self.downloader.workers:
            assert worker.done()
    
    @pytest.mark.asyncio
    async def test_download_image_success(self):
        """测试成功下载图片"""
        # 创建临时输出目录
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'test.jpg')

            # 创建异步迭代器Mock
            async def mock_iter_chunked(size):
                yield b'fake_image_data'

            # Mock HTTP响应
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.content.iter_chunked = mock_iter_chunked

            # Mock aiofiles
            with patch('aiofiles.open', create=True) as mock_open:
                mock_file = AsyncMock()
                mock_open.return_value.__aenter__.return_value = mock_file

                # Mock os.path.getsize
                with patch('os.path.getsize', return_value=15):
                    # Mock session.get
                    with patch.object(self.downloader, 'session') as mock_session:
                        mock_session.get.return_value.__aenter__.return_value = mock_response

                        success, file_size, error_msg = await self.downloader._download_image(
                            'https://example.com/test.jpg',
                            output_path,
                            'test-worker'
                        )

                        assert success == True
                        assert file_size == 15
                        assert error_msg is None
    
    @pytest.mark.asyncio
    async def test_download_image_http_error(self):
        """测试HTTP错误处理"""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'test.jpg')
            
            # Mock HTTP错误响应
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.reason = 'Not Found'
            
            with patch.object(self.downloader, 'session') as mock_session:
                mock_session.get.return_value.__aenter__.return_value = mock_response
                
                success, file_size, error_msg = await self.downloader._download_image(
                    'https://example.com/notfound.jpg',
                    output_path,
                    'test-worker'
                )
                
                assert success == False
                assert file_size is None
                assert 'HTTP 404' in error_msg
    
    @pytest.mark.asyncio
    async def test_download_image_timeout(self):
        """测试下载超时处理"""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'test.jpg')
            
            # Mock超时异常
            with patch.object(self.downloader, 'session') as mock_session:
                mock_session.get.side_effect = asyncio.TimeoutError()
                
                success, file_size, error_msg = await self.downloader._download_image(
                    'https://example.com/timeout.jpg',
                    output_path,
                    'test-worker'
                )
                
                assert success == False
                assert file_size is None
                assert error_msg == '下载超时'
    
    @pytest.mark.asyncio
    async def test_download_existing_file(self):
        """测试跳过已存在的文件"""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, 'existing.jpg')
            
            # 创建已存在的文件
            with open(output_path, 'wb') as f:
                f.write(b'existing_data')
            
            success, file_size, error_msg = await self.downloader._download_image(
                'https://example.com/existing.jpg',
                output_path,
                'test-worker'
            )
            
            assert success == True
            assert file_size == 13  # len(b'existing_data')
            assert error_msg is None
    
    @pytest.mark.asyncio
    async def test_download_stats(self):
        """测试下载统计信息"""
        stats = self.downloader.get_stats()
        
        # 验证初始统计信息
        assert stats['total_tasks'] == 0
        assert stats['completed'] == 0
        assert stats['failed'] == 0
        assert stats['in_progress'] == 0
        assert stats['start_time'] is None
        
        # 启动下载器后验证统计信息
        await self.downloader.start()
        stats = self.downloader.get_stats()
        assert stats['start_time'] is not None
        assert 'running_time' in stats
        
        await self.downloader.stop()
    
    @pytest.mark.asyncio
    async def test_wait_for_completion(self):
        """测试等待下载完成"""
        await self.downloader.start()
        
        # 测试无任务时的等待
        await self.downloader.wait_for_completion(timeout=1.0)
        
        await self.downloader.stop()
    
    @pytest.mark.asyncio
    async def test_wait_for_completion_timeout(self):
        """测试等待下载完成超时"""
        await self.downloader.start()
        
        # 添加一个永远不会完成的任务（模拟）
        # 这里我们不实际添加任务，只是测试超时机制
        await self.downloader.wait_for_completion(timeout=0.1)
        
        await self.downloader.stop()
    
    def test_downloader_not_started_warning(self):
        """测试下载器未启动时的警告"""
        # 在未启动状态下调度任务
        task_data = {
            'task_id': 1,
            'image_url': 'https://example.com/test.jpg',
            'output_path': '/tmp/test.jpg'
        }

        # 应该记录警告但不抛出异常
        self.downloader.schedule_download(task_data)

        # 统计信息不应该更新（因为任务未被实际调度）
        stats = self.downloader.get_stats()
        assert stats['total_tasks'] == 0

    @pytest.mark.asyncio
    async def test_repository_integration(self):
        """测试与Repository的集成"""
        # 这个测试验证下载器能正确调用Repository方法
        # 由于Repository已经在其他测试中验证，这里主要测试调用

        await self.downloader.start()

        # Mock Repository方法
        with patch.object(self.downloader.repository, 'update_download_task_status') as mock_update:
            # 模拟下载任务
            task_data = {
                'task_id': 1,
                'image_url': 'https://example.com/test.jpg',
                'output_path': '/tmp/test.jpg'
            }

            # 模拟成功下载
            with patch.object(self.downloader, '_download_image', return_value=(True, 1024, None)):
                await self.downloader._download_single_task(task_data, 'test-worker')

            # 验证Repository方法被调用
            assert mock_update.call_count >= 2  # 至少调用两次：downloading和completed

        await self.downloader.stop()
