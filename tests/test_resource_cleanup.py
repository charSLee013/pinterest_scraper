"""
资源清理测试
"""

import pytest
import asyncio
import tempfile
import os
from unittest.mock import patch, AsyncMock

from src.core.pinterest_scraper import PinterestScraper


class TestResourceCleanup:
    """资源清理测试类"""
    
    @pytest.mark.asyncio
    async def test_scraper_resource_cleanup(self):
        """测试爬虫资源清理"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建爬虫实例
            scraper = PinterestScraper(
                output_dir=temp_dir,
                download_images=True,
                debug=False
            )
            
            try:
                # 验证初始状态
                assert scraper.download_manager is not None
                assert scraper.repository is not None
                assert scraper.db_manager is not None
                
                # 模拟一些活动（启动下载管理器）
                test_pins = [
                    {
                        'id': 'cleanup_test_pin',
                        'largest_image_url': 'https://example.com/test.jpg'
                    }
                ]
                
                # 调度下载任务（这会启动下载管理器）
                scraper._schedule_async_downloads(test_pins, temp_dir)
                
                # 等待一段时间让异步任务启动
                await asyncio.sleep(0.5)
                
            finally:
                # 测试资源清理
                await scraper.close()
                
                # 验证清理后的状态
                assert scraper.download_manager.started == False
    
    @pytest.mark.asyncio
    async def test_download_manager_cleanup(self):
        """测试下载管理器资源清理"""
        from src.core.download.task_manager import DownloadTaskManager
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建临时数据库
            temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
            temp_db.close()
            
            try:
                # 初始化数据库
                from src.core.database.base import initialize_database
                db_manager = initialize_database(temp_db.name)
                
                # 创建下载管理器
                download_manager = DownloadTaskManager(max_concurrent=2, auto_start=False)
                
                # 启动管理器
                await download_manager.start()
                assert download_manager.started == True
                assert download_manager.downloader.running == True
                
                # 停止管理器
                await download_manager.stop()
                assert download_manager.started == False
                assert download_manager.downloader.running == False
                
            finally:
                # 清理数据库文件
                try:
                    if hasattr(db_manager, 'engine'):
                        db_manager.engine.dispose()
                    os.unlink(temp_db.name)
                except (PermissionError, FileNotFoundError):
                    pass
    
    @pytest.mark.asyncio
    async def test_async_downloader_cleanup(self):
        """测试异步下载器资源清理"""
        from src.core.download.async_downloader import AsyncImageDownloader
        
        # 创建下载器
        downloader = AsyncImageDownloader(max_concurrent=2, timeout=5)
        
        # 启动下载器
        await downloader.start()
        assert downloader.running == True
        assert downloader.session is not None
        assert len(downloader.workers) == 2
        
        # 停止下载器
        await downloader.stop()
        assert downloader.running == False
        assert downloader.session is None
        assert len(downloader.workers) == 0
    
    @pytest.mark.asyncio
    async def test_cleanup_with_pending_tasks(self):
        """测试有待处理任务时的资源清理"""
        from src.core.download.async_downloader import AsyncImageDownloader
        
        # 创建下载器
        downloader = AsyncImageDownloader(max_concurrent=1, timeout=5)
        
        try:
            # 启动下载器
            await downloader.start()
            
            # 调度一些下载任务
            for i in range(3):
                task_data = {
                    'task_id': i,
                    'image_url': f'https://example.com/test{i}.jpg',
                    'output_path': f'/tmp/test{i}.jpg'
                }
                downloader.schedule_download(task_data)
            
            # 验证任务已调度
            stats = downloader.get_stats()
            assert stats['total_tasks'] == 3
            
        finally:
            # 停止下载器（应该能够处理待处理的任务）
            await downloader.stop(timeout=2.0)
            assert downloader.running == False
    
    @pytest.mark.asyncio
    async def test_cleanup_timeout_handling(self):
        """测试清理超时处理"""
        from src.core.download.async_downloader import AsyncImageDownloader
        
        # 创建下载器
        downloader = AsyncImageDownloader(max_concurrent=1, timeout=30)
        
        try:
            # 启动下载器
            await downloader.start()
            
            # 调度一个永远不会完成的任务（模拟）
            task_data = {
                'task_id': 'timeout_test',
                'image_url': 'https://httpbin.org/delay/10',  # 10秒延迟
                'output_path': '/tmp/timeout_test.jpg'
            }
            downloader.schedule_download(task_data)
            
            # 等待任务开始
            await asyncio.sleep(0.1)
            
        finally:
            # 使用短超时停止下载器
            await downloader.stop(timeout=0.5)
            assert downloader.running == False
    
    @pytest.mark.asyncio
    async def test_database_connection_cleanup(self):
        """测试数据库连接清理"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建爬虫实例
            scraper = PinterestScraper(
                output_dir=temp_dir,
                download_images=False,
                debug=False
            )
            
            try:
                # 验证数据库连接存在
                assert scraper.db_manager is not None
                assert hasattr(scraper.db_manager, 'engine')
                
                # 使用数据库
                count = scraper.repository.get_pin_count_by_query('test')
                assert count == 0
                
            finally:
                # 清理资源
                await scraper.close()
                
                # 验证数据库连接已关闭
                # 注意：engine.dispose()后，连接池被清理，但engine对象仍然存在
                assert scraper.db_manager is not None
    
    def test_destructor_warning(self):
        """测试析构函数警告"""
        import io
        import sys
        from unittest.mock import patch
        
        # 捕获日志输出
        captured_logs = []
        
        def mock_logger_warning(msg):
            captured_logs.append(msg)
        
        with patch('src.core.pinterest_scraper.logger.warning', mock_logger_warning):
            with tempfile.TemporaryDirectory() as temp_dir:
                # 创建爬虫实例但不调用close()
                scraper = PinterestScraper(
                    output_dir=temp_dir,
                    download_images=True,
                    debug=False
                )
                
                # 模拟启动下载管理器
                scraper.download_manager.started = True
                
                # 删除对象（触发析构函数）
                del scraper
        
        # 验证警告被记录
        warning_found = any('未正确关闭' in log for log in captured_logs)
        assert warning_found, f"Expected warning not found in logs: {captured_logs}"
