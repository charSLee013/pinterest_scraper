"""
下载任务管理器测试
"""

import pytest
import asyncio
import tempfile
import os
from unittest.mock import AsyncMock, patch, MagicMock

from src.core.download.task_manager import DownloadTaskManager
from src.core.database.base import initialize_database
from src.core.database.repository import SQLiteRepository


class TestDownloadTaskManager:
    """下载任务管理器测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_task_manager(self):
        """设置测试任务管理器"""
        # 创建临时数据库
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        
        # 初始化数据库
        self.db_manager = initialize_database(self.temp_db.name)
        
        # 创建任务管理器
        self.task_manager = DownloadTaskManager(max_concurrent=2, auto_start=False)
        
        yield
        
        # 清理
        try:
            if hasattr(self.db_manager, 'engine'):
                self.db_manager.engine.dispose()
            os.unlink(self.temp_db.name)
        except (PermissionError, FileNotFoundError):
            pass
    
    @pytest.mark.asyncio
    async def test_task_manager_start_stop(self):
        """测试任务管理器启动和停止"""
        # 测试启动
        await self.task_manager.start()
        assert self.task_manager.started == True
        assert self.task_manager.downloader.running == True
        
        # 测试停止
        await self.task_manager.stop()
        assert self.task_manager.started == False
        assert self.task_manager.downloader.running == False
    
    @pytest.mark.asyncio
    async def test_schedule_pin_downloads(self):
        """测试为Pin调度下载任务"""
        # 先创建一些Pin数据和下载任务
        repository = SQLiteRepository()
        
        # 创建测试Pin数据
        test_pins = [
            {
                'id': 'pin_1',
                'largest_image_url': 'https://example.com/1.jpg'
            },
            {
                'id': 'pin_2',
                'largest_image_url': 'https://example.com/2.jpg'
            }
        ]
        
        # 保存Pin数据到数据库（这会自动创建下载任务）
        repository.save_pins_batch(test_pins, 'test_query')
        
        # 启动任务管理器
        await self.task_manager.start()
        
        # 调度下载任务
        with tempfile.TemporaryDirectory() as temp_dir:
            scheduled_count = await self.task_manager.schedule_pin_downloads(test_pins, temp_dir)
            
            # 验证调度结果
            assert scheduled_count == 2
        
        await self.task_manager.stop()
    
    @pytest.mark.asyncio
    async def test_process_pending_tasks(self):
        """测试处理待下载任务"""
        # 创建一些待下载任务
        repository = SQLiteRepository()
        
        test_pins = [
            {
                'id': 'pending_pin_1',
                'largest_image_url': 'https://example.com/pending1.jpg'
            }
        ]
        
        repository.save_pins_batch(test_pins, 'pending_test')
        
        # 启动任务管理器（会自动处理待下载任务）
        await self.task_manager.start()
        
        # 验证下载器接收到了任务
        stats = self.task_manager.get_download_stats()
        assert stats['total_tasks'] >= 1
        
        await self.task_manager.stop()
    
    def test_generate_output_path(self):
        """测试输出路径生成"""
        pin = {
            'id': 'test_pin_123',
            'largest_image_url': 'https://example.com/image.jpg'
        }
        
        output_dir = '/test/output'
        output_path = self.task_manager._generate_output_path(pin, output_dir)
        
        expected_path = os.path.join(output_dir, 'images', 'test_pin_123.jpg')
        assert output_path == expected_path
    
    def test_generate_output_path_with_extension(self):
        """测试带扩展名的输出路径生成"""
        pin = {
            'id': 'test_pin_png',
            'largest_image_url': 'https://example.com/image.png'
        }
        
        output_dir = '/test/output'
        output_path = self.task_manager._generate_output_path(pin, output_dir)
        
        expected_path = os.path.join(output_dir, 'images', 'test_pin_png.png')
        assert output_path == expected_path
    
    def test_generate_output_path_no_extension(self):
        """测试无扩展名的输出路径生成"""
        pin = {
            'id': 'test_pin_no_ext',
            'largest_image_url': 'https://example.com/image'
        }
        
        output_dir = '/test/output'
        output_path = self.task_manager._generate_output_path(pin, output_dir)
        
        expected_path = os.path.join(output_dir, 'images', 'test_pin_no_ext.jpg')
        assert output_path == expected_path
    
    def test_generate_output_path_from_task(self):
        """测试从任务生成输出路径"""
        task = {
            'pin_id': 'task_pin_123',
            'image_url': 'https://example.com/task.webp'
        }
        
        output_path = self.task_manager._generate_output_path_from_task(task)
        
        expected_path = os.path.join('output', 'images', 'task_pin_123.webp')
        assert output_path == expected_path
    
    @pytest.mark.asyncio
    async def test_auto_start_feature(self):
        """测试自动启动功能"""
        # 创建启用自动启动的任务管理器
        auto_task_manager = DownloadTaskManager(max_concurrent=1, auto_start=True)
        
        try:
            # 调度下载任务应该自动启动管理器
            test_pins = [
                {
                    'id': 'auto_pin',
                    'largest_image_url': 'https://example.com/auto.jpg'
                }
            ]
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # 这应该触发自动启动
                await auto_task_manager.schedule_pin_downloads(test_pins, temp_dir)
                
                # 验证管理器已启动
                assert auto_task_manager.started == True
        
        finally:
            await auto_task_manager.stop()
    
    @pytest.mark.asyncio
    async def test_wait_for_completion(self):
        """测试等待下载完成"""
        await self.task_manager.start()
        
        # 测试无任务时的等待
        await self.task_manager.wait_for_completion(timeout=1.0)
        
        await self.task_manager.stop()
    
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """测试异步上下文管理器"""
        async with DownloadTaskManager(max_concurrent=1) as manager:
            assert manager.started == True
        
        # 退出上下文后应该自动停止
        assert manager.started == False
    
    @pytest.mark.asyncio
    async def test_get_download_stats(self):
        """测试获取下载统计信息"""
        await self.task_manager.start()
        
        stats = self.task_manager.get_download_stats()
        
        # 验证统计信息结构
        assert 'total_tasks' in stats
        assert 'completed' in stats
        assert 'failed' in stats
        assert 'in_progress' in stats
        
        await self.task_manager.stop()
    
    @pytest.mark.asyncio
    async def test_schedule_empty_pins(self):
        """测试调度空Pin列表"""
        await self.task_manager.start()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            scheduled_count = await self.task_manager.schedule_pin_downloads([], temp_dir)
            assert scheduled_count == 0
        
        await self.task_manager.stop()
    
    @pytest.mark.asyncio
    async def test_schedule_pins_without_image_url(self):
        """测试调度没有图片URL的Pin"""
        await self.task_manager.start()
        
        test_pins = [
            {
                'id': 'no_image_pin',
                # 没有largest_image_url字段
            }
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            scheduled_count = await self.task_manager.schedule_pin_downloads(test_pins, temp_dir)
            assert scheduled_count == 0
        
        await self.task_manager.stop()
