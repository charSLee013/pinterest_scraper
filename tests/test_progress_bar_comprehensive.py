#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
进度条综合测试

合并了原有的进度条测试文件，提供完整的进度条测试覆盖：
1. 进度条精确性验证
2. 进度条一致性测试
3. 第一阶段和第二阶段进度条逻辑验证
4. 数据持久化状态反映测试
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock, call
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.pinterest_scraper import PinterestScraper
from src.core.database.repository import SQLiteRepository


class TestProgressBarComprehensive:
    """进度条综合测试类"""
    
    @pytest.fixture
    def temp_output_dir(self):
        """创建临时输出目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def mock_scraper(self, temp_output_dir):
        """创建模拟的Pinterest爬虫实例"""
        return PinterestScraper(
            output_dir=temp_output_dir,
            download_images=False,  # 禁用图片下载以专注于进度条测试
            max_concurrent=2
        )
    
    @pytest.fixture
    def repository(self, temp_output_dir):
        """创建测试用的Repository实例"""
        return SQLiteRepository(keyword="test_keyword", output_dir=temp_output_dir)
    
    def create_sample_pins(self, count=10):
        """创建示例Pin数据"""
        pins = []
        for i in range(count):
            pin = {
                'id': f'pin_{i}',
                'title': f'Test Pin {i}',
                'description': f'Test Description {i}',
                'url': f'https://pinterest.com/pin/{i}',
                'image_url': f'https://example.com/image_{i}.jpg',
                'query': 'test_keyword',
                'created_at': '2024-01-01T00:00:00Z',
                'updated_at': '2024-01-01T00:00:00Z'
            }
            pins.append(pin)
        return pins
    
    @pytest.mark.asyncio
    async def test_progress_bar_accuracy_phase1(self, mock_scraper, repository):
        """测试第一阶段进度条精确性"""
        # 创建测试数据
        sample_pins = self.create_sample_pins(20)
        
        # 模拟第一阶段数据采集过程
        saved_count = 0
        progress_updates = []
        
        def mock_progress_callback(current, total, description=""):
            progress_updates.append((current, total, description))
        
        # 模拟逐个保存Pin的过程
        for i, pin in enumerate(sample_pins):
            result = repository.save_pin(pin)
            if result:
                saved_count += 1
            
            # 模拟进度条更新
            mock_progress_callback(saved_count, len(sample_pins), f"保存Pin {i+1}")
        
        # 验证进度条更新的准确性
        assert len(progress_updates) == 20
        
        # 验证最后一次更新显示完整进度
        final_update = progress_updates[-1]
        assert final_update[0] == 20  # current
        assert final_update[1] == 20  # total
        
        # 验证数据库中实际保存的数量与进度条一致
        actual_pins = repository.load_pins_by_query('test_keyword', limit=25)
        assert len(actual_pins) == saved_count
        assert len(actual_pins) == final_update[0]
    
    @pytest.mark.asyncio
    async def test_progress_bar_accuracy_phase2(self, repository):
        """测试第二阶段进度条精确性"""
        # 先保存一些Pin数据
        sample_pins = self.create_sample_pins(15)
        for pin in sample_pins:
            repository.save_pin(pin)
        
        # 模拟第二阶段处理过程
        processed_count = 0
        progress_updates = []
        
        def mock_progress_callback(current, total, description=""):
            progress_updates.append((current, total, description))
        
        # 模拟逐个处理Pin的过程
        all_pins = repository.load_pins_by_query('test_keyword', limit=20)
        for i, pin in enumerate(all_pins):
            # 模拟处理逻辑（例如增强Pin数据）
            pin['enhanced'] = True
            result = repository.save_pin(pin)
            if result:
                processed_count += 1
            
            # 模拟进度条更新
            mock_progress_callback(processed_count, len(all_pins), f"处理Pin {i+1}")
        
        # 验证进度条更新的准确性
        assert len(progress_updates) == 15
        
        # 验证最后一次更新显示完整进度
        final_update = progress_updates[-1]
        assert final_update[0] == 15  # current
        assert final_update[1] == 15  # total
        
        # 验证数据库中实际处理的数量与进度条一致
        enhanced_pins = [pin for pin in repository.load_pins_by_query('test_keyword', limit=20) 
                        if pin.get('enhanced')]
        assert len(enhanced_pins) == processed_count
        assert len(enhanced_pins) == final_update[0]
    
    @pytest.mark.asyncio
    async def test_progress_bar_consistency_between_phases(self, repository):
        """测试第一阶段和第二阶段进度条逻辑一致性"""
        # 第一阶段：保存Pin数据
        phase1_pins = self.create_sample_pins(12)
        phase1_progress = []
        
        saved_count = 0
        for i, pin in enumerate(phase1_pins):
            result = repository.save_pin(pin)
            if result:
                saved_count += 1
            phase1_progress.append((saved_count, len(phase1_pins)))
        
        # 第二阶段：处理Pin数据
        phase2_progress = []
        processed_count = 0
        
        all_pins = repository.load_pins_by_query('test_keyword', limit=15)
        for i, pin in enumerate(all_pins):
            pin['processed'] = True
            result = repository.save_pin(pin)
            if result:
                processed_count += 1
            phase2_progress.append((processed_count, len(all_pins)))
        
        # 验证两个阶段的进度条逻辑一致性
        # 1. 都从0开始计数
        assert phase1_progress[0][0] >= 0
        assert phase2_progress[0][0] >= 0
        
        # 2. 最终计数都等于总数
        assert phase1_progress[-1][0] == phase1_progress[-1][1]
        assert phase2_progress[-1][0] == phase2_progress[-1][1]
        
        # 3. 计数都是单调递增的
        for i in range(1, len(phase1_progress)):
            assert phase1_progress[i][0] >= phase1_progress[i-1][0]
        
        for i in range(1, len(phase2_progress)):
            assert phase2_progress[i][0] >= phase2_progress[i-1][0]
    
    @pytest.mark.asyncio
    async def test_progress_bar_error_handling(self, repository):
        """测试进度条在错误情况下的处理"""
        sample_pins = self.create_sample_pins(10)
        progress_updates = []
        error_count = 0
        success_count = 0
        
        def mock_progress_callback(current, total, description="", error=False):
            progress_updates.append((current, total, description, error))
        
        # 模拟部分保存失败的情况
        for i, pin in enumerate(sample_pins):
            try:
                # 模拟第5个Pin保存失败
                if i == 4:
                    raise Exception("模拟保存失败")
                
                result = repository.save_pin(pin)
                if result:
                    success_count += 1
                    mock_progress_callback(success_count, len(sample_pins), f"保存成功 {i+1}")
                
            except Exception:
                error_count += 1
                mock_progress_callback(success_count, len(sample_pins), f"保存失败 {i+1}", error=True)
        
        # 验证进度条正确反映了成功和失败的情况
        assert len(progress_updates) == 10
        assert success_count == 9  # 10个Pin中有1个失败
        assert error_count == 1
        
        # 验证数据库中实际保存的数量
        actual_pins = repository.load_pins_by_query('test_keyword', limit=15)
        assert len(actual_pins) == success_count
    
    @pytest.mark.asyncio
    async def test_progress_bar_concurrent_updates(self, repository):
        """测试并发情况下的进度条更新"""
        import threading
        import time
        
        sample_pins = self.create_sample_pins(20)
        progress_updates = []
        lock = threading.Lock()
        success_count = 0
        
        def mock_progress_callback(current, total, description=""):
            with lock:
                progress_updates.append((current, total, description))
        
        def worker_function(pins_batch, worker_id):
            nonlocal success_count
            for i, pin in enumerate(pins_batch):
                pin['id'] = f"{pin['id']}_worker_{worker_id}"  # 确保ID唯一
                result = repository.save_pin(pin)
                if result:
                    with lock:
                        success_count += 1
                        mock_progress_callback(success_count, len(sample_pins), 
                                             f"Worker {worker_id} saved pin {i+1}")
                time.sleep(0.01)  # 模拟处理时间
        
        # 将Pin分成两批，用两个线程并发处理
        batch1 = sample_pins[:10]
        batch2 = sample_pins[10:]
        
        thread1 = threading.Thread(target=worker_function, args=(batch1, 1))
        thread2 = threading.Thread(target=worker_function, args=(batch2, 2))
        
        thread1.start()
        thread2.start()
        
        thread1.join()
        thread2.join()
        
        # 验证并发情况下进度条更新的正确性
        assert len(progress_updates) == 20
        assert success_count == 20
        
        # 验证最终进度
        final_progress = max(progress_updates, key=lambda x: x[0])
        assert final_progress[0] == 20
        assert final_progress[1] == 20
        
        # 验证数据库中实际保存的数量
        actual_pins = repository.load_pins_by_query('test_keyword', limit=25)
        assert len(actual_pins) == success_count


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
