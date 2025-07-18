#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
综合修复验证测试

合并了原有的多个修复验证测试文件，提供完整的修复验证覆盖：
1. 第二阶段实时存储修复验证
2. SQLAlchemy UPSERT错误修复验证
3. IntegrityError修复验证
4. 数据库并发访问修复验证
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
import threading
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch, AsyncMock

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database.base import initialize_database
from src.core.database.repository import SQLiteRepository


class TestComprehensiveFixVerification:
    """综合修复验证测试类"""
    
    @pytest.fixture
    def temp_db_dir(self):
        """创建临时数据库目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def repository(self, temp_db_dir):
        """创建测试用的Repository实例"""
        return SQLiteRepository(keyword="test_keyword", output_dir=temp_db_dir)
    
    def test_sqlalchemy_upsert_fix(self, repository):
        """测试SQLAlchemy UPSERT错误修复"""
        # 创建测试Pin数据
        pin_data = {
            'id': 'test_pin_123',
            'title': 'Test Pin',
            'description': 'Test Description',
            'url': 'https://pinterest.com/pin/123',
            'image_url': 'https://example.com/image.jpg',
            'query': 'test_keyword',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        # 第一次保存
        result1 = repository.save_pin(pin_data)
        assert result1 is True
        
        # 修改数据后再次保存（测试UPSERT）
        pin_data['title'] = 'Updated Test Pin'
        pin_data['updated_at'] = datetime.now()
        
        result2 = repository.save_pin(pin_data)
        assert result2 is True
        
        # 验证数据被正确更新
        saved_pin = repository.get_pin_by_id('test_pin_123')
        assert saved_pin is not None
        assert saved_pin['title'] == 'Updated Test Pin'
    
    def test_concurrent_database_access(self, repository):
        """测试并发数据库访问修复"""
        def worker_function(worker_id):
            """工作线程函数"""
            results = []
            for i in range(10):
                pin_data = {
                    'id': f'pin_{worker_id}_{i}',
                    'title': f'Pin from worker {worker_id}',
                    'description': f'Description {i}',
                    'url': f'https://pinterest.com/pin/{worker_id}_{i}',
                    'image_url': f'https://example.com/image_{worker_id}_{i}.jpg',
                    'query': 'test_keyword',
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                try:
                    result = repository.save_pin(pin_data)
                    results.append(('success', result))
                except Exception as e:
                    results.append(('error', str(e)))
                
                time.sleep(0.01)  # 短暂延迟增加竞态条件概率
            
            return results
        
        # 使用多线程并发访问数据库
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker_function, i) for i in range(5)]
            all_results = []
            
            for future in futures:
                worker_results = future.result()
                all_results.extend(worker_results)
        
        # 验证所有操作都成功
        success_count = sum(1 for status, _ in all_results if status == 'success')
        error_count = sum(1 for status, _ in all_results if status == 'error')
        
        assert success_count == 50  # 5个工作线程 × 10次操作
        assert error_count == 0
    
    def test_integrity_error_prevention(self, repository):
        """测试IntegrityError预防机制"""
        # 创建相同ID的Pin数据
        pin_data_1 = {
            'id': 'duplicate_pin_123',
            'title': 'First Pin',
            'description': 'First Description',
            'url': 'https://pinterest.com/pin/123',
            'image_url': 'https://example.com/image1.jpg',
            'query': 'test_keyword',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        pin_data_2 = {
            'id': 'duplicate_pin_123',  # 相同ID
            'title': 'Second Pin',
            'description': 'Second Description',
            'url': 'https://pinterest.com/pin/456',
            'image_url': 'https://example.com/image2.jpg',
            'query': 'test_keyword',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        # 保存第一个Pin
        result1 = repository.save_pin(pin_data_1)
        assert result1 is True
        
        # 保存相同ID的Pin（应该更新而不是报错）
        result2 = repository.save_pin(pin_data_2)
        assert result2 is True
        
        # 验证数据被正确更新
        saved_pin = repository.get_pin_by_id('duplicate_pin_123')
        assert saved_pin is not None
        assert saved_pin['title'] == 'Second Pin'
    
    def test_download_task_upsert_fix(self, repository):
        """测试DownloadTask UPSERT修复"""
        # 创建下载任务
        task_id = repository.create_download_task(
            pin_id='test_pin_456',
            image_url='https://example.com/test_image.jpg'
        )
        assert task_id is not None
        
        # 更新任务状态
        result = repository.update_download_task_status(task_id, 'completed')
        assert result is True
        
        # 再次更新任务状态
        result = repository.update_download_task_status(task_id, 'failed')
        assert result is True
        
        # 验证任务状态被正确更新
        task = repository.get_download_task_by_id(task_id)
        assert task is not None
        assert task['status'] == 'failed'
    
    def test_batch_save_atomicity(self, repository):
        """测试批量保存的原子性"""
        # 创建多个Pin数据
        pins_data = []
        for i in range(20):
            pin_data = {
                'id': f'batch_pin_{i}',
                'title': f'Batch Pin {i}',
                'description': f'Batch Description {i}',
                'url': f'https://pinterest.com/pin/batch_{i}',
                'image_url': f'https://example.com/batch_{i}.jpg',
                'query': 'test_keyword',
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            pins_data.append(pin_data)
        
        # 批量保存
        success_count = 0
        for pin_data in pins_data:
            if repository.save_pin(pin_data):
                success_count += 1
        
        # 验证所有Pin都被成功保存
        assert success_count == 20
        
        # 验证数据库中确实有20个Pin
        all_pins = repository.load_pins_by_query('test_keyword', limit=25)
        batch_pins = [pin for pin in all_pins if pin['id'].startswith('batch_pin_')]
        assert len(batch_pins) == 20
    
    def test_database_connection_recovery(self, repository):
        """测试数据库连接恢复机制"""
        # 正常操作
        pin_data = {
            'id': 'recovery_test_pin',
            'title': 'Recovery Test Pin',
            'description': 'Test Description',
            'url': 'https://pinterest.com/pin/recovery',
            'image_url': 'https://example.com/recovery.jpg',
            'query': 'test_keyword',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        result1 = repository.save_pin(pin_data)
        assert result1 is True
        
        # 模拟连接中断后的恢复
        # 这里我们通过创建新的repository实例来模拟连接恢复
        new_repository = SQLiteRepository(
            keyword="test_keyword", 
            output_dir=repository.output_dir
        )
        
        # 验证数据仍然可以访问
        saved_pin = new_repository.get_pin_by_id('recovery_test_pin')
        assert saved_pin is not None
        assert saved_pin['title'] == 'Recovery Test Pin'
        
        # 验证新连接可以正常工作
        pin_data['title'] = 'Updated Recovery Test Pin'
        result2 = new_repository.save_pin(pin_data)
        assert result2 is True
    
    def test_transaction_rollback_on_error(self, repository):
        """测试错误时的事务回滚"""
        # 这个测试验证在发生错误时事务能够正确回滚
        initial_count = len(repository.load_pins_by_query('test_keyword', limit=1000))
        
        # 尝试保存无效数据（缺少必需字段）
        invalid_pin_data = {
            'id': 'invalid_pin',
            # 缺少必需的字段
        }
        
        try:
            repository.save_pin(invalid_pin_data)
        except Exception:
            pass  # 预期会出错
        
        # 验证数据库状态没有改变
        final_count = len(repository.load_pins_by_query('test_keyword', limit=1000))
        assert final_count == initial_count
        
        # 验证后续正常操作仍然可以工作
        valid_pin_data = {
            'id': 'valid_pin_after_error',
            'title': 'Valid Pin',
            'description': 'Valid Description',
            'url': 'https://pinterest.com/pin/valid',
            'image_url': 'https://example.com/valid.jpg',
            'query': 'test_keyword',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        result = repository.save_pin(valid_pin_data)
        assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
