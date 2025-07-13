"""
数据库模型测试
"""

import json
import pytest
import tempfile
import os
from datetime import datetime

from src.core.database.base import initialize_database, get_database_session
from src.core.database.schema import Pin, DownloadTask, ScrapingSession, CacheMetadata


class TestDatabaseModels:
    """数据库模型测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_database(self):
        """设置测试数据库"""
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()

        # 初始化数据库
        self.db_manager = initialize_database(self.temp_db.name)

        yield

        # 清理 - 先关闭数据库连接再删除文件
        try:
            if hasattr(self.db_manager, 'engine'):
                self.db_manager.engine.dispose()
            os.unlink(self.temp_db.name)
        except (PermissionError, FileNotFoundError):
            # 在Windows上可能出现文件锁定，忽略清理错误
            pass
    
    def test_pin_model_creation(self):
        """测试Pin模型创建"""
        with get_database_session() as session:
            # 创建测试Pin
            pin = Pin(
                id='test_pin_123',
                pin_hash='abcd1234567890',
                query='test query',
                title='Test Pin Title',
                description='Test Pin Description',
                creator_name='Test Creator',
                creator_id='creator_123',
                largest_image_url='https://example.com/image.jpg'
            )
            
            session.add(pin)
            session.commit()
            
            # 验证Pin已保存
            saved_pin = session.query(Pin).filter_by(id='test_pin_123').first()
            assert saved_pin is not None
            assert saved_pin.title == 'Test Pin Title'
            assert saved_pin.query == 'test query'
            assert saved_pin.pin_hash == 'abcd1234567890'
    
    def test_pin_json_properties(self):
        """测试Pin的JSON属性"""
        with get_database_session() as session:
            pin = Pin(
                id='test_pin_json',
                pin_hash='json1234567890',
                query='json test'
            )
            
            # 测试image_urls_dict属性
            test_urls = {'original': 'https://example.com/orig.jpg', '736': 'https://example.com/736.jpg'}
            pin.image_urls_dict = test_urls
            
            # 测试stats_dict属性
            test_stats = {'saves': 100, 'comments': 50}
            pin.stats_dict = test_stats
            
            # 测试raw_data_dict属性
            test_raw_data = {'id': 'test_pin_json', 'title': 'Test', 'extra_field': 'extra_value'}
            pin.raw_data_dict = test_raw_data
            
            session.add(pin)
            session.commit()
            
            # 重新查询验证
            saved_pin = session.query(Pin).filter_by(id='test_pin_json').first()
            assert saved_pin.image_urls_dict == test_urls
            assert saved_pin.stats_dict == test_stats
            assert saved_pin.raw_data_dict == test_raw_data
    
    def test_pin_to_dict(self):
        """测试Pin转换为字典"""
        with get_database_session() as session:
            # 创建Pin和关联的下载任务
            pin = Pin(
                id='test_dict_pin',
                pin_hash='dict1234567890',
                query='dict test',
                title='Dict Test Pin',
                creator_name='Dict Creator'
            )
            
            # 设置原始数据
            raw_data = {
                'id': 'test_dict_pin',
                'title': 'Dict Test Pin',
                'creator': {'name': 'Dict Creator'},
                'custom_field': 'custom_value'
            }
            pin.raw_data_dict = raw_data
            
            session.add(pin)
            session.commit()
            
            # 添加下载任务
            download_task = DownloadTask(
                pin_id='test_dict_pin',
                pin_hash='dict1234567890',
                image_url='https://example.com/test.jpg',
                status='completed',
                local_path='/path/to/image.jpg'
            )
            session.add(download_task)
            session.commit()
            
            # 重新查询Pin（包含关联的下载任务）
            saved_pin = session.query(Pin).filter_by(id='test_dict_pin').first()
            
            # 转换为字典
            pin_dict = saved_pin.to_dict()
            
            # 验证字典内容
            assert pin_dict['id'] == 'test_dict_pin'
            assert pin_dict['title'] == 'Dict Test Pin'
            assert pin_dict['custom_field'] == 'custom_value'
            assert pin_dict['downloaded'] == True
            assert pin_dict['download_path'] == '/path/to/image.jpg'
    
    def test_download_task_model(self):
        """测试DownloadTask模型"""
        with get_database_session() as session:
            # 先创建Pin
            pin = Pin(
                id='test_download_pin',
                pin_hash='download1234567890',
                query='download test'
            )
            session.add(pin)
            session.commit()
            
            # 创建下载任务
            task = DownloadTask(
                pin_id='test_download_pin',
                pin_hash='download1234567890',
                image_url='https://example.com/download.jpg',
                status='pending'
            )
            
            session.add(task)
            session.commit()
            
            # 验证任务已保存
            saved_task = session.query(DownloadTask).filter_by(pin_id='test_download_pin').first()
            assert saved_task is not None
            assert saved_task.status == 'pending'
            assert saved_task.retry_count == 0
            
            # 测试关联关系
            assert saved_task.pin is not None
            assert saved_task.pin.id == 'test_download_pin'
    
    def test_scraping_session_model(self):
        """测试ScrapingSession模型"""
        with get_database_session() as session:
            # 创建采集会话
            session_obj = ScrapingSession(
                id='test_session_123',
                query='session test',
                target_count=100,
                output_dir='/test/output',
                download_images=True,
                status='running'
            )
            
            # 设置统计信息
            stats = {'total_scrolls': 50, 'api_calls': 25}
            session_obj.stats_dict = stats
            
            session.add(session_obj)
            session.commit()
            
            # 验证会话已保存
            saved_session = session.query(ScrapingSession).filter_by(id='test_session_123').first()
            assert saved_session is not None
            assert saved_session.query == 'session test'
            assert saved_session.target_count == 100
            assert saved_session.stats_dict == stats
    
    def test_cache_metadata_model(self):
        """测试CacheMetadata模型"""
        with get_database_session() as session:
            # 创建缓存元数据
            cache_meta = CacheMetadata(
                query='cache test',
                pin_count=50,
                cache_version='1.0'
            )
            
            session.add(cache_meta)
            session.commit()
            
            # 验证元数据已保存
            saved_meta = session.query(CacheMetadata).filter_by(query='cache test').first()
            assert saved_meta is not None
            assert saved_meta.pin_count == 50
            assert saved_meta.cache_version == '1.0'
    
    def test_foreign_key_constraints(self):
        """测试外键约束"""
        with get_database_session() as session:
            # 创建Pin
            pin = Pin(
                id='fk_test_pin',
                pin_hash='fk1234567890',
                query='fk test'
            )
            session.add(pin)
            session.commit()
            
            # 创建下载任务
            task = DownloadTask(
                pin_id='fk_test_pin',
                pin_hash='fk1234567890',
                image_url='https://example.com/fk.jpg'
            )
            session.add(task)
            session.commit()
            
            # 验证关联关系
            saved_pin = session.query(Pin).filter_by(id='fk_test_pin').first()
            assert len(saved_pin.download_tasks) == 1
            assert saved_pin.download_tasks[0].image_url == 'https://example.com/fk.jpg'
    
    def test_unique_constraints(self):
        """测试唯一约束"""
        from sqlalchemy.exc import IntegrityError

        with get_database_session() as session:
            # 创建第一个Pin
            pin1 = Pin(
                id='unique_test_1',
                pin_hash='unique1234567890',
                query='unique test'
            )
            session.add(pin1)
            session.commit()

        # 在新的会话中尝试创建具有相同pin_hash的Pin
        with pytest.raises(Exception):  # 应该抛出完整性错误
            with get_database_session() as session:
                pin2 = Pin(
                    id='unique_test_2',
                    pin_hash='unique1234567890',  # 相同的hash
                    query='unique test'
                )
                session.add(pin2)
                session.commit()
    
    def test_indexes_exist(self):
        """测试索引是否正确创建"""
        # 这个测试主要验证表结构创建时没有错误
        # 实际的索引性能测试需要大量数据
        with get_database_session() as session:
            # 创建一些测试数据
            for i in range(10):
                pin = Pin(
                    id=f'index_test_{i}',
                    pin_hash=f'index{i:010d}',
                    query='index test',
                    creator_id=f'creator_{i % 3}'  # 测试creator索引
                )
                session.add(pin)
            
            session.commit()
            
            # 验证查询能正常执行（索引有效）
            pins = session.query(Pin).filter_by(query='index test').all()
            assert len(pins) == 10
            
            creator_pins = session.query(Pin).filter_by(creator_id='creator_1').all()
            assert len(creator_pins) > 0
