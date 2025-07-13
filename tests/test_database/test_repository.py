"""
Repository数据持久化层测试
"""

import pytest
import tempfile
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from src.core.database.base import initialize_database, get_database_session
from src.core.database.repository import SQLiteRepository
from src.core.database.schema import Pin, DownloadTask, ScrapingSession


class TestSQLiteRepository:
    """SQLite Repository测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_repository(self):
        """设置测试Repository"""
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()

        # 初始化数据库
        self.db_manager = initialize_database(self.temp_db.name)
        self.repository = SQLiteRepository()

        yield

        # 清理 - 先关闭数据库连接再删除文件
        try:
            if hasattr(self.db_manager, 'engine'):
                self.db_manager.engine.dispose()
            os.unlink(self.temp_db.name)
        except (PermissionError, FileNotFoundError):
            # 在Windows上可能出现文件锁定，忽略清理错误
            pass

    def test_keyword_context_repository(self):
        """测试关键词上下文Repository"""
        import tempfile
        temp_dir = tempfile.mkdtemp()

        try:
            from src.core.database.manager_factory import DatabaseManagerFactory

            # 创建关键词特定的Repository
            repo = SQLiteRepository(keyword="test_keyword", output_dir=temp_dir)

            # 验证关键词上下文
            assert repo.keyword == "test_keyword"
            assert repo.output_dir == temp_dir

            # 测试基本功能
            test_pins = [
                {
                    'id': 'keyword_pin_1',
                    'title': 'Keyword test pin',
                    'description': 'Test pin for keyword context',
                    'image_url': 'https://example.com/keyword_test.jpg',
                    'largest_image_url': 'https://example.com/keyword_test_large.jpg',
                    'creator': {'name': 'keyword_creator'},
                    'query': 'test_keyword'
                }
            ]

            # 保存和查询数据
            repo.save_pins_batch(test_pins)
            loaded_pins = repo.load_pins_by_query("test_keyword")

            assert len(loaded_pins) == 1
            assert loaded_pins[0]['id'] == 'keyword_pin_1'

            # 验证数据库文件创建
            import os
            db_path = os.path.join(temp_dir, "test_keyword", "pinterest.db")
            assert os.path.exists(db_path)

        finally:
            DatabaseManagerFactory.cleanup_all_managers()
            import shutil
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
    
    def test_save_pins_batch_new_pins(self):
        """测试批量保存新Pin"""
        # 准备测试数据
        test_pins = [
            {
                'id': 'pin_1',
                'title': 'Test Pin 1',
                'description': 'Description 1',
                'largest_image_url': 'https://example.com/1.jpg',
                'creator': {'name': 'Creator 1', 'id': 'creator_1'},
                'stats': {'saves': 100}
            },
            {
                'id': 'pin_2',
                'title': 'Test Pin 2',
                'description': 'Description 2',
                'largest_image_url': 'https://example.com/2.jpg',
                'creator': {'name': 'Creator 2', 'id': 'creator_2'},
                'stats': {'saves': 200}
            }
        ]
        
        # 执行批量保存
        result = self.repository.save_pins_batch(test_pins, 'test query')
        assert result == True
        
        # 验证数据已保存
        saved_pins = self.repository.load_pins_by_query('test query')
        assert len(saved_pins) == 2
        
        # 验证Pin数据
        pin_ids = [pin['id'] for pin in saved_pins]
        assert 'pin_1' in pin_ids
        assert 'pin_2' in pin_ids
        
        # 验证下载任务已创建
        with get_database_session() as session:
            tasks = session.query(DownloadTask).all()
            assert len(tasks) == 2
    
    def test_save_pins_batch_duplicate_handling(self):
        """测试重复Pin的处理"""
        # 第一次保存
        test_pins = [
            {
                'id': 'duplicate_pin',
                'title': 'Original Title',
                'largest_image_url': 'https://example.com/dup.jpg'
            }
        ]
        
        result1 = self.repository.save_pins_batch(test_pins, 'dup test')
        assert result1 == True
        
        # 第二次保存相同Pin（应该更新而不是重复）
        updated_pins = [
            {
                'id': 'duplicate_pin',
                'title': 'Updated Title',
                'largest_image_url': 'https://example.com/dup.jpg'
            }
        ]
        
        result2 = self.repository.save_pins_batch(updated_pins, 'dup test')
        assert result2 == True
        
        # 验证只有一个Pin，且标题已更新
        saved_pins = self.repository.load_pins_by_query('dup test')
        assert len(saved_pins) == 1
        assert saved_pins[0]['title'] == 'Updated Title'
    
    def test_load_pins_by_query_with_pagination(self):
        """测试分页加载Pin数据"""
        # 创建测试数据
        test_pins = []
        for i in range(20):
            test_pins.append({
                'id': f'page_pin_{i}',
                'title': f'Page Pin {i}',
                'largest_image_url': f'https://example.com/{i}.jpg'
            })
        
        self.repository.save_pins_batch(test_pins, 'page test')
        
        # 测试分页加载
        page1 = self.repository.load_pins_by_query('page test', limit=10, offset=0)
        assert len(page1) == 10
        
        page2 = self.repository.load_pins_by_query('page test', limit=10, offset=10)
        assert len(page2) == 10
        
        # 验证没有重复
        page1_ids = [pin['id'] for pin in page1]
        page2_ids = [pin['id'] for pin in page2]
        assert len(set(page1_ids) & set(page2_ids)) == 0
    
    def test_get_pin_count_by_query(self):
        """测试获取Pin数量"""
        # 初始数量应该为0
        count = self.repository.get_pin_count_by_query('count test')
        assert count == 0
        
        # 添加一些Pin
        test_pins = [
            {'id': f'count_pin_{i}', 'largest_image_url': f'https://example.com/{i}.jpg'}
            for i in range(5)
        ]
        
        self.repository.save_pins_batch(test_pins, 'count test')
        
        # 验证数量
        count = self.repository.get_pin_count_by_query('count test')
        assert count == 5
    
    def test_create_scraping_session(self):
        """测试创建采集会话"""
        session_id = self.repository.create_scraping_session(
            query='session test',
            target_count=100,
            output_dir='/test/output',
            download_images=True
        )
        
        assert session_id is not None
        assert len(session_id) > 0
        
        # 验证会话已保存
        with get_database_session() as session:
            scraping_session = session.query(ScrapingSession).filter_by(id=session_id).first()
            assert scraping_session is not None
            assert scraping_session.query == 'session test'
            assert scraping_session.target_count == 100
            assert scraping_session.status == 'running'
    
    def test_update_session_status(self):
        """测试更新会话状态"""
        # 创建会话
        session_id = self.repository.create_scraping_session(
            query='status test',
            target_count=50,
            output_dir='/test'
        )
        
        # 更新状态
        stats = {'total_pins': 45, 'downloaded_images': 40}
        self.repository.update_session_status(
            session_id=session_id,
            status='completed',
            actual_count=45,
            stats=stats
        )
        
        # 验证更新
        with get_database_session() as session:
            scraping_session = session.query(ScrapingSession).filter_by(id=session_id).first()
            assert scraping_session.status == 'completed'
            assert scraping_session.actual_count == 45
            assert scraping_session.stats_dict == stats
            assert scraping_session.completed_at is not None
    
    def test_download_task_operations(self):
        """测试下载任务操作"""
        # 先创建Pin和下载任务
        test_pins = [
            {
                'id': 'download_test_pin',
                'largest_image_url': 'https://example.com/download.jpg'
            }
        ]
        
        self.repository.save_pins_batch(test_pins, 'download test')
        
        # 获取待下载任务
        pending_tasks = self.repository.get_pending_download_tasks()
        assert len(pending_tasks) == 1

        task = pending_tasks[0]
        assert task['status'] == 'pending'

        # 更新任务状态为下载中
        self.repository.update_download_task_status(
            task_id=task['id'],
            status='downloading'
        )

        # 更新任务状态为完成
        self.repository.update_download_task_status(
            task_id=task['id'],
            status='completed',
            local_path='/test/path/image.jpg',
            file_size=1024
        )
        
        # 验证更新
        with get_database_session() as session:
            updated_task = session.query(DownloadTask).filter_by(id=task['id']).first()
            assert updated_task.status == 'completed'
            assert updated_task.local_path == '/test/path/image.jpg'
            assert updated_task.file_size == 1024
    
    def test_concurrent_access(self):
        """测试并发访问安全性"""
        def save_pins_worker(worker_id):
            """工作线程函数"""
            test_pins = [
                {
                    'id': f'concurrent_pin_{worker_id}_{i}',
                    'title': f'Concurrent Pin {worker_id}-{i}',
                    'largest_image_url': f'https://example.com/{worker_id}_{i}.jpg'
                }
                for i in range(10)
            ]
            
            return self.repository.save_pins_batch(test_pins, 'concurrent test')
        
        # 使用多线程并发保存
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(save_pins_worker, i) for i in range(5)]
            results = [future.result() for future in futures]
        
        # 验证所有操作都成功
        assert all(results)
        
        # 验证数据完整性
        total_pins = self.repository.load_pins_by_query('concurrent test')
        assert len(total_pins) == 50  # 5个线程 × 10个Pin
        
        # 验证没有重复
        pin_ids = [pin['id'] for pin in total_pins]
        assert len(set(pin_ids)) == 50
    
    def test_transaction_rollback(self):
        """测试事务回滚"""
        # 创建一个会导致错误的Pin数据（缺少必要字段）
        invalid_pins = [
            {
                'id': 'valid_pin',
                'title': 'Valid Pin',
                'largest_image_url': 'https://example.com/valid.jpg'
            },
            {
                # 缺少id字段，但有其他数据
                'title': 'Invalid Pin',
                'largest_image_url': 'https://example.com/invalid.jpg'
            }
        ]
        
        # 保存应该成功（Repository会处理缺少id的情况）
        result = self.repository.save_pins_batch(invalid_pins, 'rollback test')
        assert result == True
        
        # 验证有效数据已保存
        saved_pins = self.repository.load_pins_by_query('rollback test')
        assert len(saved_pins) == 2  # Repository会为缺少id的Pin生成UUID
    
    def test_empty_pins_batch(self):
        """测试空Pin列表的处理"""
        result = self.repository.save_pins_batch([], 'empty test')
        assert result == True
        
        pins = self.repository.load_pins_by_query('empty test')
        assert len(pins) == 0
    
    def test_cache_metadata_update(self):
        """测试缓存元数据更新"""
        # 保存一些Pin
        test_pins = [
            {'id': f'cache_pin_{i}', 'largest_image_url': f'https://example.com/{i}.jpg'}
            for i in range(3)
        ]
        
        self.repository.save_pins_batch(test_pins, 'cache test')
        
        # 验证缓存元数据已更新
        with get_database_session() as session:
            from src.core.database.schema import CacheMetadata
            cache_meta = session.query(CacheMetadata).filter_by(query='cache test').first()
            assert cache_meta is not None
            assert cache_meta.pin_count == 3
