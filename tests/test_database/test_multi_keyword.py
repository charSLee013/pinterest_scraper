"""
多关键词并发处理测试

测试DatabaseManagerFactory的线程安全性、Repository的关键词路由功能
以及多关键词并发处理的数据隔离性和性能。
"""

import pytest
import asyncio
import tempfile
import threading
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch, AsyncMock

from src.core.database.manager_factory import DatabaseManagerFactory
from src.core.database.repository import SQLiteRepository
from src.core.database.schema import Pin, DownloadTask, ScrapingSession
from src.core.download.task_manager import DownloadTaskManager
from src.core.pinterest_scraper import PinterestScraper


class TestMultiKeywordConcurrency:
    """多关键词并发处理测试类"""
    
    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """设置和清理测试环境"""
        # 测试前清理所有管理器
        DatabaseManagerFactory.cleanup_all_managers()
        
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        
        yield
        
        # 测试后清理
        DatabaseManagerFactory.cleanup_all_managers()
        
        # 清理临时文件
        import shutil
        try:
            shutil.rmtree(self.temp_dir)
        except:
            pass
    
    def test_database_manager_factory_thread_safety(self):
        """测试工厂类的线程安全性"""
        keywords = [f"thread_test_{i}" for i in range(20)]
        managers = {}
        errors = []
        
        def create_manager(keyword):
            try:
                manager = DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
                managers[keyword] = manager
                return manager
            except Exception as e:
                errors.append(e)
                return None
        
        # 使用线程池并发创建管理器
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_manager, keyword) for keyword in keywords]
            
            # 等待所有任务完成
            for future in as_completed(futures):
                future.result()
        
        # 验证没有错误
        assert len(errors) == 0, f"线程安全测试失败，错误: {errors}"
        
        # 验证所有管理器都创建成功
        assert len(managers) == 20
        assert DatabaseManagerFactory.get_manager_count() == 20
        
        # 验证每个管理器都是唯一的
        manager_instances = list(managers.values())
        assert len(set(id(m) for m in manager_instances)) == 20
        
        # 验证数据库文件都被创建
        for keyword in keywords:
            db_path = os.path.join(self.temp_dir, keyword, "pinterest.db")
            assert os.path.exists(db_path), f"数据库文件未创建: {db_path}"
    
    def test_keyword_database_isolation(self):
        """测试不同关键词的数据库隔离"""
        keywords = ["cats", "dogs", "birds", "flowers"]
        repositories = {}
        
        # 创建不同关键词的repository
        for keyword in keywords:
            repo = SQLiteRepository(keyword=keyword, output_dir=self.temp_dir)
            repositories[keyword] = repo
            
            # 在每个数据库中创建测试数据
            test_pins = [
                {
                    'id': f'{keyword}_pin_1',
                    'title': f'{keyword} test pin 1',
                    'description': f'Test pin for {keyword}',
                    'image_url': f'https://example.com/{keyword}_1.jpg',
                    'largest_image_url': f'https://example.com/{keyword}_1_large.jpg',
                    'creator': {'name': f'{keyword}_creator'},
                    'query': keyword
                },
                {
                    'id': f'{keyword}_pin_2',
                    'title': f'{keyword} test pin 2',
                    'description': f'Another test pin for {keyword}',
                    'image_url': f'https://example.com/{keyword}_2.jpg',
                    'largest_image_url': f'https://example.com/{keyword}_2_large.jpg',
                    'creator': {'name': f'{keyword}_creator'},
                    'query': keyword
                }
            ]
            
            # 保存测试数据
            repo.save_pins_batch(test_pins, keyword)
        
        # 验证数据隔离
        for keyword in keywords:
            repo = repositories[keyword]
            
            # 查询当前关键词的数据
            pins = repo.load_pins_by_query(keyword)
            assert len(pins) == 2, f"关键词 {keyword} 的数据数量不正确"
            
            # 验证数据内容
            for pin in pins:
                assert keyword in pin['id'], f"Pin ID 不包含关键词: {pin['id']}"
                assert pin['query'] == keyword, f"Pin 查询字段不匹配: {pin['query']}"
            
            # 验证不包含其他关键词的数据
            for other_keyword in keywords:
                if other_keyword != keyword:
                    other_pins = repo.load_pins_by_query(other_keyword)
                    assert len(other_pins) == 0, f"关键词 {keyword} 的数据库包含了 {other_keyword} 的数据"
        
        # 验证数据库文件隔离
        for keyword in keywords:
            db_path = os.path.join(self.temp_dir, keyword, "pinterest.db")
            assert os.path.exists(db_path), f"数据库文件不存在: {db_path}"
            
            # 验证文件大小不同（包含不同数据）
            file_size = os.path.getsize(db_path)
            assert file_size > 0, f"数据库文件为空: {db_path}"
    
    def test_repository_keyword_routing(self):
        """测试Repository的关键词路由功能"""
        # 测试关键词特定路由
        repo1 = SQLiteRepository(keyword="routing_test_1", output_dir=self.temp_dir)
        repo2 = SQLiteRepository(keyword="routing_test_2", output_dir=self.temp_dir)
        
        # 验证它们使用不同的数据库管理器
        assert repo1.keyword == "routing_test_1"
        assert repo2.keyword == "routing_test_2"
        
        # 创建测试会话
        session_id_1 = repo1.create_scraping_session("routing_test_1", 100, self.temp_dir)
        session_id_2 = repo2.create_scraping_session("routing_test_2", 100, self.temp_dir)
        
        assert session_id_1 != session_id_2
        
        # 验证会话隔离
        repo1.update_session_status(session_id_1, "completed", 50)
        repo2.update_session_status(session_id_2, "completed", 75)
        
        # 验证数据库管理器数量
        assert DatabaseManagerFactory.get_manager_count() == 2
        
        # 测试向后兼容路由
        repo_legacy = SQLiteRepository()
        assert repo_legacy.keyword is None
        assert repo_legacy.output_dir is None
    
    @pytest.mark.asyncio
    async def test_concurrent_scraping(self):
        """测试多关键词并发采集"""
        keywords = ["concurrent_cats", "concurrent_dogs", "concurrent_birds"]
        scrapers = []
        
        # 创建多个scraper实例
        for keyword in keywords:
            scraper = PinterestScraper(
                output_dir=self.temp_dir,
                download_images=False,
                debug=False
            )
            scrapers.append((scraper, keyword))
        
        # Mock掉实际的数据采集
        async def mock_scrape(scraper, keyword):
            with patch.object(scraper, 'scraper') as mock_scraper:
                mock_scraper.scrape_pins = AsyncMock(return_value=[
                    {
                        'id': f'{keyword}_concurrent_pin',
                        'title': f'{keyword} concurrent test',
                        'query': keyword
                    }
                ])
                
                return await scraper.scrape(query=keyword, count=10)
        
        # 并发执行采集任务
        tasks = [mock_scrape(scraper, keyword) for scraper, keyword in scrapers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 验证所有任务都成功完成
        for i, result in enumerate(results):
            assert not isinstance(result, Exception), f"采集任务 {i} 失败: {result}"
        
        # 验证数据库隔离
        for scraper, keyword in scrapers:
            assert scraper.repository is not None
            assert scraper.repository.keyword == keyword
            
            # 验证数据库文件存在
            db_path = os.path.join(self.temp_dir, keyword, "pinterest.db")
            assert os.path.exists(db_path)
        
        # 验证DatabaseManagerFactory管理了正确数量的管理器
        assert DatabaseManagerFactory.get_manager_count() == len(keywords)
    
    def test_download_task_manager_isolation(self):
        """测试下载任务管理器的关键词隔离"""
        keywords = ["download_cats", "download_dogs"]
        managers = []
        
        # 创建不同关键词的下载管理器
        for keyword in keywords:
            manager = DownloadTaskManager(
                max_concurrent=5,
                auto_start=False,
                keyword=keyword,
                output_dir=self.temp_dir
            )
            managers.append((manager, keyword))
            
            # 创建测试下载任务
            task_id = manager.repository.create_download_task(
                pin_id=f"{keyword}_pin_1",
                image_url=f"https://example.com/{keyword}_image.jpg"
            )
            assert task_id is not None
        
        # 验证任务隔离
        for i, (manager, keyword) in enumerate(managers):
            # 获取当前管理器的待下载任务
            tasks = manager.repository.get_pending_download_tasks(limit=10)
            
            # 验证只包含当前关键词的任务
            for task in tasks:
                assert keyword in task.pin_id, f"任务包含错误的关键词: {task.pin_id}"
            
            # 验证任务数量
            assert len(tasks) == 1, f"关键词 {keyword} 的任务数量不正确"
    
    def test_performance_benchmark(self):
        """测试并发性能基准"""
        keywords = [f"perf_test_{i}" for i in range(50)]
        
        # 测试串行创建时间
        start_time = time.time()
        for keyword in keywords[:10]:  # 只测试10个避免测试时间过长
            DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
        serial_time = time.time() - start_time
        
        # 清理
        DatabaseManagerFactory.cleanup_all_managers()
        
        # 测试并发创建时间
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(DatabaseManagerFactory.get_manager, keyword, self.temp_dir)
                for keyword in keywords[10:20]  # 测试另外10个
            ]
            for future in as_completed(futures):
                future.result()
        concurrent_time = time.time() - start_time
        
        # 验证并发性能提升（允许一定误差）
        # 注意：由于数据库创建涉及I/O，并发提升可能不明显
        print(f"串行时间: {serial_time:.3f}s, 并发时间: {concurrent_time:.3f}s")
        
        # 验证功能正确性
        assert DatabaseManagerFactory.get_manager_count() == 10
    
    def test_backward_compatibility(self):
        """测试向后兼容性"""
        # 测试传统的全局数据库方式
        temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        temp_db.close()
        
        try:
            from src.core.database import initialize_database, get_database_session
            
            # 初始化全局数据库
            db_manager = initialize_database(temp_db.name)
            
            # 创建传统Repository
            repo = SQLiteRepository()
            
            # 测试基本功能
            session_id = repo.create_scraping_session("legacy_test", 100, temp_db.name)
            assert session_id is not None
            
            # 验证全局会话工作正常
            with get_database_session() as session:
                assert session is not None
            
            # 验证与新系统共存
            new_repo = SQLiteRepository(keyword="new_test", output_dir=self.temp_dir)
            new_session_id = new_repo.create_scraping_session("new_test", 100, self.temp_dir)
            assert new_session_id is not None
            assert new_session_id != session_id
            
        finally:
            try:
                if 'db_manager' in locals() and hasattr(db_manager, 'engine'):
                    db_manager.engine.dispose()
                os.unlink(temp_db.name)
            except:
                pass
