"""
数据库管理器工厂类测试

测试DatabaseManagerFactory的核心功能：
- 线程安全的管理器创建和缓存
- 关键词特定的数据库路径生成
- 资源管理和清理机制
- 并发访问测试
"""

import os
import pytest
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

from src.core.database.manager_factory import DatabaseManagerFactory
from src.core.database.base import DatabaseManager


class TestDatabaseManagerFactory:
    """数据库管理器工厂测试类"""
    
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
    
    def test_get_manager_basic_functionality(self):
        """测试基本的管理器获取功能"""
        keyword = "test_cats"
        
        # 获取管理器
        manager = DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
        
        # 验证管理器类型
        assert isinstance(manager, DatabaseManager)
        
        # 验证数据库路径
        expected_path = os.path.join(self.temp_dir, "test_cats", "pinterest.db")
        assert manager.db_path == expected_path
        
        # 验证数据库文件和目录已创建
        assert os.path.exists(os.path.dirname(expected_path))
        assert os.path.exists(expected_path)
    
    def test_manager_caching(self):
        """测试管理器缓存功能"""
        keyword = "test_dogs"
        
        # 第一次获取
        manager1 = DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
        
        # 第二次获取应该返回相同实例
        manager2 = DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
        
        assert manager1 is manager2
        assert DatabaseManagerFactory.get_manager_count() == 1
    
    def test_different_keywords_different_managers(self):
        """测试不同关键词创建不同管理器"""
        keyword1 = "cats"
        keyword2 = "dogs"
        
        manager1 = DatabaseManagerFactory.get_manager(keyword1, self.temp_dir)
        manager2 = DatabaseManagerFactory.get_manager(keyword2, self.temp_dir)
        
        # 应该是不同的实例
        assert manager1 is not manager2
        
        # 应该有不同的数据库路径
        assert manager1.db_path != manager2.db_path
        
        # 管理器数量应该是2
        assert DatabaseManagerFactory.get_manager_count() == 2
    
    def test_keyword_sanitization(self):
        """测试关键词安全化"""
        unsafe_keyword = "test/cats:dogs*"
        
        manager = DatabaseManagerFactory.get_manager(unsafe_keyword, self.temp_dir)
        
        # 验证路径中的关键词已被安全化
        assert "test_cats_dogs_" in manager.db_path
        assert "/" not in os.path.basename(os.path.dirname(manager.db_path))
        assert ":" not in os.path.basename(os.path.dirname(manager.db_path))
        assert "*" not in os.path.basename(os.path.dirname(manager.db_path))
    
    def test_cleanup_manager(self):
        """测试管理器清理功能"""
        keyword = "test_cleanup"
        
        # 创建管理器
        manager = DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
        assert DatabaseManagerFactory.get_manager_count() == 1
        
        # 清理管理器
        success = DatabaseManagerFactory.cleanup_manager(keyword, self.temp_dir)
        assert success is True
        assert DatabaseManagerFactory.get_manager_count() == 0
    
    def test_cleanup_all_managers(self):
        """测试清理所有管理器"""
        keywords = ["cats", "dogs", "birds"]
        
        # 创建多个管理器
        for keyword in keywords:
            DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
        
        assert DatabaseManagerFactory.get_manager_count() == 3
        
        # 清理所有管理器
        cleaned_count = DatabaseManagerFactory.cleanup_all_managers()
        assert cleaned_count == 3
        assert DatabaseManagerFactory.get_manager_count() == 0
    
    def test_thread_safety(self):
        """测试线程安全性"""
        keywords = [f"thread_test_{i}" for i in range(10)]
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
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(create_manager, keyword) for keyword in keywords]
            
            # 等待所有任务完成
            for future in as_completed(futures):
                future.result()
        
        # 验证没有错误
        assert len(errors) == 0
        
        # 验证所有管理器都创建成功
        assert len(managers) == 10
        assert DatabaseManagerFactory.get_manager_count() == 10
        
        # 验证每个管理器都是唯一的
        manager_instances = list(managers.values())
        assert len(set(id(m) for m in manager_instances)) == 10
    
    def test_concurrent_same_keyword_access(self):
        """测试同一关键词的并发访问"""
        keyword = "concurrent_test"
        managers = []
        errors = []
        
        def get_same_manager():
            try:
                manager = DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
                managers.append(manager)
                return manager
            except Exception as e:
                errors.append(e)
                return None
        
        # 多个线程同时获取同一关键词的管理器
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_same_manager) for _ in range(20)]
            
            for future in as_completed(futures):
                future.result()
        
        # 验证没有错误
        assert len(errors) == 0
        
        # 验证所有返回的都是同一个实例
        assert len(managers) == 20
        first_manager = managers[0]
        assert all(manager is first_manager for manager in managers)
        
        # 验证只有一个管理器被缓存
        assert DatabaseManagerFactory.get_manager_count() == 1
    
    def test_invalid_inputs(self):
        """测试无效输入的处理"""
        # 空关键词
        with pytest.raises(ValueError, match="关键词不能为空"):
            DatabaseManagerFactory.get_manager("", self.temp_dir)
        
        with pytest.raises(ValueError, match="关键词不能为空"):
            DatabaseManagerFactory.get_manager("   ", self.temp_dir)
        
        # 空输出目录
        with pytest.raises(ValueError, match="输出目录不能为空"):
            DatabaseManagerFactory.get_manager("test", "")
        
        with pytest.raises(ValueError, match="输出目录不能为空"):
            DatabaseManagerFactory.get_manager("test", "   ")
    
    def test_get_cached_keywords(self):
        """测试获取缓存关键词列表"""
        keywords = ["cats", "dogs", "birds"]
        
        # 创建管理器
        for keyword in keywords:
            DatabaseManagerFactory.get_manager(keyword, self.temp_dir)
        
        # 获取缓存的关键词
        cached_keywords = DatabaseManagerFactory.get_cached_keywords()
        
        # 验证关键词列表
        assert len(cached_keywords) == 3
        assert set(cached_keywords) == set(keywords)
    
    def test_generate_database_path(self):
        """测试数据库路径生成"""
        keyword = "path_test"
        
        # 生成路径
        db_path = DatabaseManagerFactory.generate_database_path(keyword, self.temp_dir)
        
        # 验证路径格式
        expected_path = os.path.join(self.temp_dir, "path_test", "pinterest.db")
        assert db_path == expected_path
        
        # 验证路径生成不会创建管理器
        assert DatabaseManagerFactory.get_manager_count() == 0
