"""
数据库管理器工厂类

实现按关键词管理多个数据库实例的工厂模式，替换全局单例_db_manager，
支持动态创建和缓存关键词特定的数据库管理器。

核心功能：
- 线程安全的数据库管理器创建和缓存
- 按关键词生成独立的数据库文件路径
- 资源管理和清理机制
- 支持多关键词并发处理
"""

import os
import threading
import weakref
from typing import Dict, Optional
from loguru import logger
from sqlalchemy import text

from .base import DatabaseManager
from ...utils.utils import sanitize_filename


class DatabaseManagerFactory:
    """数据库管理器工厂类
    
    通过工厂模式管理多个关键词特定的数据库实例，确保线程安全和资源有效管理。
    
    主要功能：
    - 按关键词创建和缓存数据库管理器实例
    - 线程安全的实例管理
    - 自动生成关键词特定的数据库路径
    - 资源清理和内存管理
    
    Example:
        >>> manager = DatabaseManagerFactory.get_manager("cats", "output")
        >>> # 数据库文件: output/cats/pinterest.db
        >>> DatabaseManagerFactory.cleanup_manager("cats", "output")
    """
    
    # 类级别的管理器缓存，使用弱引用避免内存泄漏
    _managers: Dict[str, DatabaseManager] = {}
    _lock = threading.Lock()
    
    @classmethod
    def get_manager(cls, keyword: str, output_dir: str) -> DatabaseManager:
        """获取或创建关键词特定的数据库管理器
        
        Args:
            keyword: 搜索关键词
            output_dir: 输出根目录
            
        Returns:
            关键词特定的数据库管理器实例
            
        Raises:
            ValueError: 当关键词或输出目录为空时
            OSError: 当数据库目录创建失败时
        """
        if not keyword or not keyword.strip():
            raise ValueError("关键词不能为空")
        
        if not output_dir or not output_dir.strip():
            raise ValueError("输出目录不能为空")
        
        # 生成关键词特定的数据库路径
        safe_keyword = sanitize_filename(keyword.strip())
        keyword_dir = os.path.join(output_dir, safe_keyword)
        db_path = os.path.join(keyword_dir, 'pinterest.db')
        
        # 生成缓存键
        cache_key = f'{safe_keyword}:{os.path.abspath(output_dir)}'
        
        # 线程安全的管理器获取或创建
        with cls._lock:
            if cache_key not in cls._managers:
                try:
                    # 确保关键词目录存在
                    os.makedirs(keyword_dir, exist_ok=True)

                    # 创建数据库管理器
                    manager = DatabaseManager(db_path)
                    manager.create_tables()

                    # 缓存管理器实例
                    cls._managers[cache_key] = manager

                    logger.debug(f"创建关键词数据库管理器: {keyword} -> {db_path}")

                except Exception as e:
                    logger.error(f"创建数据库管理器失败: {keyword}, 错误: {e}")
                    logger.error(f"数据库路径: {db_path}")
                    logger.error(f"关键词目录: {keyword_dir}")
                    import traceback
                    logger.error(f"错误堆栈: {traceback.format_exc()}")
                    raise
            else:
                # 验证现有管理器是否仍然有效
                try:
                    existing_manager = cls._managers[cache_key]
                    # 测试数据库连接
                    with existing_manager.get_session() as session:
                        session.execute(text("SELECT 1"))
                    logger.debug(f"复用现有数据库管理器: {keyword}")
                except Exception as e:
                    logger.warning(f"现有数据库管理器无效，重新创建: {keyword}, 错误: {e}")
                    # 移除无效的管理器并重新创建
                    del cls._managers[cache_key]
                    return cls.get_manager(keyword, output_dir)

            return cls._managers[cache_key]
    
    @classmethod
    def cleanup_manager(cls, keyword: str, output_dir: str) -> bool:
        """清理特定关键词的数据库管理器
        
        Args:
            keyword: 搜索关键词
            output_dir: 输出根目录
            
        Returns:
            清理是否成功
        """
        if not keyword or not output_dir:
            return False
        
        safe_keyword = sanitize_filename(keyword.strip())
        cache_key = f'{safe_keyword}:{os.path.abspath(output_dir)}'
        
        with cls._lock:
            if cache_key in cls._managers:
                try:
                    manager = cls._managers[cache_key]
                    
                    # 关闭数据库连接
                    if hasattr(manager, 'engine') and manager.engine:
                        manager.engine.dispose()
                    
                    # 从缓存中移除
                    del cls._managers[cache_key]
                    
                    logger.info(f"清理关键词数据库管理器: {keyword}")
                    return True
                    
                except Exception as e:
                    logger.error(f"清理数据库管理器失败: {keyword}, 错误: {e}")
                    return False
        
        return True
    
    @classmethod
    def cleanup_all_managers(cls) -> int:
        """清理所有数据库管理器
        
        Returns:
            清理的管理器数量
        """
        cleaned_count = 0
        
        with cls._lock:
            managers_to_clean = list(cls._managers.items())
            
            for cache_key, manager in managers_to_clean:
                try:
                    # 关闭数据库连接
                    if hasattr(manager, 'engine') and manager.engine:
                        manager.engine.dispose()
                    
                    cleaned_count += 1
                    
                except Exception as e:
                    logger.error(f"清理数据库管理器失败: {cache_key}, 错误: {e}")
            
            # 清空缓存
            cls._managers.clear()
        
        if cleaned_count > 0:
            logger.info(f"清理了 {cleaned_count} 个数据库管理器")
        
        return cleaned_count
    
    @classmethod
    def get_manager_count(cls) -> int:
        """获取当前缓存的管理器数量
        
        Returns:
            管理器数量
        """
        with cls._lock:
            return len(cls._managers)
    
    @classmethod
    def get_cached_keywords(cls) -> list:
        """获取当前缓存的关键词列表
        
        Returns:
            关键词列表
        """
        with cls._lock:
            keywords = []
            for cache_key in cls._managers.keys():
                keyword = cache_key.split(':')[0]
                keywords.append(keyword)
            return keywords
    
    @classmethod
    def generate_database_path(cls, keyword: str, output_dir: str) -> str:
        """生成关键词特定的数据库路径（不创建管理器）
        
        Args:
            keyword: 搜索关键词
            output_dir: 输出根目录
            
        Returns:
            数据库文件路径
        """
        safe_keyword = sanitize_filename(keyword.strip())
        keyword_dir = os.path.join(output_dir, safe_keyword)
        return os.path.join(keyword_dir, 'pinterest.db')
