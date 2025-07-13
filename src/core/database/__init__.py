"""
Pinterest爬虫数据库模块

提供SQLite数据库支持，包括：
- 数据库模型定义
- Repository模式数据访问层
- 数据库连接和会话管理
"""

from .schema import Pin, DownloadTask, ScrapingSession, CacheMetadata
from .base import DatabaseManager, get_database_session, initialize_database
from .repository import SQLiteRepository
from .manager_factory import DatabaseManagerFactory
from .normalizer import PinDataNormalizer
from .atomic_saver import AtomicPinSaver

__all__ = [
    'Pin',
    'DownloadTask',
    'ScrapingSession',
    'CacheMetadata',
    'DatabaseManager',
    'DatabaseManagerFactory',
    'initialize_database',
    'get_database_session',
    'SQLiteRepository',
    'PinDataNormalizer',
    'AtomicPinSaver'
]
