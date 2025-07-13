"""
数据库基础配置和连接管理
"""

import os
import threading
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from loguru import logger

# 创建基础模型类
Base = declarative_base()

# 线程本地存储，用于管理数据库连接
_local = threading.local()


class DatabaseManager:
    """数据库管理器，负责数据库连接和会话管理"""
    
    def __init__(self, db_path: str):
        """初始化数据库管理器
        
        Args:
            db_path: SQLite数据库文件路径
        """
        self.db_path = db_path
        self.engine = None
        self.SessionLocal = None
        self._initialize_database()
    
    def _initialize_database(self):
        """初始化数据库连接"""
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # 创建数据库引擎
        self.engine = create_engine(
            f'sqlite:///{self.db_path}',
            pool_pre_ping=True,
            connect_args={
                'check_same_thread': False,
                'timeout': 30
            },
            echo=False  # 设置为True可以看到SQL语句
        )
        
        # 启用SQLite外键约束
        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")  # 启用WAL模式提高并发性能
            cursor.execute("PRAGMA synchronous=NORMAL")  # 平衡性能和安全性
            cursor.close()
        
        # 创建会话工厂
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False
        )
        
        logger.info(f"数据库初始化完成: {self.db_path}")
    
    def create_tables(self):
        """创建所有数据库表"""
        from .schema import Pin, DownloadTask, ScrapingSession, CacheMetadata
        
        Base.metadata.create_all(bind=self.engine)
        logger.info("数据库表创建完成")
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """获取数据库会话的上下文管理器"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            session.close()


# 全局数据库管理器实例
_db_manager = None


def initialize_database(db_path: str) -> DatabaseManager:
    """初始化全局数据库管理器
    
    Args:
        db_path: 数据库文件路径
        
    Returns:
        数据库管理器实例
    """
    global _db_manager
    _db_manager = DatabaseManager(db_path)
    _db_manager.create_tables()
    return _db_manager


def get_database_manager() -> DatabaseManager:
    """获取全局数据库管理器实例"""
    if _db_manager is None:
        raise RuntimeError("数据库未初始化，请先调用 initialize_database()")
    return _db_manager


@contextmanager
def get_database_session() -> Generator[Session, None, None]:
    """获取数据库会话的便捷函数"""
    db_manager = get_database_manager()
    with db_manager.get_session() as session:
        yield session
