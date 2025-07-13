#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库索引迁移脚本
用于为现有数据库添加新的索引和约束
"""

import os
import sys
from pathlib import Path
from typing import List

from sqlalchemy import create_engine, text, inspect
from loguru import logger

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.core.database.base import DatabaseManager
from src.core.database.schema import Pin, DownloadTask


class IndexMigrator:
    """索引迁移器"""
    
    def __init__(self, db_path: str):
        """初始化迁移器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}')
        self.inspector = inspect(self.engine)
    
    def get_existing_indexes(self, table_name: str) -> List[str]:
        """获取表的现有索引
        
        Args:
            table_name: 表名
            
        Returns:
            索引名称列表
        """
        try:
            indexes = self.inspector.get_indexes(table_name)
            return [idx['name'] for idx in indexes]
        except Exception as e:
            logger.warning(f"获取表 {table_name} 的索引失败: {e}")
            return []
    
    def create_index_if_not_exists(self, index_sql: str, index_name: str) -> bool:
        """如果索引不存在则创建
        
        Args:
            index_sql: 创建索引的SQL语句
            index_name: 索引名称
            
        Returns:
            是否成功创建
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(index_sql))
                conn.commit()
                logger.info(f"成功创建索引: {index_name}")
                return True
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"索引已存在: {index_name}")
                return True
            else:
                logger.error(f"创建索引失败 {index_name}: {e}")
                return False
    
    def migrate_pin_indexes(self) -> bool:
        """迁移Pin表的索引"""
        logger.info("开始迁移Pin表索引...")
        
        # 检查表是否存在
        if 'pins' not in self.inspector.get_table_names():
            logger.warning("Pin表不存在，跳过索引迁移")
            return True
        
        # 获取现有索引
        existing_indexes = self.get_existing_indexes('pins')
        logger.debug(f"Pin表现有索引: {existing_indexes}")
        
        # 定义新索引
        new_indexes = [
            {
                'name': 'idx_pin_query_updated',
                'sql': 'CREATE INDEX IF NOT EXISTS idx_pin_query_updated ON pins (query, updated_at)'
            },
            {
                'name': 'idx_pin_hash_query',
                'sql': 'CREATE INDEX IF NOT EXISTS idx_pin_hash_query ON pins (pin_hash, query)'
            }
        ]
        
        success_count = 0
        for index_info in new_indexes:
            if index_info['name'] not in existing_indexes:
                if self.create_index_if_not_exists(index_info['sql'], index_info['name']):
                    success_count += 1
            else:
                logger.debug(f"索引已存在，跳过: {index_info['name']}")
                success_count += 1
        
        logger.info(f"Pin表索引迁移完成: {success_count}/{len(new_indexes)}")
        return success_count == len(new_indexes)
    
    def migrate_download_task_indexes(self) -> bool:
        """迁移DownloadTask表的索引"""
        logger.info("开始迁移DownloadTask表索引...")
        
        # 检查表是否存在
        if 'download_tasks' not in self.inspector.get_table_names():
            logger.warning("DownloadTask表不存在，跳过索引迁移")
            return True
        
        # 获取现有索引
        existing_indexes = self.get_existing_indexes('download_tasks')
        logger.debug(f"DownloadTask表现有索引: {existing_indexes}")
        
        # 定义新索引
        new_indexes = [
            {
                'name': 'idx_download_retry_status',
                'sql': 'CREATE INDEX IF NOT EXISTS idx_download_retry_status ON download_tasks (retry_count, status)'
            },
            {
                'name': 'idx_download_updated',
                'sql': 'CREATE INDEX IF NOT EXISTS idx_download_updated ON download_tasks (updated_at)'
            }
        ]
        
        success_count = 0
        for index_info in new_indexes:
            if index_info['name'] not in existing_indexes:
                if self.create_index_if_not_exists(index_info['sql'], index_info['name']):
                    success_count += 1
            else:
                logger.debug(f"索引已存在，跳过: {index_info['name']}")
                success_count += 1
        
        logger.info(f"DownloadTask表索引迁移完成: {success_count}/{len(new_indexes)}")
        return success_count == len(new_indexes)
    
    def add_unique_constraint_if_not_exists(self) -> bool:
        """为DownloadTask表添加唯一约束（如果不存在）"""
        logger.info("检查DownloadTask表的唯一约束...")
        
        try:
            # 检查约束是否已存在
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT name FROM sqlite_master 
                    WHERE type='index' AND name='uq_download_task_pin_id'
                """))
                
                if result.fetchone():
                    logger.debug("唯一约束已存在: uq_download_task_pin_id")
                    return True
                
                # 添加唯一约束
                conn.execute(text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_download_task_pin_id 
                    ON download_tasks (pin_id)
                """))
                conn.commit()
                
                logger.info("成功添加唯一约束: uq_download_task_pin_id")
                return True
                
        except Exception as e:
            if "UNIQUE constraint failed" in str(e) or "already exists" in str(e).lower():
                logger.debug("唯一约束已存在或数据冲突")
                return True
            else:
                logger.error(f"添加唯一约束失败: {e}")
                return False
    
    def migrate_all(self) -> bool:
        """执行所有迁移"""
        logger.info(f"开始数据库索引迁移: {self.db_path}")
        
        success = True
        success &= self.migrate_pin_indexes()
        success &= self.migrate_download_task_indexes()
        success &= self.add_unique_constraint_if_not_exists()
        
        if success:
            logger.info("所有索引迁移完成")
        else:
            logger.error("部分索引迁移失败")
        
        return success


def migrate_database(db_path: str) -> bool:
    """迁移单个数据库文件
    
    Args:
        db_path: 数据库文件路径
        
    Returns:
        是否成功
    """
    if not os.path.exists(db_path):
        logger.warning(f"数据库文件不存在: {db_path}")
        return False
    
    migrator = IndexMigrator(db_path)
    return migrator.migrate_all()


def migrate_all_databases(output_dir: str) -> bool:
    """迁移输出目录下的所有数据库文件
    
    Args:
        output_dir: 输出目录
        
    Returns:
        是否全部成功
    """
    output_path = Path(output_dir)
    if not output_path.exists():
        logger.warning(f"输出目录不存在: {output_dir}")
        return False
    
    # 查找所有数据库文件
    db_files = list(output_path.rglob("*.db"))
    
    if not db_files:
        logger.info("未找到数据库文件")
        return True
    
    logger.info(f"找到 {len(db_files)} 个数据库文件")
    
    success_count = 0
    for db_file in db_files:
        logger.info(f"迁移数据库: {db_file}")
        if migrate_database(str(db_file)):
            success_count += 1
        else:
            logger.error(f"迁移失败: {db_file}")
    
    logger.info(f"迁移完成: {success_count}/{len(db_files)} 成功")
    return success_count == len(db_files)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='数据库索引迁移工具')
    parser.add_argument('--db-path', help='单个数据库文件路径')
    parser.add_argument('--output-dir', default='output', help='输出目录路径（迁移所有数据库）')
    
    args = parser.parse_args()
    
    if args.db_path:
        # 迁移单个数据库
        success = migrate_database(args.db_path)
    else:
        # 迁移所有数据库
        success = migrate_all_databases(args.output_dir)
    
    if success:
        print("✅ 数据库索引迁移成功")
        sys.exit(0)
    else:
        print("❌ 数据库索引迁移失败")
        sys.exit(1)
