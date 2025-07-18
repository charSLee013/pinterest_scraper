#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库修复工具

专门用于修复从运行中复制的数据库文件可能存在的问题：
1. WAL文件状态不一致
2. 文件锁定状态
3. 事务状态不完整
4. 数据库碎片和索引问题

使用场景：
- 从正在运行的进程中复制了数据库文件
- 数据库出现 "database disk image is malformed" 错误
- 需要清理和优化数据库文件
"""

import os
import shutil
import sqlite3
from pathlib import Path
from typing import List, Optional
from loguru import logger

from ..core.database.repository import SQLiteRepository


class DatabaseRepairTool:
    """数据库修复工具"""
    
    def __init__(self, output_dir: str):
        """初始化数据库修复工具
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        
    def repair_all_databases(self, target_keyword: Optional[str] = None) -> bool:
        """修复所有数据库或指定关键词数据库
        
        Args:
            target_keyword: 目标关键词，None表示修复所有数据库
            
        Returns:
            是否修复成功
        """
        logger.info("🔧 开始数据库修复工具")
        
        if target_keyword:
            # 修复指定关键词
            return self._repair_single_database(target_keyword)
        else:
            # 修复所有关键词
            keywords = self._discover_all_keywords()
            success_count = 0
            
            for keyword in keywords:
                logger.info(f"🔧 修复数据库: {keyword}")
                if self._repair_single_database(keyword):
                    success_count += 1
                else:
                    logger.error(f"❌ 修复失败: {keyword}")
            
            logger.info(f"✅ 数据库修复完成: {success_count}/{len(keywords)} 个数据库修复成功")
            return success_count == len(keywords)
    
    def _repair_single_database(self, keyword: str) -> bool:
        """修复单个关键词数据库
        
        Args:
            keyword: 关键词
            
        Returns:
            是否修复成功
        """
        try:
            logger.info(f"🔧 开始修复数据库: {keyword}")
            
            # 创建Repository
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
            db_path = repository.db_manager.db_path
            
            # 检查数据库文件是否存在
            if not os.path.exists(db_path):
                logger.warning(f"⚠️ 数据库文件不存在: {db_path}")
                return True  # 不存在就不需要修复
            
            logger.info(f"📁 数据库路径: {db_path}")
            
            # 步骤1：备份原数据库
            backup_success = self._backup_database(db_path, keyword)
            if not backup_success:
                logger.error(f"❌ 备份失败，跳过修复: {keyword}")
                return False
            
            # 步骤2：强制WAL检查点
            wal_success = self._force_wal_checkpoint_direct(db_path, keyword)
            if not wal_success:
                logger.error(f"❌ WAL检查点失败: {keyword}")
                return False
            
            # 步骤3：数据库完整性检查
            integrity_success = self._integrity_check_direct(db_path, keyword)
            if not integrity_success:
                logger.error(f"❌ 完整性检查失败: {keyword}")
                return False
            
            # 步骤4：优化数据库
            optimize_success = self._optimize_database_direct(db_path, keyword)
            if not optimize_success:
                logger.warning(f"⚠️ 优化失败，但可以继续: {keyword}")
            
            # 步骤5：验证修复结果
            verify_success = self._verify_repair(repository, keyword)
            if not verify_success:
                logger.error(f"❌ 修复验证失败: {keyword}")
                return False
            
            logger.info(f"✅ 数据库修复成功: {keyword}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 数据库修复异常 {keyword}: {e}")
            return False
    
    def _backup_database(self, db_path: str, keyword: str) -> bool:
        """备份数据库文件"""
        try:
            backup_path = f"{db_path}.backup"
            shutil.copy2(db_path, backup_path)
            logger.info(f"💾 数据库备份完成: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"❌ 数据库备份失败 {keyword}: {e}")
            return False
    
    def _force_wal_checkpoint_direct(self, db_path: str, keyword: str) -> bool:
        """直接操作数据库文件进行WAL检查点"""
        try:
            # 使用直接的SQLite连接
            conn = sqlite3.connect(db_path, timeout=30.0)
            cursor = conn.cursor()
            
            # 强制WAL检查点
            cursor.execute("PRAGMA wal_checkpoint(FULL)")
            result = cursor.fetchone()
            
            if result:
                busy_count, log_size, checkpointed_size = result
                logger.info(f"🔧 WAL检查点完成 {keyword}: busy={busy_count}, log_size={log_size}, checkpointed={checkpointed_size}")
            
            # 确保WAL模式
            cursor.execute("PRAGMA journal_mode=WAL")
            
            # 设置安全的PRAGMA
            cursor.execute("PRAGMA synchronous=FULL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA wal_autocheckpoint=1000")
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ WAL检查点修复成功: {keyword}")
            return True
            
        except Exception as e:
            logger.error(f"❌ WAL检查点修复失败 {keyword}: {e}")
            return False
    
    def _integrity_check_direct(self, db_path: str, keyword: str) -> bool:
        """直接进行数据库完整性检查"""
        try:
            conn = sqlite3.connect(db_path, timeout=30.0)
            cursor = conn.cursor()
            
            # 快速完整性检查
            cursor.execute("PRAGMA quick_check")
            result = cursor.fetchone()
            
            if result and result[0] == "ok":
                logger.info(f"✅ 数据库完整性检查通过: {keyword}")
                conn.close()
                return True
            else:
                logger.error(f"❌ 数据库完整性检查失败 {keyword}: {result}")
                
                # 尝试完整检查
                cursor.execute("PRAGMA integrity_check")
                full_check = cursor.fetchall()
                logger.error(f"完整性检查详情: {full_check[:5]}")  # 只显示前5个错误
                
                conn.close()
                return False
                
        except Exception as e:
            logger.error(f"❌ 数据库完整性检查异常 {keyword}: {e}")
            return False
    
    def _optimize_database_direct(self, db_path: str, keyword: str) -> bool:
        """直接优化数据库"""
        try:
            conn = sqlite3.connect(db_path, timeout=60.0)  # 优化可能需要更长时间
            cursor = conn.cursor()
            
            # 分析数据库
            logger.info(f"🔧 分析数据库统计信息: {keyword}")
            cursor.execute("ANALYZE")
            
            # 清理碎片（可能需要较长时间）
            logger.info(f"🔧 清理数据库碎片: {keyword}")
            cursor.execute("VACUUM")
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ 数据库优化完成: {keyword}")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ 数据库优化失败 {keyword}: {e}")
            return False
    
    def _verify_repair(self, repository: SQLiteRepository, keyword: str) -> bool:
        """验证修复结果"""
        try:
            # 尝试执行一个简单的查询来验证数据库可用性
            with repository._get_session() as session:
                from src.core.database.schema import Pin
                
                # 查询Pin总数
                total_count = session.query(Pin).count()
                logger.info(f"📊 修复验证 {keyword}: 数据库包含 {total_count} 个Pin")
                
                # 查询base64编码Pin数量
                base64_count = session.query(Pin).filter(Pin.id.like('UGlu%')).count()
                logger.info(f"📊 修复验证 {keyword}: 包含 {base64_count} 个base64编码Pin")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ 修复验证失败 {keyword}: {e}")
            return False
    
    def _discover_all_keywords(self) -> List[str]:
        """发现所有关键词目录"""
        keywords = []
        output_path = Path(self.output_dir)
        
        if not output_path.exists():
            logger.warning(f"输出目录不存在: {self.output_dir}")
            return keywords
        
        for item in output_path.iterdir():
            if item.is_dir():
                db_file = item / "pinterest.db"
                if db_file.exists():
                    keywords.append(item.name)
        
        logger.info(f"发现 {len(keywords)} 个关键词数据库")
        return keywords


def main():
    """主函数，用于命令行调用"""
    import argparse
    
    parser = argparse.ArgumentParser(description="数据库修复工具")
    parser.add_argument("--output-dir", default="./output", help="输出目录")
    parser.add_argument("--keyword", help="指定关键词，不指定则修复所有数据库")
    
    args = parser.parse_args()
    
    repair_tool = DatabaseRepairTool(args.output_dir)
    success = repair_tool.repair_all_databases(args.keyword)
    
    if success:
        print("✅ 数据库修复完成")
        return 0
    else:
        print("❌ 数据库修复失败")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
