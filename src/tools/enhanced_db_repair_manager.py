#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
增强数据库修复管理器

专门用于处理base64编码Pin的转换和数据库修复工作流程。
实现原子性事务和完整的Pin数据增强。
"""

import asyncio
import os
from typing import Dict, List, Optional, Any
from pathlib import Path
from loguru import logger

from ..core.database.repository import SQLiteRepository
from ..utils.transactional_pin_updater import TransactionalPinUpdater


class EnhancedDatabaseRepairManager:
    """增强数据库修复管理器
    
    负责协调完整的数据库修复工作流程：
    1. 检测需要修复的数据库
    2. 转换base64编码的Pin ID
    3. 增强缺少图片URL的Pin
    4. 验证修复完整性
    """
    
    def __init__(self, output_dir: str, backup: bool = True):
        """初始化修复管理器
        
        Args:
            output_dir: 输出目录
            backup: 是否创建备份
        """
        self.output_dir = output_dir
        self.backup = backup
        self.stats = {
            "databases_processed": 0,
            "total_pins": 0,
            "base64_converted": 0,
            "pins_enhanced": 0,
            "failed_conversions": 0,
            "databases_with_issues": 0
        }
    
    async def repair_all_databases(self, target_keyword: Optional[str] = None) -> Dict[str, Any]:
        """修复所有数据库或指定关键词数据库
        
        Args:
            target_keyword: 目标关键词，None表示处理所有数据库
            
        Returns:
            修复统计信息
        """
        logger.info("开始增强数据库修复流程")
        
        # 发现需要修复的数据库
        databases_to_repair = self.discover_databases_needing_repair(target_keyword)
        
        if not databases_to_repair:
            logger.info("未发现需要修复的数据库")
            return self.stats
        
        logger.info(f"发现 {len(databases_to_repair)} 个数据库需要修复")
        
        # 逐个修复数据库
        for db_info in databases_to_repair:
            keyword = db_info['keyword']
            logger.info(f"开始修复数据库: {keyword}")
            
            try:
                repair_stats = await self.repair_single_database(keyword)
                
                # 更新总体统计
                self.stats["databases_processed"] += 1
                self.stats["total_pins"] += repair_stats.get("total_pins", 0)
                self.stats["base64_converted"] += repair_stats.get("base64_converted", 0)
                self.stats["pins_enhanced"] += repair_stats.get("pins_enhanced", 0)
                self.stats["failed_conversions"] += repair_stats.get("failed_conversions", 0)
                
                if repair_stats.get("base64_converted", 0) > 0 or repair_stats.get("pins_enhanced", 0) > 0:
                    self.stats["databases_with_issues"] += 1
                
                logger.info(f"数据库 {keyword} 修复完成: {repair_stats}")
                
            except Exception as e:
                logger.error(f"修复数据库 {keyword} 失败: {e}")
                self.stats["failed_conversions"] += 1
        
        logger.info(f"增强数据库修复完成: {self.stats}")
        return self.stats
    
    async def repair_single_database(self, keyword: str) -> Dict[str, Any]:
        """修复单个关键词数据库
        
        Args:
            keyword: 关键词
            
        Returns:
            修复统计信息
        """
        try:
            # 初始化Repository和TransactionalPinUpdater
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
            updater = TransactionalPinUpdater(repository)
            
            # 执行完整的数据库修复流程
            repair_stats = await updater.repair_database_with_base64_conversion(keyword)
            
            return repair_stats
            
        except Exception as e:
            logger.error(f"修复数据库 {keyword} 时出错: {e}")
            return {
                "total_pins": 0,
                "base64_converted": 0,
                "pins_enhanced": 0,
                "failed_conversions": 1,
                "error": str(e)
            }
    
    def discover_databases_needing_repair(self, target_keyword: Optional[str] = None) -> List[Dict[str, str]]:
        """发现需要修复的数据库
        
        Args:
            target_keyword: 目标关键词，None表示扫描所有数据库
            
        Returns:
            需要修复的数据库信息列表
        """
        databases = []
        output_path = Path(self.output_dir)
        
        if not output_path.exists():
            logger.warning(f"输出目录不存在: {self.output_dir}")
            return databases
        
        # 如果指定了关键词，只检查该关键词
        if target_keyword:
            keyword_dir = output_path / target_keyword
            if keyword_dir.exists():
                db_file = keyword_dir / "pinterest.db"
                if db_file.exists():
                    if self._database_needs_repair(str(db_file), target_keyword):
                        databases.append({
                            'keyword': target_keyword,
                            'db_path': str(db_file),
                            'images_dir': str(keyword_dir / "images")
                        })
            return databases
        
        # 扫描所有关键词目录
        for keyword_dir in output_path.iterdir():
            if keyword_dir.is_dir():
                keyword = keyword_dir.name
                db_file = keyword_dir / "pinterest.db"
                
                if db_file.exists():
                    if self._database_needs_repair(str(db_file), keyword):
                        databases.append({
                            'keyword': keyword,
                            'db_path': str(db_file),
                            'images_dir': str(keyword_dir / "images")
                        })
        
        return databases
    
    def _database_needs_repair(self, db_path: str, keyword: str) -> bool:
        """检查数据库是否需要修复
        
        Args:
            db_path: 数据库文件路径
            keyword: 关键词
            
        Returns:
            是否需要修复
        """
        try:
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
            
            # 快速检查：加载少量Pin数据进行分析
            pins = repository.load_pins_by_query(keyword, limit=100)
            
            for pin in pins:
                pin_id = pin.get('id', '')
                image_urls = pin.get('image_urls', {})
                largest_image_url = pin.get('largest_image_url', '')
                
                # 检查是否有base64编码的Pin ID
                if pin_id.startswith('UGlu'):
                    logger.debug(f"发现base64编码Pin: {pin_id}")
                    return True
                
                # 检查是否缺少图片URL
                if not largest_image_url and not image_urls:
                    logger.debug(f"发现缺少图片URL的Pin: {pin_id}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"检查数据库 {keyword} 时出错: {e}")
            return False
