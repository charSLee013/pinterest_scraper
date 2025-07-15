#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
实时Base64转换器（多核加速版）

在保证单Pin事务原子性和随时可中断特性的前提下，
充分利用多核优势加速转换过程。

核心特性：
1. 批量获取待转换Pin列表（减少数据库查询次数）
2. 多线程并发处理（充分利用多核CPU）
3. 每个Pin仍保持独立的原子事务
4. 支持优雅中断和进度同步
5. Windows兼容的单行进度显示
"""

import asyncio
import base64
import json
import hashlib
import time
import sys
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from multiprocessing import Manager, Value, Lock
from typing import Dict, List, Optional, Tuple
from loguru import logger

from ..core.database.repository import SQLiteRepository
from ..utils.progress_display import WindowsProgressDisplay


class RealtimeBase64Converter:
    """实时Base64转换器（多核加速版）

    特点：
    1. 批量获取待转换Pin（减少数据库查询）
    2. 多线程并发处理（充分利用多核CPU）
    3. 保持单Pin原子事务
    4. 支持优雅中断
    5. 实时进度同步
    6. Windows兼容的单行进度显示
    """
    
    def __init__(self, output_dir: str, max_workers: int = None):
        """初始化实时Base64转换器

        Args:
            output_dir: 输出目录
            max_workers: 最大工作线程数，None表示自动检测
        """
        self.output_dir = output_dir
        self.max_workers = max_workers or min(8, (os.cpu_count() or 1) + 4)
        
        # 中断控制
        self._stop_event = threading.Event()
        self._setup_signal_handlers()
        
        # 统计信息
        self.conversion_stats = {
            "total_converted": 0,
            "total_failed": 0,
            "current_keyword": "",
            "keywords_processed": 0
        }
    
    def _setup_signal_handlers(self):
        """设置信号处理器，支持优雅中断"""
        def signal_handler(signum, frame):
            logger.info("🛑 接收到中断信号，正在优雅停止...")
            self._stop_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def process_all_databases(self, target_keyword: Optional[str] = None) -> Dict[str, int]:
        """处理所有数据库或指定关键词数据库
        
        Args:
            target_keyword: 目标关键词，None表示处理所有数据库
            
        Returns:
            转换统计信息
        """
        logger.info("🚀 开始加速Base64转换阶段")
        
        if target_keyword:
            # 处理指定关键词
            await self._process_single_database_accelerated(target_keyword)
        else:
            # 处理所有关键词
            keywords = self._discover_all_keywords()
            for keyword in keywords:
                if self._stop_event.is_set():
                    logger.info("🛑 检测到中断信号，停止处理")
                    break
                await self._process_single_database_accelerated(keyword)
                self.conversion_stats["keywords_processed"] += 1
        
        logger.info(f"✅ 加速Base64转换阶段完成: {self.conversion_stats}")
        return self.conversion_stats
    
    async def _process_single_database_accelerated(self, keyword: str) -> bool:
        """加速处理单个关键词数据库
        
        Args:
            keyword: 关键词
            
        Returns:
            是否处理成功
        """
        logger.info(f"🚀 开始加速处理关键词: {keyword}")
        self.conversion_stats["current_keyword"] = keyword
        
        try:
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
            
            # 1. 批量获取所有待转换Pin
            base64_pins = self._get_all_base64_pins(repository)
            if not base64_pins:
                logger.info(f"✅ 关键词 {keyword} 没有base64编码Pin，跳过")
                return True
            
            total_pins = len(base64_pins)
            logger.info(f"📊 关键词 {keyword} 发现 {total_pins} 个base64编码Pin，开始并发转换")
            
            # 2. 创建进度显示器
            with WindowsProgressDisplay(
                total=total_pins,
                desc=f"加速转换{keyword}",
                unit="pin"
            ) as progress:
                
                # 3. 并发处理
                conversion_count = await self._process_pins_concurrently(
                    base64_pins, keyword, repository, progress
                )
            
            logger.info(f"✅ 关键词 {keyword} 加速转换完成，共转换 {conversion_count} 个Pin")
            return True
            
        except Exception as e:
            logger.error(f"❌ 加速处理关键词 {keyword} 失败: {e}")
            return False
    
    def _get_all_base64_pins(self, repository: SQLiteRepository) -> List[Dict]:
        """批量获取所有base64编码Pin
        
        Args:
            repository: 数据库仓库
            
        Returns:
            base64编码Pin列表
        """
        try:
            with repository._get_session() as session:
                from src.core.database.schema import Pin
                
                # 批量查询所有base64编码Pin
                pin_records = session.query(Pin).filter(
                    Pin.id.like('UGlu%')  # base64编码的Pin ID都以'UGlu'开头
                ).all()
                
                pins = []
                for pin_record in pin_records:
                    pins.append({
                        'id': pin_record.id,
                        'title': pin_record.title or '',
                        'description': pin_record.description or '',
                        'creator_name': pin_record.creator_name or '',
                        'creator_id': pin_record.creator_id or '',
                        'board_name': pin_record.board_name or '',
                        'board_id': pin_record.board_id or '',
                        'image_urls': pin_record.image_urls or '{}',
                        'largest_image_url': pin_record.largest_image_url or '',
                        'stats': pin_record.stats or '{}',
                        'raw_data': pin_record.raw_data or '{}',
                        'query': pin_record.query or ''
                    })
                
                return pins
                
        except Exception as e:
            logger.error(f"批量获取base64编码Pin失败: {e}")
            return []
    
    async def _process_pins_concurrently(self, pins: List[Dict], keyword: str, 
                                       repository: SQLiteRepository, 
                                       progress: WindowsProgressDisplay) -> int:
        """并发处理Pin列表
        
        Args:
            pins: Pin列表
            keyword: 关键词
            repository: 数据库仓库
            progress: 进度显示器
            
        Returns:
            成功转换的Pin数量
        """
        conversion_count = 0
        
        # 使用线程池并发处理（数据库I/O密集型任务适合线程池）
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_pin = {
                executor.submit(self._convert_single_pin_sync, pin, keyword): pin
                for pin in pins
            }
            
            # 收集结果
            for future in as_completed(future_to_pin):
                if self._stop_event.is_set():
                    logger.info("🛑 检测到中断信号，停止并发处理")
                    break
                
                pin = future_to_pin[future]
                try:
                    success = future.result()
                    if success:
                        conversion_count += 1
                        self.conversion_stats["total_converted"] += 1
                    else:
                        self.conversion_stats["total_failed"] += 1
                    
                    # 更新进度
                    progress.update(1)
                    
                except Exception as e:
                    logger.error(f"Pin转换任务执行失败: {e}")
                    self.conversion_stats["total_failed"] += 1
                    progress.update(1)
        
        return conversion_count
    
    def _convert_single_pin_sync(self, pin: Dict, keyword: str) -> bool:
        """同步转换单个Pin（线程安全版本）
        
        Args:
            pin: Pin数据
            keyword: 关键词
            
        Returns:
            是否转换成功
        """
        try:
            # 为每个线程创建独立的数据库连接
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
            
            # 1. 解码Pin ID
            decoded_id = self._decode_base64_pin_id(pin['id'])
            if not decoded_id:
                return False
            
            # 2. 执行原子事务
            return self._atomic_pin_id_conversion_sync(pin, decoded_id, keyword, repository)
            
        except Exception as e:
            logger.error(f"转换Pin {pin['id']} 失败: {e}")
            return False
    
    def _atomic_pin_id_conversion_sync(self, old_pin: Dict, new_pin_id: str, 
                                     keyword: str, repository: SQLiteRepository) -> bool:
        """同步原子性Pin ID转换操作
        
        Args:
            old_pin: 旧Pin数据
            new_pin_id: 新Pin ID
            keyword: 关键词
            repository: 数据库仓库
            
        Returns:
            是否转换成功
        """
        try:
            with repository._get_session() as session:
                from src.core.database.schema import Pin
                
                # 开始事务
                session.begin()
                
                try:
                    # 1. 检查新Pin ID是否已存在
                    existing_pin = session.query(Pin).filter_by(id=new_pin_id).first()
                    if existing_pin:
                        # 如果新Pin ID已存在，只删除旧的base64编码Pin
                        deleted_count = session.query(Pin).filter_by(id=old_pin['id']).delete()
                        if deleted_count > 0:
                            session.commit()
                            return True
                        else:
                            session.rollback()
                            return False
                    
                    # 2. 删除旧记录
                    deleted_count = session.query(Pin).filter_by(id=old_pin['id']).delete()
                    if deleted_count == 0:
                        session.rollback()
                        return False
                    
                    # 3. 创建新Pin记录
                    pin_hash = hashlib.md5(f"{new_pin_id}_{keyword}".encode('utf-8')).hexdigest()
                    
                    new_pin = Pin(
                        id=new_pin_id,
                        pin_hash=pin_hash,
                        title=old_pin.get('title', ''),
                        description=old_pin.get('description', ''),
                        creator_name=old_pin.get('creator_name', ''),
                        creator_id=old_pin.get('creator_id', ''),
                        board_name=old_pin.get('board_name', ''),
                        board_id=old_pin.get('board_id', ''),
                        image_urls=old_pin.get('image_urls', '{}'),
                        largest_image_url=old_pin.get('largest_image_url', ''),
                        stats=old_pin.get('stats', '{}'),
                        raw_data=old_pin.get('raw_data', '{}'),
                        query=keyword
                    )
                    
                    # 4. 插入新记录
                    session.add(new_pin)
                    
                    # 5. 提交事务
                    session.commit()
                    return True
                    
                except Exception as e:
                    session.rollback()
                    logger.error(f"Pin ID转换事务失败: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"原子性Pin替换失败: {e}")
            return False
    
    def _decode_base64_pin_id(self, encoded_pin_id: str) -> Optional[str]:
        """解码base64编码的Pin ID
        
        Args:
            encoded_pin_id: base64编码的Pin ID
            
        Returns:
            解码后的数字Pin ID，失败返回None
        """
        try:
            if encoded_pin_id.startswith('UGlu'):
                decoded = base64.b64decode(encoded_pin_id).decode('utf-8')
                if decoded.startswith('Pin:'):
                    return decoded[4:]
            return None
        except Exception as e:
            return None
    
    def _discover_all_keywords(self) -> List[str]:
        """发现所有关键词"""
        # 这里复用原有的发现逻辑
        from pathlib import Path
        keywords = []
        output_path = Path(self.output_dir)
        
        if output_path.exists():
            for keyword_dir in output_path.iterdir():
                if keyword_dir.is_dir():
                    db_file = keyword_dir / "pinterest.db"
                    if db_file.exists():
                        keywords.append(keyword_dir.name)
        
        return keywords
