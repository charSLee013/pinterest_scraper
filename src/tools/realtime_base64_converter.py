#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批量-并行-原子化Base64转换器

全新架构设计，完全解决数据库损坏问题的同时保持高性能：

核心架构：
1. 【数据库操作单线程化】：批量读取 → 原子写入，完全消除并发写入风险
2. 【计算任务多线程化】：Base64解码等CPU密集型操作并行处理
3. 【批量原子事务】：每批次作为一个完整事务，确保数据一致性
4. 【优雅中断支持】：批次间检查中断信号，当前批次完成后安全退出
5. 【性能优化】：批次大小可配置，充分利用多核CPU优势

技术特性：
- 数据库安全：单线程串行操作SQLite，零损坏风险
- 高性能计算：多线程并行Base64解码和数据转换
- 原子性保证：批次级事务，要么全部成功要么全部失败
- 中断安全：批次边界检查中断，确保数据完整性
- 内存控制：批次大小限制，避免内存溢出
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
import tempfile
import shutil
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, NamedTuple
from dataclasses import dataclass
from pathlib import Path
from loguru import logger

from ..core.database.repository import SQLiteRepository
from ..utils.progress_display import WindowsProgressDisplay


@dataclass
class ConversionBatch:
    """转换批次数据结构"""
    batch_id: int
    pins: List[Dict]
    keyword: str

    def __len__(self):
        return len(self.pins)


@dataclass
class ConversionResult:
    """单个Pin转换结果"""
    original_pin: Dict
    decoded_id: Optional[str]
    success: bool
    error_message: Optional[str] = None


class BatchAtomicBase64Converter:
    """批量-并行-原子化Base64转换器

    新架构特点：
    1. 【数据库安全】：单线程批量读取和原子写入，零损坏风险
    2. 【高性能计算】：多线程并行Base64解码，充分利用多核
    3. 【批量事务】：每批次作为原子事务，确保数据一致性
    4. 【优雅中断】：批次边界检查中断，安全退出
    5. 【内存控制】：可配置批次大小，避免内存溢出
    6. 【进度可视】：实时显示批次处理进度
    """

    def __init__(self, output_dir: str, batch_size: int = 4096, max_workers: int = None):
        """初始化批量原子转换器

        Args:
            output_dir: 输出目录
            batch_size: 批次大小（每批处理的Pin数量）
            max_workers: 计算线程数，None表示自动检测CPU核心数
        """
        self.output_dir = output_dir
        # 激进优化：大幅增加批次大小上限以实现2倍性能提升
        self.batch_size = max(1, min(batch_size, 8192))  # 提升批次大小上限到8192
        # 激进优化：大幅提升并发数以实现2倍性能提升
        cpu_cores = os.cpu_count() or 1
        # 对于Base64解码这种CPU密集型任务，使用更高的并发数
        aggressive_workers = min(32, cpu_cores * 4)  # 提升到CPU核心数×4，上限32
        self.max_workers = max_workers or aggressive_workers

        # 使用全局中断管理器，不设置自己的信号处理器
        from .stage_manager import _global_interrupt_manager
        self.interrupt_manager = _global_interrupt_manager

        # 统计信息
        self.conversion_stats = {
            "total_converted": 0,
            "total_failed": 0,
            "total_batches": 0,
            "current_keyword": "",
            "keywords_processed": 0,
            "batch_size": self.batch_size,
            "max_workers": self.max_workers
        }

        logger.info(f"🚀 初始化批量原子转换器（激进性能优化版 - 目标2倍性能提升）")
        logger.info(f"   - 批次大小: {self.batch_size} pins/batch (激进优化: 默认4096, 上限8192)")
        logger.info(f"   - 计算线程: {self.max_workers} threads (激进优化: CPU核心数×4, 上限32)")
        logger.info(f"   - 事务优化: 动态批量提交 (500-1000个Pin/批次)")
        logger.info(f"   - 数据库优化: SQLite性能参数调优 (64MB缓存, 内存映射)")
        logger.info(f"   - 动态调优: 根据数据集大小自动优化参数")
        logger.info(f"   - 架构模式: 批量读取 → 高并发转换 → 原子写入")
    

    
    async def process_all_databases(self, target_keyword: Optional[str] = None) -> Dict[str, int]:
        """处理所有数据库或指定关键词数据库
        
        Args:
            target_keyword: 目标关键词，None表示处理所有数据库
            
        Returns:
            转换统计信息
        """
        logger.info("🚀 开始批量-并行-原子化Base64转换")

        if target_keyword:
            # 处理指定关键词
            await self._process_single_database_batch_atomic(target_keyword)
        else:
            # 处理所有关键词
            keywords = self._discover_all_keywords()
            for keyword in keywords:
                if self.interrupt_manager.is_interrupted():
                    logger.info("🛑 检测到中断信号，停止处理")
                    raise KeyboardInterrupt("Base64转换被用户中断")
                await self._process_single_database_batch_atomic(keyword)
                self.conversion_stats["keywords_processed"] += 1

        logger.info(f"✅ 批量原子转换完成: {self.conversion_stats}")
        return self.conversion_stats
    
    async def _process_single_database_batch_atomic(self, keyword: str) -> bool:
        """批量原子处理单个关键词数据库

        新架构流程：
        0. 数据库健康检查和修复
        1. 单线程批量读取base64编码Pin
        2. 多线程并行转换（纯计算任务）
        3. 单线程原子批量写入数据库
        4. 批次边界检查中断信号

        Args:
            keyword: 关键词

        Returns:
            是否处理成功
        """
        try:
            logger.info(f"� 开始批量原子处理关键词: {keyword}")
            self.conversion_stats["current_keyword"] = keyword

            # 创建Repository（仅用于单线程数据库操作）
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)

            # 【阶段0】数据库健康检查和修复
            if not await self._check_and_repair_database(repository, keyword):
                logger.error(f"❌ 数据库健康检查失败，跳过关键词: {keyword}")
                return False

            # 【阶段1】单线程批量读取所有base64编码Pin
            all_base64_pins = self._get_all_base64_pins(repository)

            if not all_base64_pins:
                logger.info(f"✅ 关键词 {keyword}: 没有发现base64编码Pin")
                return True

            total_batches = (len(all_base64_pins) + self.batch_size - 1) // self.batch_size
            logger.info(f"📊 关键词 {keyword}: 发现 {len(all_base64_pins)} 个base64编码Pin")
            logger.info(f"📦 将分为 {total_batches} 个批次处理 (批次大小: {self.batch_size})")

            # 【阶段2】分批次处理：批量读取 → 并行转换 → 原子写入
            total_converted = await self._process_batches_atomic(all_base64_pins, keyword, repository)

            logger.info(f"✅ 关键词 {keyword}: 批量原子转换完成 {total_converted}/{len(all_base64_pins)} 个Pin")
            return True

        except Exception as e:
            logger.error(f"❌ 批量原子处理关键词 {keyword} 失败: {e}")
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
                
                logger.debug(f"【单线程读取】批量加载了 {len(pins)} 个base64编码Pin")
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

            try:
                # 收集结果
                for future in as_completed(future_to_pin):
                    if self.interrupt_manager.is_interrupted():
                        logger.info("🛑 检测到中断信号，正在取消剩余任务...")

                        # 取消所有未完成的任务
                        cancelled_count = 0
                        for f in future_to_pin:
                            if not f.done():
                                if f.cancel():
                                    cancelled_count += 1

                        logger.info(f"🛑 已取消 {cancelled_count} 个未完成的任务")

                        # 抛出KeyboardInterrupt以终止处理
                        raise KeyboardInterrupt("Base64转换被用户中断")

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

            except KeyboardInterrupt:
                # 处理额外的中断信号
                logger.info("🛑 接收到额外中断信号，立即停止")
                raise  # 重新抛出KeyboardInterrupt

        # 如果检测到中断信号，强制关闭线程池
        if self.interrupt_manager.is_interrupted():
            logger.info("🛑 强制关闭线程池，不等待剩余任务")
            # 注意：这里不能调用executor.shutdown()，因为已经退出with语句
        
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
            # 检查是否需要停止
            if self.interrupt_manager.is_interrupted():
                raise KeyboardInterrupt("Base64转换被用户中断")

            # 为每个线程创建独立的数据库连接
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)

            # 1. 解码Pin ID
            decoded_id = self._decode_base64_pin_id(pin['id'])
            if not decoded_id:
                return False

            # 再次检查是否需要停止
            if self.interrupt_manager.is_interrupted():
                raise KeyboardInterrupt("Base64转换被用户中断")

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
            # 检查是否需要停止
            if self.interrupt_manager.is_interrupted():
                raise KeyboardInterrupt("Base64转换被用户中断")

            with repository._get_session() as session:
                from src.core.database.schema import Pin

                # 开始事务
                session.begin()

                try:
                    # 再次检查是否需要停止
                    if self.interrupt_manager.is_interrupted():
                        session.rollback()
                        raise KeyboardInterrupt("Base64转换被用户中断")

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

    # ==================== 新架构：批量-并行-原子化方法 ====================

    def _optimize_batch_size_for_dataset(self, total_pins: int) -> int:
        """根据数据集大小动态优化批次大小

        Args:
            total_pins: 总Pin数量

        Returns:
            优化后的批次大小
        """
        # 激进优化：根据数据集大小动态调整批次大小
        if total_pins >= 10000:
            # 大数据集：使用最大批次大小
            optimized_size = min(8192, self.batch_size * 2)
        elif total_pins >= 5000:
            # 中等数据集：使用较大批次大小
            optimized_size = min(6144, int(self.batch_size * 1.5))
        elif total_pins >= 1000:
            # 小数据集：使用标准批次大小
            optimized_size = self.batch_size
        else:
            # 极小数据集：使用较小批次大小避免开销
            optimized_size = min(1024, self.batch_size)

        logger.debug(f"数据集大小 {total_pins} -> 优化批次大小: {optimized_size}")
        return optimized_size

    async def _process_batches_atomic(self, all_pins: List[Dict], keyword: str,
                                    repository: SQLiteRepository) -> int:
        """【核心方法】分批次原子处理Pin列表

        架构流程：
        1. 将Pin列表分割为批次
        2. 对每个批次：批量读取 → 并行转换 → 原子写入
        3. 在批次边界检查中断信号

        Args:
            all_pins: 所有待转换的Pin列表
            keyword: 关键词
            repository: 数据库仓库

        Returns:
            成功转换的Pin总数
        """
        total_converted = 0

        # 激进优化：根据数据集大小动态优化批次大小
        optimized_batch_size = self._optimize_batch_size_for_dataset(len(all_pins))
        total_batches = (len(all_pins) + optimized_batch_size - 1) // optimized_batch_size

        # 创建进度条
        with WindowsProgressDisplay(
            total=len(all_pins),
            desc=f"批量原子转换{keyword}",
            unit="pin"
        ) as progress:

            # 分批次处理
            for batch_id in range(total_batches):
                # 检查中断信号（在批次边界）
                if self.interrupt_manager.is_interrupted():
                    logger.info(f"🛑 在批次 {batch_id + 1}/{total_batches} 检测到中断信号，安全退出")
                    raise KeyboardInterrupt(f"Base64转换在批次 {batch_id + 1}/{total_batches} 被用户中断")

                # 计算当前批次范围（使用优化后的批次大小）
                start_idx = batch_id * optimized_batch_size
                end_idx = min(start_idx + optimized_batch_size, len(all_pins))
                batch_pins = all_pins[start_idx:end_idx]

                # 创建批次对象
                batch = ConversionBatch(
                    batch_id=batch_id,
                    pins=batch_pins,
                    keyword=keyword
                )

                logger.debug(f"📦 处理批次 {batch_id + 1}/{total_batches}: {len(batch_pins)} 个Pin")

                # 处理单个批次
                batch_converted = await self._process_single_batch_atomic(batch, repository, progress)
                total_converted += batch_converted

                # 更新统计
                self.conversion_stats["total_batches"] += 1

                # 批次间短暂休息，让系统有机会处理其他任务
                await asyncio.sleep(0.01)

        return total_converted

    async def _process_single_batch_atomic(self, batch: ConversionBatch,
                                         repository: SQLiteRepository,
                                         progress: WindowsProgressDisplay) -> int:
        """【原子事务】处理单个批次

        流程：
        1. 多线程并行转换（纯计算任务）
        2. 单线程原子批量写入数据库

        Args:
            batch: 转换批次
            repository: 数据库仓库
            progress: 进度显示器

        Returns:
            成功转换的Pin数量
        """
        try:
            # 【步骤1】多线程并行转换（纯计算任务，不涉及数据库）
            conversion_results = await self._parallel_convert_batch(batch)

            # 【步骤2】单线程原子批量写入数据库
            success_count = self._atomic_batch_write(batch, conversion_results, repository)

            # 更新进度条
            progress.update(len(batch))

            # 更新统计
            self.conversion_stats["total_converted"] += success_count
            self.conversion_stats["total_failed"] += (len(batch) - success_count)

            logger.debug(f"📦 批次 {batch.batch_id}: 成功转换 {success_count}/{len(batch)} 个Pin")
            return success_count

        except Exception as e:
            logger.error(f"❌ 批次 {batch.batch_id} 处理失败: {e}")
            # 更新进度条（即使失败也要更新）
            progress.update(len(batch))
            # 更新失败统计
            self.conversion_stats["total_failed"] += len(batch)
            return 0

    async def _parallel_convert_batch(self, batch: ConversionBatch) -> List[ConversionResult]:
        """【多线程并行】转换批次中的所有Pin（纯计算任务）

        Args:
            batch: 转换批次

        Returns:
            转换结果列表
        """
        conversion_results = []

        # 使用线程池进行并行Base64解码（CPU密集型任务）
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有转换任务
            future_to_pin = {
                executor.submit(self._convert_single_pin_pure, pin): pin
                for pin in batch.pins
            }

            # 收集转换结果
            for future in as_completed(future_to_pin):
                pin = future_to_pin[future]
                try:
                    result = future.result()
                    conversion_results.append(result)
                except Exception as e:
                    # 创建失败结果
                    error_result = ConversionResult(
                        original_pin=pin,
                        decoded_id=None,
                        success=False,
                        error_message=str(e)
                    )
                    conversion_results.append(error_result)

        return conversion_results

    def _convert_single_pin_pure(self, pin: Dict) -> ConversionResult:
        """【纯计算】转换单个Pin（不涉及数据库操作）

        Args:
            pin: Pin数据

        Returns:
            转换结果
        """
        try:
            # Base64解码
            decoded_id = self._decode_base64_pin_id(pin['id'])

            if decoded_id:
                return ConversionResult(
                    original_pin=pin,
                    decoded_id=decoded_id,
                    success=True
                )
            else:
                return ConversionResult(
                    original_pin=pin,
                    decoded_id=None,
                    success=False,
                    error_message="Base64解码失败"
                )

        except Exception as e:
            return ConversionResult(
                original_pin=pin,
                decoded_id=None,
                success=False,
                error_message=str(e)
            )

    def _atomic_batch_write(self, batch: ConversionBatch,
                          conversion_results: List[ConversionResult],
                          repository: SQLiteRepository) -> int:
        """【单线程原子事务】批量写入转换结果到数据库

        Args:
            batch: 转换批次
            conversion_results: 转换结果列表
            repository: 数据库仓库

        Returns:
            成功写入的Pin数量
        """
        success_count = 0

        try:
            with repository._get_session() as session:
                from src.core.database.schema import Pin

                # 开始事务
                session.begin()

                try:
                    # 批量处理所有成功的转换结果
                    for result in conversion_results:
                        if not result.success or not result.decoded_id:
                            continue

                        old_pin = result.original_pin
                        new_pin_id = result.decoded_id

                        # 检查新Pin ID是否已存在
                        existing_pin = session.query(Pin).filter_by(id=new_pin_id).first()
                        if existing_pin:
                            # 如果新Pin ID已存在，只删除旧的base64编码Pin
                            deleted_count = session.query(Pin).filter_by(id=old_pin['id']).delete()
                            if deleted_count > 0:
                                success_count += 1
                            continue

                        # 删除旧记录
                        deleted_count = session.query(Pin).filter_by(id=old_pin['id']).delete()
                        if deleted_count == 0:
                            continue  # 旧记录不存在，跳过

                        # 创建新Pin记录
                        pin_hash = hashlib.md5(f"{new_pin_id}_{batch.keyword}".encode('utf-8')).hexdigest()

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
                            query=batch.keyword
                        )

                        # 插入新记录
                        session.add(new_pin)
                        success_count += 1

                    # 提交整个批次的事务
                    session.commit()
                    logger.debug(f"【原子写入】批次 {batch.batch_id}: 成功写入 {success_count} 个Pin")

                except Exception as e:
                    # 回滚整个批次
                    session.rollback()
                    logger.error(f"❌ 批次 {batch.batch_id} 原子写入失败，已回滚: {e}")
                    success_count = 0

        except Exception as e:
            logger.error(f"❌ 批次 {batch.batch_id} 数据库操作失败: {e}")
            success_count = 0

        return success_count

    async def _check_and_repair_database(self, repository: SQLiteRepository, keyword: str) -> bool:
        """增强的数据库健康检查和自动修复

        自动检测并修复从运行中复制的数据库文件问题：
        1. WAL文件状态不一致
        2. 文件锁定状态
        3. 事务状态不完整
        4. 数据库损坏自动抢救

        Args:
            repository: 数据库仓库
            keyword: 关键词

        Returns:
            是否修复成功
        """
        try:
            logger.info(f"🔍 开始增强数据库健康检查: {keyword}")

            # 步骤1：检测数据库文件状态
            db_path = self._get_database_path(repository, keyword)
            if not self._detect_database_issues(db_path, keyword):
                logger.warning(f"⚠️ 检测到数据库问题，开始自动修复: {keyword}")

                # 自动修复损坏的数据库
                success = await self._auto_repair_corrupted_database(db_path, keyword)
                if not success:
                    logger.error(f"❌ 自动修复失败: {keyword}")
                    return False

                logger.info(f"✅ 数据库自动修复成功: {keyword}")

            # 步骤2：强制WAL检查点，合并WAL文件到主数据库
            success = self._force_wal_checkpoint(repository, keyword)
            if not success:
                logger.warning(f"⚠️ WAL检查点失败，尝试深度修复: {keyword}")
                success = await self._auto_repair_corrupted_database(db_path, keyword)
                if not success:
                    return False

            # 步骤3：数据库完整性检查
            success = self._integrity_check(repository, keyword)
            if not success:
                logger.warning(f"⚠️ 完整性检查失败，尝试数据抢救: {keyword}")
                success = await self._auto_repair_corrupted_database(db_path, keyword)
                if not success:
                    return False

            # 步骤4：优化数据库（清理碎片，重建索引）
            success = self._optimize_database(repository, keyword)
            if not success:
                logger.warning(f"⚠️ 数据库优化失败，但可以继续: {keyword}")

            logger.info(f"✅ 增强数据库健康检查完成: {keyword}")
            return True

        except Exception as e:
            logger.error(f"❌ 增强数据库健康检查异常 {keyword}: {e}")
            # 尝试最后的自动修复
            try:
                db_path = self._get_database_path(repository, keyword)
                success = await self._auto_repair_corrupted_database(db_path, keyword)
                if success:
                    logger.info(f"✅ 异常恢复成功: {keyword}")
                    return True
            except Exception as repair_e:
                logger.error(f"❌ 异常恢复也失败 {keyword}: {repair_e}")

            return False

    def _get_database_path(self, repository: SQLiteRepository, keyword: str) -> str:
        """获取数据库文件路径

        Args:
            repository: 数据库仓库
            keyword: 关键词

        Returns:
            数据库文件路径
        """
        try:
            if repository.keyword and repository.output_dir:
                # 使用关键词特定的数据库路径
                from ..core.database.manager_factory import DatabaseManagerFactory
                manager = DatabaseManagerFactory.get_manager(repository.keyword, repository.output_dir)
                return manager.db_path
            else:
                # 回退到默认路径构建
                import os
                return os.path.join(repository.output_dir or "./output", keyword, "pinterest.db")
        except Exception as e:
            logger.error(f"获取数据库路径失败 {keyword}: {e}")
            # 最后的回退方案
            import os
            return os.path.join("./output", keyword, "pinterest.db")

    def _detect_database_issues(self, db_path: str, keyword: str) -> bool:
        """检测数据库文件是否存在问题

        Args:
            db_path: 数据库文件路径
            keyword: 关键词

        Returns:
            True表示数据库正常，False表示存在问题需要修复
        """
        try:
            logger.debug(f"🔍 检测数据库文件状态: {keyword}")

            # 检查数据库文件是否存在
            if not os.path.exists(db_path):
                logger.warning(f"⚠️ 数据库文件不存在: {db_path}")
                return True  # 不存在就不需要修复

            # 【新增】检查是否已经存在修复完成的文件
            repaired_ready_file = f"{db_path}.repaired_ready"
            if os.path.exists(repaired_ready_file):
                logger.info(f"✅ 检测到已修复的数据库文件: {keyword}")
                # 尝试使用修复完成的文件替换原文件
                try:
                    # 备份原文件
                    backup_path = f"{db_path}.replaced_backup"
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
                    shutil.move(db_path, backup_path)

                    # 使用修复完成的文件
                    shutil.move(repaired_ready_file, db_path)
                    logger.info(f"✅ 已使用修复完成的数据库: {keyword}")
                    return True
                except Exception as e:
                    logger.warning(f"⚠️ 使用修复文件失败: {e}")
                    # 继续检查原文件

            # 检查WAL和SHM文件状态
            wal_file = f"{db_path}-wal"
            shm_file = f"{db_path}-shm"

            wal_exists = os.path.exists(wal_file)
            shm_exists = os.path.exists(shm_file)

            if wal_exists or shm_exists:
                logger.debug(f"🔍 检测到WAL/SHM文件: WAL={wal_exists}, SHM={shm_exists}")

                # 检查WAL文件大小 - 提高阈值，避免误判
                if wal_exists:
                    wal_size = os.path.getsize(wal_file)
                    # 提高阈值到10MB，避免正常使用中的WAL文件被误判
                    if wal_size > 10 * 1024 * 1024:  # WAL文件大于10MB才认为有问题
                        logger.warning(f"⚠️ WAL文件过大: {wal_size:,} 字节")
                        return False
                    elif wal_size > 1024 * 1024:  # 1-10MB之间给出提示但不修复
                        logger.info(f"ℹ️ WAL文件较大但正常: {wal_size:,} 字节")

            # 尝试快速连接测试
            try:
                conn = sqlite3.connect(db_path, timeout=5.0)
                cursor = conn.cursor()

                # 快速完整性检查
                cursor.execute("PRAGMA quick_check")
                result = cursor.fetchone()

                conn.close()

                if result and result[0] == "ok":
                    logger.debug(f"✅ 数据库快速检查通过: {keyword}")
                    return True
                else:
                    logger.warning(f"⚠️ 数据库快速检查失败: {result}")
                    return False

            except sqlite3.DatabaseError as e:
                logger.warning(f"⚠️ 数据库连接失败: {e}")
                return False
            except Exception as e:
                logger.warning(f"⚠️ 数据库检测异常: {e}")
                return False

        except Exception as e:
            logger.error(f"❌ 数据库问题检测异常 {keyword}: {e}")
            return False

    def _force_wal_checkpoint(self, repository: SQLiteRepository, keyword: str) -> bool:
        """强制WAL检查点，解决从运行中复制数据库的WAL状态问题"""
        try:
            with repository._get_session() as session:
                # 强制WAL检查点，将WAL文件内容合并到主数据库
                from sqlalchemy import text
                result = session.execute(text("PRAGMA wal_checkpoint(FULL)"))
                checkpoint_result = result.fetchone()

                if checkpoint_result:
                    busy_count, log_size, checkpointed_size = checkpoint_result
                    logger.debug(f"🔧 WAL检查点完成 {keyword}: busy={busy_count}, log_size={log_size}, checkpointed={checkpointed_size}")

                # 确保WAL模式仍然启用
                session.execute(text("PRAGMA journal_mode=WAL"))

                return True

        except Exception as e:
            logger.error(f"❌ WAL检查点失败 {keyword}: {e}")
            return False

    def _integrity_check(self, repository: SQLiteRepository, keyword: str) -> bool:
        """数据库完整性检查"""
        try:
            with repository._get_session() as session:
                # 快速完整性检查
                from sqlalchemy import text
                result = session.execute(text("PRAGMA quick_check"))
                check_result = result.fetchone()

                if check_result and check_result[0] == "ok":
                    logger.debug(f"✅ 数据库完整性检查通过: {keyword}")
                    return True
                else:
                    logger.error(f"❌ 数据库完整性检查失败 {keyword}: {check_result}")

                    # 尝试完整检查获取更多信息
                    result = session.execute(text("PRAGMA integrity_check"))
                    full_check = result.fetchall()
                    logger.error(f"完整性检查详情: {full_check}")
                    return False

        except Exception as e:
            logger.error(f"❌ 数据库完整性检查异常 {keyword}: {e}")
            return False

    def _optimize_database(self, repository: SQLiteRepository, keyword: str) -> bool:
        """优化数据库（清理碎片，重建索引）"""
        try:
            with repository._get_session() as session:
                # 分析数据库统计信息
                from sqlalchemy import text
                session.execute(text("ANALYZE"))

                # 清理数据库碎片（可能需要一些时间）
                logger.debug(f"🔧 优化数据库碎片: {keyword}")
                session.execute(text("VACUUM"))

                logger.debug(f"✅ 数据库优化完成: {keyword}")
                return True

        except Exception as e:
            logger.warning(f"⚠️ 数据库优化失败 {keyword}: {e}")
            # 优化失败不是致命错误，可以继续处理
            return False

    async def _auto_repair_corrupted_database(self, db_path: str, keyword: str) -> bool:
        """自动修复损坏的数据库

        集成数据抢救逻辑，自动修复损坏的数据库文件

        Args:
            db_path: 数据库文件路径
            keyword: 关键词

        Returns:
            是否修复成功
        """
        try:
            logger.info(f"🚑 开始自动修复损坏的数据库: {keyword}")
            logger.info(f"📁 数据库路径: {db_path}")

            # 检查文件是否存在
            if not os.path.exists(db_path):
                logger.error(f"❌ 数据库文件不存在: {db_path}")
                return False

            # 创建临时目录用于修复操作
            temp_dir = tempfile.mkdtemp(prefix=f"auto_repair_{keyword}_")
            logger.info(f"📁 临时修复目录: {temp_dir}")

            try:
                # 步骤0：强制关闭所有现有连接
                await self._force_close_all_connections(keyword)

                # 步骤1：多重备份损坏的数据库
                backup_success = self._create_multiple_backups(db_path, keyword)
                if not backup_success:
                    logger.error(f"❌ 备份失败，停止修复: {keyword}")
                    return False

                # 步骤2：自动数据抢救
                recovered_db_path = os.path.join(temp_dir, "recovered.db")
                rescued_count = await self._auto_rescue_data(db_path, recovered_db_path, keyword)

                if rescued_count == 0:
                    logger.error(f"❌ 没有抢救到任何数据: {keyword}")
                    return False

                logger.info(f"✅ 数据抢救成功: {keyword}, 抢救了 {rescued_count:,} 条记录")

                # 步骤3：再次强制关闭连接
                await self._force_close_all_connections(keyword)

                # 步骤4：对抢救的数据执行Base64转换
                logger.info(f"🔄 开始对抢救的数据执行Base64转换: {keyword}")
                converted_db_path = await self._convert_rescued_data_base64(recovered_db_path, keyword)

                if not converted_db_path:
                    logger.error(f"❌ 抢救数据Base64转换失败: {keyword}")
                    return False

                # 步骤5：创建干净的新数据库
                new_db_path = f"{db_path}.repaired"
                create_success = self._create_clean_database_auto(converted_db_path, new_db_path, keyword)

                if not create_success:
                    logger.error(f"❌ 创建新数据库失败: {keyword}")
                    return False

                # 步骤5：最终强制关闭连接
                await self._force_close_all_connections(keyword)

                # 步骤6：安全替换数据库文件
                replace_success = await self._safe_replace_database(db_path, new_db_path, keyword)

                if not replace_success:
                    logger.error(f"❌ 数据库替换失败: {keyword}")
                    return False

                logger.info(f"✅ 数据库自动修复完成: {keyword}")

                # 【新增】最终清理：确保WAL和SHM文件被完全移除
                await self._final_cleanup_auxiliary_files(db_path, keyword)

                return True

            finally:
                # 清理临时目录
                try:
                    shutil.rmtree(temp_dir)
                    logger.debug(f"🗑️ 清理临时目录: {temp_dir}")
                except Exception as e:
                    logger.warning(f"⚠️ 清理临时目录失败: {e}")

        except Exception as e:
            logger.error(f"❌ 自动修复数据库异常 {keyword}: {e}")
            return False

    def _create_multiple_backups(self, db_path: str, keyword: str) -> bool:
        """创建多重备份确保数据安全"""
        try:
            logger.info(f"💾 创建多重备份: {keyword}")

            # 备份1：损坏数据库备份
            backup1_path = f"{db_path}.corrupted_backup"
            shutil.copy2(db_path, backup1_path)
            logger.info(f"💾 损坏数据库备份: {backup1_path}")

            # 备份2：时间戳备份
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup2_path = f"{db_path}.backup_{timestamp}"
            shutil.copy2(db_path, backup2_path)
            logger.info(f"💾 时间戳备份: {backup2_path}")

            return True

        except Exception as e:
            logger.error(f"❌ 创建备份失败 {keyword}: {e}")
            return True

    async def _auto_rescue_data(self, corrupted_db_path: str, recovered_db_path: str, keyword: str) -> int:
        """自动抢救数据（带进度显示）

        Args:
            corrupted_db_path: 损坏的数据库路径
            recovered_db_path: 恢复的数据库路径
            keyword: 关键词

        Returns:
            抢救的记录数量
        """
        try:
            logger.info(f"🚑 开始自动数据抢救: {keyword}")

            # 创建新数据库结构
            recovered_conn = sqlite3.connect(recovered_db_path, timeout=60.0)
            recovered_cursor = recovered_conn.cursor()

            # 创建pins表结构
            recovered_cursor.execute("""
            CREATE TABLE IF NOT EXISTS pins (
                id TEXT PRIMARY KEY,
                pin_hash TEXT,
                title TEXT,
                description TEXT,
                creator_name TEXT,
                creator_id TEXT,
                board_name TEXT,
                board_id TEXT,
                image_urls TEXT,
                largest_image_url TEXT,
                stats TEXT,
                raw_data TEXT,
                query TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            # 尝试从损坏的数据库中读取数据
            try:
                corrupted_conn = sqlite3.connect(corrupted_db_path, timeout=30.0)
                corrupted_cursor = corrupted_conn.cursor()

                # 先统计总记录数（用于进度显示）
                try:
                    corrupted_cursor.execute("SELECT COUNT(*) FROM pins")
                    total_count = corrupted_cursor.fetchone()[0]
                    logger.info(f"📊 预计抢救记录数: {total_count:,}")
                except:
                    total_count = 0
                    logger.warning("⚠️ 无法统计总记录数，使用逐行抢救模式")

                # 开始抢救数据
                logger.info(f"🚑 开始逐行抢救数据: {keyword}")

                # 尝试读取pins表的数据
                corrupted_cursor.execute("SELECT * FROM pins")

                rescued_count = 0
                batch_size = 1000
                batch_data = []

                # 创建进度显示
                if total_count > 0:
                    progress_desc = f"抢救数据{keyword}"
                    with WindowsProgressDisplay(
                        total=total_count,
                        desc=progress_desc,
                        unit="record"
                    ) as progress:

                        while True:
                            try:
                                row = corrupted_cursor.fetchone()
                                if row is None:
                                    break

                                batch_data.append(row)
                                rescued_count += 1

                                # 批量插入提高性能
                                if len(batch_data) >= batch_size:
                                    self._batch_insert_rescued_data(recovered_cursor, batch_data)
                                    batch_data = []
                                    recovered_conn.commit()
                                    progress.update(batch_size)

                            except Exception as e:
                                # 跳过损坏的行
                                logger.debug(f"跳过损坏的行: {e}")
                                continue

                        # 插入剩余数据
                        if batch_data:
                            self._batch_insert_rescued_data(recovered_cursor, batch_data)
                            recovered_conn.commit()
                            progress.update(len(batch_data))
                else:
                    # 无进度条模式
                    while True:
                        try:
                            row = corrupted_cursor.fetchone()
                            if row is None:
                                break

                            batch_data.append(row)
                            rescued_count += 1

                            # 批量插入
                            if len(batch_data) >= batch_size:
                                self._batch_insert_rescued_data(recovered_cursor, batch_data)
                                batch_data = []
                                recovered_conn.commit()

                                if rescued_count % 5000 == 0:
                                    logger.info(f"📊 已抢救 {rescued_count:,} 条记录...")

                        except Exception as e:
                            logger.debug(f"跳过损坏的行: {e}")
                            continue

                    # 插入剩余数据
                    if batch_data:
                        self._batch_insert_rescued_data(recovered_cursor, batch_data)
                        recovered_conn.commit()

                corrupted_conn.close()
                recovered_conn.close()

                logger.info(f"✅ 数据抢救完成: {keyword}, 成功抢救 {rescued_count:,} 条记录")
                return rescued_count

            except Exception as e:
                logger.error(f"❌ 数据抢救失败 {keyword}: {e}")
                recovered_conn.close()
                return 0

        except Exception as e:
            logger.error(f"❌ 自动数据抢救异常 {keyword}: {e}")
            return 0

    def _batch_insert_rescued_data(self, cursor, batch_data: List) -> None:
        """批量插入抢救的数据"""
        try:
            cursor.executemany("""
            INSERT OR REPLACE INTO pins VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
        except Exception as e:
            # 如果批量插入失败，尝试逐行插入
            logger.debug(f"批量插入失败，尝试逐行插入: {e}")
            for row in batch_data:
                try:
                    cursor.execute("""
                    INSERT OR REPLACE INTO pins VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, row)
                except Exception as row_e:
                    logger.debug(f"跳过损坏的行: {row_e}")
                    continue

    def _create_clean_database_auto(self, recovered_db_path: str, new_db_path: str, keyword: str) -> bool:
        """自动创建干净的新数据库"""
        try:
            logger.info(f"🔧 创建干净的新数据库: {keyword}")

            # 复制抢救的数据库
            shutil.copy2(recovered_db_path, new_db_path)

            # 优化新数据库
            conn = sqlite3.connect(new_db_path, timeout=60.0)
            cursor = conn.cursor()

            # 设置安全的PRAGMA
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=FULL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA wal_autocheckpoint=1000")

            # 重建索引
            cursor.execute("REINDEX")

            # 分析数据库
            cursor.execute("ANALYZE")

            # 清理碎片
            logger.info(f"🔧 清理数据库碎片: {keyword}")
            cursor.execute("VACUUM")

            conn.commit()
            conn.close()

            logger.info(f"✅ 干净数据库创建完成: {keyword}")
            return True

        except Exception as e:
            logger.error(f"❌ 创建干净数据库失败 {keyword}: {e}")
            return False

    async def _safe_replace_database(self, original_db_path: str, new_db_path: str, keyword: str) -> bool:
        """安全替换数据库文件（增强的Windows文件锁定处理）"""
        try:
            logger.info(f"🔄 安全替换数据库文件: {keyword}")

            # 尝试多次替换，处理文件锁定问题
            max_retries = 8  # 增加重试次数
            retry_delay = 1.0

            for attempt in range(max_retries):
                try:
                    # 强制关闭所有可能的连接
                    await self._force_close_all_connections(keyword)

                    # 删除WAL和SHM文件
                    wal_file = f"{original_db_path}-wal"
                    shm_file = f"{original_db_path}-shm"
                    journal_file = f"{original_db_path}-journal"

                    for aux_file in [wal_file, shm_file, journal_file]:
                        if os.path.exists(aux_file):
                            try:
                                # 尝试多次删除辅助文件
                                for del_attempt in range(3):
                                    try:
                                        os.remove(aux_file)
                                        logger.debug(f"🗑️ 删除辅助文件: {aux_file}")
                                        break
                                    except Exception as del_e:
                                        if del_attempt < 2:
                                            await asyncio.sleep(0.5)
                                        else:
                                            logger.debug(f"无法删除辅助文件 {aux_file}: {del_e}")
                            except Exception as e:
                                logger.debug(f"删除辅助文件失败 {aux_file}: {e}")

                    # 强制垃圾回收
                    import gc
                    gc.collect()

                    # 等待文件锁释放
                    await asyncio.sleep(retry_delay)

                    # 尝试测试文件是否可以访问
                    try:
                        # 尝试以独占模式打开文件来测试锁定状态
                        with open(original_db_path, 'r+b') as test_file:
                            pass
                        logger.debug(f"✅ 文件锁定测试通过: {keyword}")
                    except Exception as lock_test_e:
                        logger.debug(f"文件仍被锁定: {lock_test_e}")
                        raise lock_test_e

                    # 备份原数据库
                    backup_path = f"{original_db_path}.replaced_backup"
                    if os.path.exists(backup_path):
                        os.remove(backup_path)

                    shutil.move(original_db_path, backup_path)
                    logger.info(f"💾 原数据库已备份: {backup_path}")

                    # 移动新数据库到原位置
                    shutil.move(new_db_path, original_db_path)
                    logger.info(f"✅ 数据库替换成功: {keyword}")

                    # 验证替换结果
                    if os.path.exists(original_db_path):
                        file_size = os.path.getsize(original_db_path)
                        logger.info(f"📊 新数据库文件大小: {file_size:,} 字节")
                        return True
                    else:
                        logger.error(f"❌ 替换后文件不存在: {original_db_path}")
                        return False

                except Exception as e:
                    logger.warning(f"⚠️ 替换尝试 {attempt + 1}/{max_retries} 失败: {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 等待 {retry_delay} 秒后重试...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.5, 10.0)  # 指数退避，最大10秒
                    else:
                        logger.error(f"❌ 所有替换尝试都失败: {keyword}")
                        # 最后尝试：将修复的数据库保存为备用文件
                        try:
                            fallback_path = f"{original_db_path}.repaired_ready"
                            shutil.copy2(new_db_path, fallback_path)
                            logger.info(f"💾 修复的数据库已保存为: {fallback_path}")
                            logger.info(f"🔧 请手动替换数据库文件或重启程序")
                        except Exception as fallback_e:
                            logger.error(f"❌ 保存备用文件也失败: {fallback_e}")
                        return False

            return False

        except Exception as e:
            logger.error(f"❌ 安全替换数据库异常 {keyword}: {e}")
            return False

    async def _force_close_all_connections(self, keyword: str) -> None:
        """强制关闭所有数据库连接

        Args:
            keyword: 关键词
        """
        try:
            logger.debug(f"🔒 强制关闭所有数据库连接: {keyword}")

            # 1. 清理DatabaseManagerFactory中的缓存连接
            try:
                from ..core.database.manager_factory import DatabaseManagerFactory

                # 使用正确的清理方法
                cleanup_success = DatabaseManagerFactory.cleanup_manager(keyword, self.output_dir)
                if cleanup_success:
                    logger.debug(f"🔒 清理管理器缓存成功: {keyword}")
                else:
                    logger.debug(f"🔒 清理管理器缓存失败: {keyword}")

            except Exception as e:
                logger.debug(f"清理管理器缓存失败: {e}")

            # 2. 强制垃圾回收，释放未关闭的连接
            import gc
            gc.collect()

            # 3. 强制执行WAL检查点，确保所有数据写入主文件
            try:
                db_path = os.path.join(self.output_dir, keyword, "pinterest.db")
                if os.path.exists(db_path):
                    # 创建临时连接执行检查点
                    temp_conn = sqlite3.connect(db_path, timeout=5.0)
                    temp_cursor = temp_conn.cursor()
                    temp_cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    temp_conn.close()
                    logger.debug(f"🔄 强制WAL检查点完成: {keyword}")
            except Exception as e:
                logger.debug(f"强制WAL检查点失败: {e}")

            # 4. 等待更长时间让连接完全释放
            await asyncio.sleep(2.0)

            logger.debug(f"✅ 数据库连接强制关闭完成: {keyword}")

        except Exception as e:
            logger.warning(f"⚠️ 强制关闭连接时出错 {keyword}: {e}")

    async def _final_cleanup_auxiliary_files(self, db_path: str, keyword: str) -> None:
        """最终清理：确保WAL和SHM文件被完全移除，避免下次启动误判"""
        try:
            logger.info(f"🧹 开始最终清理辅助文件: {keyword}")

            # 强制WAL检查点，确保所有数据都写入主文件
            try:
                conn = sqlite3.connect(db_path, timeout=10.0)
                cursor = conn.cursor()
                cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                result = cursor.fetchone()
                if result:
                    busy_count, log_size, checkpointed_size = result
                    logger.debug(f"最终WAL检查点: busy={busy_count}, log_size={log_size}, checkpointed={checkpointed_size}")
                conn.close()
                logger.debug(f"✅ 最终WAL检查点完成: {keyword}")
            except Exception as e:
                logger.warning(f"⚠️ 最终WAL检查点失败: {e}")

            # 等待连接完全释放
            await asyncio.sleep(1.0)

            # 删除所有辅助文件
            auxiliary_files = [
                f"{db_path}-wal",
                f"{db_path}-shm",
                f"{db_path}-journal"
            ]

            for aux_file in auxiliary_files:
                if os.path.exists(aux_file):
                    try:
                        # 多次尝试删除
                        for attempt in range(5):
                            try:
                                os.remove(aux_file)
                                logger.info(f"🗑️ 已删除辅助文件: {os.path.basename(aux_file)}")
                                break
                            except Exception as del_e:
                                if attempt < 4:
                                    await asyncio.sleep(0.5)
                                else:
                                    logger.warning(f"⚠️ 无法删除辅助文件 {aux_file}: {del_e}")
                    except Exception as e:
                        logger.warning(f"⚠️ 删除辅助文件失败 {aux_file}: {e}")

            logger.info(f"✅ 最终清理完成: {keyword} - 只保留 pinterest.db")

        except Exception as e:
            logger.warning(f"⚠️ 最终清理辅助文件时出错 {keyword}: {e}")

    async def _convert_rescued_data_base64(self, rescued_db_path: str, keyword: str) -> str:
        """对抢救的数据执行Base64转换（修复UNIQUE constraint问题）

        Args:
            rescued_db_path: 抢救的数据库路径
            keyword: 关键词

        Returns:
            转换后的数据库路径，失败返回None
        """
        try:
            logger.info(f"🔄 开始Base64转换抢救的数据: {keyword}")

            # 连接抢救的数据库
            conn = sqlite3.connect(rescued_db_path, timeout=60.0)
            cursor = conn.cursor()

            # 激进优化：设置SQLite性能优化参数以实现2倍性能提升
            cursor.execute("PRAGMA synchronous = NORMAL")      # 平衡性能和安全性
            cursor.execute("PRAGMA cache_size = -64000")       # 64MB缓存
            cursor.execute("PRAGMA temp_store = MEMORY")       # 临时表存储在内存
            cursor.execute("PRAGMA mmap_size = 268435456")     # 256MB内存映射
            cursor.execute("PRAGMA optimize")                  # 优化查询计划

            # 统计需要转换的base64编码Pin
            cursor.execute("SELECT COUNT(*) FROM pins WHERE id LIKE 'UGlu%'")
            base64_count = cursor.fetchone()[0]

            if base64_count == 0:
                logger.info(f"✅ 抢救的数据无需Base64转换: {keyword}")
                conn.close()
                return rescued_db_path

            logger.info(f"📊 发现 {base64_count:,} 个base64编码Pin需要转换")

            # 阶段1：分析和去重冲突的base64编码
            deduped_count = await self._deduplicate_base64_pins(cursor, keyword)
            logger.info(f"🔧 去重完成: 删除了 {deduped_count} 个重复的base64编码Pin")

            # 阶段2：执行安全的批量转换
            converted_count = await self._safe_batch_conversion(cursor, keyword)

            # 强制完成所有事务并关闭连接
            try:
                # 确保所有事务都已提交
                cursor.execute("COMMIT")
            except:
                pass  # 如果没有活动事务，忽略错误

            try:
                # 执行WAL检查点，确保所有数据写入主数据库文件
                cursor.execute("PRAGMA wal_checkpoint(FULL)")
                logger.debug(f"✅ WAL检查点完成: {keyword}")
            except Exception as e:
                logger.debug(f"WAL检查点失败: {e}")

            # 关闭连接
            conn.close()

            # 额外等待确保连接完全释放
            await asyncio.sleep(0.5)

            logger.info(f"✅ Base64转换完成: {keyword}, 转换了 {converted_count:,} 个Pin")
            return rescued_db_path

        except Exception as e:
            logger.error(f"❌ Base64转换抢救数据失败 {keyword}: {e}")
            return None

    async def _deduplicate_base64_pins(self, cursor, keyword: str) -> int:
        """去重冲突的base64编码Pin

        Args:
            cursor: 数据库游标
            keyword: 关键词

        Returns:
            删除的重复记录数量
        """
        try:
            logger.info(f"🔧 开始分析和去重base64编码Pin: {keyword}")

            # 获取所有base64编码的Pin
            cursor.execute("SELECT id, pin_hash, created_at FROM pins WHERE id LIKE 'UGlu%' ORDER BY created_at DESC")
            base64_pins = cursor.fetchall()

            # 分析解码冲突
            from collections import defaultdict
            conflicts = defaultdict(list)

            for pin_id, pin_hash, created_at in base64_pins:
                try:
                    # 解码base64 Pin ID
                    decoded_bytes = base64.b64decode(pin_id)
                    decoded_str = decoded_bytes.decode('utf-8')

                    if decoded_str.startswith('Pin:'):
                        # 正确处理空格和格式问题
                        real_pin_id = decoded_str[4:].strip()
                        conflicts[real_pin_id].append((pin_id, pin_hash, created_at))

                except Exception as e:
                    logger.debug(f"跳过无效的base64 Pin: {pin_id}, 错误: {e}")
                    continue

            # 处理冲突：保留最新的记录，删除重复的
            deleted_count = 0
            for real_pin_id, pin_records in conflicts.items():
                if len(pin_records) > 1:
                    logger.warning(f"发现冲突: 真实ID '{real_pin_id}' 对应 {len(pin_records)} 个base64编码")

                    # 按创建时间排序，保留最新的
                    pin_records.sort(key=lambda x: x[2], reverse=True)  # 按created_at降序
                    keep_record = pin_records[0]
                    delete_records = pin_records[1:]

                    logger.info(f"保留记录: {keep_record[0]} (最新)")

                    # 删除重复记录
                    for pin_id, pin_hash, created_at in delete_records:
                        cursor.execute("DELETE FROM pins WHERE id = ?", (pin_id,))
                        deleted_count += 1
                        logger.debug(f"删除重复记录: {pin_id}")

            # 提交删除操作
            cursor.connection.commit()

            logger.info(f"✅ 去重完成: 删除了 {deleted_count} 个重复记录")
            return deleted_count

        except Exception as e:
            logger.error(f"❌ 去重失败 {keyword}: {e}")
            cursor.connection.rollback()
            return 0

    async def _safe_batch_conversion(self, cursor, keyword: str) -> int:
        """安全的批量Base64转换

        Args:
            cursor: 数据库游标
            keyword: 关键词

        Returns:
            转换的记录数量
        """
        try:
            logger.info(f"🔄 开始安全批量转换: {keyword}")

            # 重新获取去重后的base64编码Pin
            cursor.execute("SELECT id, pin_hash FROM pins WHERE id LIKE 'UGlu%'")
            base64_pins = cursor.fetchall()

            if not base64_pins:
                logger.info(f"✅ 没有需要转换的base64编码Pin: {keyword}")
                return 0

            converted_count = 0
            # 激进优化：根据数据集大小动态调整提交批次大小
            if len(base64_pins) >= 10000:
                commit_batch_size = 1000  # 大数据集：每1000个Pin提交一次
            elif len(base64_pins) >= 5000:
                commit_batch_size = 750   # 中等数据集：每750个Pin提交一次
            else:
                commit_batch_size = 500   # 小数据集：每500个Pin提交一次
            pending_commits = 0

            with WindowsProgressDisplay(
                total=len(base64_pins),
                desc=f"安全转换{keyword}",
                unit="pin"
            ) as progress:

                # 逐个转换以避免批量冲突
                for pin_id, pin_hash in base64_pins:
                    try:
                        # 解码base64 Pin ID
                        decoded_bytes = base64.b64decode(pin_id)
                        decoded_str = decoded_bytes.decode('utf-8')

                        if decoded_str.startswith('Pin:'):
                            # 正确处理空格和格式问题
                            new_pin_id = decoded_str[4:].strip()
                            new_pin_hash = hashlib.sha256(new_pin_id.encode()).hexdigest()

                            # 使用连接的自动事务管理
                            try:
                                # 检查新Pin ID是否已存在
                                cursor.execute("SELECT COUNT(*) FROM pins WHERE id = ?", (new_pin_id,))
                                exists = cursor.fetchone()[0] > 0

                                if exists:
                                    # 如果新Pin ID已存在，只删除旧的base64编码Pin
                                    cursor.execute("DELETE FROM pins WHERE id = ?", (pin_id,))
                                    logger.debug(f"删除重复的base64 Pin: {pin_id} (目标ID {new_pin_id} 已存在)")
                                else:
                                    # 如果新Pin ID不存在，执行更新
                                    cursor.execute("""
                                    UPDATE pins SET id = ?, pin_hash = ? WHERE id = ?
                                    """, (new_pin_id, new_pin_hash, pin_id))
                                    logger.debug(f"转换成功: {pin_id} -> {new_pin_id}")

                                # 优化：批量提交而不是每个Pin都提交
                                converted_count += 1
                                pending_commits += 1

                                # 每达到批量大小就提交一次
                                if pending_commits >= commit_batch_size:
                                    cursor.connection.commit()
                                    pending_commits = 0

                            except Exception as inner_e:
                                # 回滚事务
                                cursor.connection.rollback()
                                raise inner_e

                    except sqlite3.IntegrityError as e:
                        logger.warning(f"转换冲突 {pin_id}: {e}")
                        continue
                    except Exception as e:
                        logger.debug(f"跳过无效的base64 Pin: {pin_id}, 错误: {e}")
                        continue

                    # 更新进度
                    progress.update(1)

                # 提交任何剩余的待处理事务
                if pending_commits > 0:
                    cursor.connection.commit()
                    logger.debug(f"最终提交剩余 {pending_commits} 个转换")

            logger.info(f"✅ 安全批量转换完成: {keyword}, 转换了 {converted_count:,} 个Pin")
            return converted_count

        except Exception as e:
            logger.error(f"❌ 安全批量转换失败 {keyword}: {e}")
            cursor.connection.rollback()
            return 0


# 为了向后兼容，保留原有的类名作为别名
RealtimeBase64Converter = BatchAtomicBase64Converter
