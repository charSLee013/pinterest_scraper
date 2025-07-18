#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pin处理队列系统 - 生产者-消费者架构

实现分页数据生产者和并发Pin处理消费者，解决内存爆炸和N+1查询问题
"""

import asyncio
import os
import time
from typing import Dict, Any, Optional, List
from loguru import logger

from ..core.database.repository import SQLiteRepository
from ..core.database.schema import check_pin_download_status_filesystem
from .smart_pin_enhancer import SmartPinEnhancer


class PinDataProducer:
    """分页Pin数据生产者 - 避免内存爆炸"""
    
    def __init__(self, repository: SQLiteRepository, keyword: str, page_size: int = 100):
        """初始化生产者
        
        Args:
            repository: 数据库Repository
            keyword: 关键词
            page_size: 每页大小
        """
        self.repository = repository
        self.keyword = keyword
        self.page_size = page_size
        self.total_count = None
        self.current_offset = 0
        self.finished = False
        
    def get_total_count(self) -> int:
        """获取总Pin数量"""
        if self.total_count is None:
            self.total_count = self.repository.get_pin_count_by_query(self.keyword)
        return self.total_count
    
    async def produce_pins(self, queue: asyncio.Queue) -> int:
        """生产Pin数据到队列
        
        Args:
            queue: 任务队列
            
        Returns:
            生产的Pin数量
        """
        total_produced = 0
        
        logger.info(f"开始生产Pin数据: {self.keyword}, 总数: {self.get_total_count()}")
        
        while not self.finished:
            # 分页加载Pin数据
            pins = self.repository.load_pins_by_query(
                self.keyword, 
                limit=self.page_size, 
                offset=self.current_offset
            )
            
            if not pins:
                # 没有更多数据
                self.finished = True
                break
            
            # 将Pin添加到队列
            for pin in pins:
                await queue.put(pin)
                total_produced += 1
            
            logger.debug(f"生产了 {len(pins)} 个Pin (offset: {self.current_offset})")
            
            # 更新偏移量
            self.current_offset += len(pins)
            
            # 如果返回的Pin数量少于页面大小，说明已经到达末尾
            if len(pins) < self.page_size:
                self.finished = True
                break
        
        logger.info(f"Pin生产完成: {total_produced} 个Pin")
        return total_produced


class PinProcessorWorker:
    """Pin处理消费者 - 并发处理Pin增强和下载"""
    
    def __init__(self, worker_id: int, pin_enhancer: SmartPinEnhancer, 
                 header_manager, output_dir: str, keyword: str):
        """初始化消费者
        
        Args:
            worker_id: 工作线程ID
            pin_enhancer: Pin增强器
            header_manager: Header管理器
            output_dir: 输出目录
            keyword: 关键词
        """
        self.worker_id = worker_id
        self.pin_enhancer = pin_enhancer
        self.header_manager = header_manager
        self.output_dir = output_dir
        self.keyword = keyword
        self.processed_count = 0
        self.enhanced_count = 0
        self.downloaded_count = 0
        self.failed_count = 0
        self.running = False
    
    async def process_pins(self, queue: asyncio.Queue, stats_callback=None) -> Dict[str, int]:
        """处理队列中的Pin
        
        Args:
            queue: 任务队列
            stats_callback: 统计回调函数
            
        Returns:
            处理统计信息
        """
        self.running = True
        logger.debug(f"Worker {self.worker_id} 开始处理Pin")
        
        while self.running:
            try:
                # 从队列获取Pin，设置超时避免无限等待
                pin = await asyncio.wait_for(queue.get(), timeout=1.0)
                
                # 处理Pin
                await self._process_single_pin(pin)
                
                # 标记任务完成
                queue.task_done()
                
                # 更新统计
                if stats_callback:
                    await stats_callback(self.worker_id, self.processed_count)
                
            except asyncio.TimeoutError:
                # 超时是正常的，继续循环检查running状态
                continue
            except Exception as e:
                logger.error(f"Worker {self.worker_id} 处理Pin失败: {e}")
                self.failed_count += 1
                # 继续处理下一个Pin
                continue
        
        logger.debug(f"Worker {self.worker_id} 处理完成: {self.processed_count} 个Pin")
        return self.get_stats()
    
    async def _process_single_pin(self, pin: Dict[str, Any]):
        """处理单个Pin"""
        try:
            self.processed_count += 1
            pin_id = pin.get('id', 'unknown')
            
            logger.debug(f"Worker {self.worker_id} 处理Pin: {pin_id}")
            
            # 1. 检查文件系统下载状态
            pin_with_status = check_pin_download_status_filesystem(
                pin, self.output_dir, self.keyword
            )
            
            # 如果已经下载，跳过处理
            if pin_with_status.get('downloaded', False):
                logger.debug(f"Pin {pin_id} 已下载，跳过")
                return
            
            # 2. 智能Pin增强（如果需要）
            enhanced_pin = await self.pin_enhancer.enhance_pin_if_needed(pin, self.keyword)
            if enhanced_pin != pin:
                self.enhanced_count += 1
                logger.debug(f"Pin {pin_id} 已增强")
            
            # 3. 检查是否有可下载的图片URL
            image_urls = self._extract_downloadable_urls(enhanced_pin)
            if not image_urls:
                logger.debug(f"Pin {pin_id} 没有可下载的图片URL")
                return
            
            # 4. 执行图片下载
            headers = self.header_manager.get_headers()
            download_success = await self._download_pin_images(enhanced_pin, headers)
            
            if download_success:
                self.downloaded_count += 1
                logger.debug(f"Pin {pin_id} 下载成功")
            else:
                self.failed_count += 1
                logger.debug(f"Pin {pin_id} 下载失败")
                
        except Exception as e:
            logger.error(f"Worker {self.worker_id} 处理Pin {pin.get('id', 'unknown')} 失败: {e}")
            self.failed_count += 1
    
    def _extract_downloadable_urls(self, pin: Dict[str, Any]) -> List[str]:
        """提取可下载的图片URL"""
        urls = []
        
        # 检查largest_image_url
        largest_url = pin.get('largest_image_url', '').strip()
        if largest_url and largest_url.startswith('http'):
            urls.append(largest_url)
        
        # 检查image_urls字典
        image_urls = pin.get('image_urls', {})
        if isinstance(image_urls, dict):
            for size, url in image_urls.items():
                if url and isinstance(url, str) and url.strip().startswith('http'):
                    if url not in urls:  # 避免重复
                        urls.append(url)
        
        return urls
    
    async def _download_pin_images(self, pin: Dict[str, Any], headers: Dict) -> bool:
        """下载Pin的图片"""
        try:
            pin_id = pin.get('id', '')
            if not pin_id:
                logger.debug("Pin缺少ID，跳过下载")
                return False

            # 提取图片URL
            image_urls = self._extract_downloadable_urls(pin)
            if not image_urls:
                logger.debug(f"Pin {pin_id} 没有可下载的图片URL")
                return False

            # 生成图片保存路径
            images_dir = os.path.join(self.output_dir, self.keyword, "images")
            os.makedirs(images_dir, exist_ok=True)

            # 尝试下载图片
            for url in image_urls:
                try:
                    # 生成文件名
                    file_extension = self._get_file_extension(url)
                    image_path = os.path.join(images_dir, f"{pin_id}{file_extension}")

                    # 检查文件是否已存在
                    if os.path.exists(image_path) and os.path.getsize(image_path) > 1024:
                        logger.debug(f"Pin {pin_id} 图片已存在，跳过下载")
                        return True

                    # 模拟下载（实际应该调用真实的下载函数）
                    # TODO: 集成实际的图片下载逻辑
                    await asyncio.sleep(0.01)  # 模拟下载时间

                    # 模拟下载成功
                    logger.debug(f"Pin {pin_id} 下载成功（模拟）")
                    return True

                except Exception as e:
                    logger.debug(f"Pin {pin_id} URL {url} 下载失败: {e}")
                    continue

            # 所有URL都失败
            logger.debug(f"Pin {pin_id} 所有URL下载失败")
            return False

        except Exception as e:
            logger.error(f"下载Pin {pin.get('id', 'unknown')} 图片失败: {e}")
            return False

    def _get_file_extension(self, url: str) -> str:
        """从URL获取文件扩展名"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            path = parsed.path.lower()

            if path.endswith('.jpg') or path.endswith('.jpeg'):
                return '.jpg'
            elif path.endswith('.png'):
                return '.png'
            elif path.endswith('.webp'):
                return '.webp'
            elif path.endswith('.gif'):
                return '.gif'
            else:
                return '.jpg'  # 默认扩展名
        except:
            return '.jpg'
    
    def stop(self):
        """停止处理"""
        self.running = False
    
    def get_stats(self) -> Dict[str, int]:
        """获取处理统计"""
        return {
            'worker_id': self.worker_id,
            'processed': self.processed_count,
            'enhanced': self.enhanced_count,
            'downloaded': self.downloaded_count,
            'failed': self.failed_count
        }


class TaskQueueManager:
    """任务队列管理器 - 协调生产者和消费者"""
    
    def __init__(self, max_workers: int = 8, queue_size: int = 200):
        """初始化队列管理器
        
        Args:
            max_workers: 最大工作线程数
            queue_size: 队列大小
        """
        self.max_workers = max_workers
        self.queue_size = queue_size
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.workers = []
        self.producer = None
        self.running = False
        self.stats = {
            'total_pins': 0,
            'processed_pins': 0,
            'enhanced_pins': 0,
            'downloaded_pins': 0,
            'failed_pins': 0,
            'start_time': None,
            'end_time': None
        }
    
    async def start_processing(self, keyword: str, output_dir: str, 
                             pin_enhancer: SmartPinEnhancer, header_manager) -> Dict[str, Any]:
        """开始处理流程
        
        Args:
            keyword: 关键词
            output_dir: 输出目录
            pin_enhancer: Pin增强器
            header_manager: Header管理器
            
        Returns:
            处理统计信息
        """
        self.running = True
        self.stats['start_time'] = time.time()
        
        logger.info(f"开始队列处理: {keyword}, 工作线程数: {self.max_workers}")
        
        try:
            # 创建Repository
            repository = SQLiteRepository(keyword=keyword, output_dir=output_dir)
            
            # 创建生产者
            self.producer = PinDataProducer(repository, keyword)
            self.stats['total_pins'] = self.producer.get_total_count()
            
            # 创建消费者工作线程
            for i in range(self.max_workers):
                worker = PinProcessorWorker(
                    worker_id=i,
                    pin_enhancer=pin_enhancer,
                    header_manager=header_manager,
                    output_dir=output_dir,
                    keyword=keyword
                )
                self.workers.append(worker)
            
            # 启动生产者和消费者
            producer_task = asyncio.create_task(self.producer.produce_pins(self.queue))
            worker_tasks = [
                asyncio.create_task(worker.process_pins(self.queue, self._update_stats))
                for worker in self.workers
            ]
            
            # 等待生产者完成
            await producer_task
            
            # 等待队列中的所有任务完成
            await self.queue.join()
            
            # 停止所有工作线程
            for worker in self.workers:
                worker.stop()
            
            # 等待工作线程结束
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            
            # 收集统计信息
            await self._collect_final_stats()
            
            self.stats['end_time'] = time.time()
            
            logger.info(f"队列处理完成: {self.stats}")
            return self.stats
            
        except Exception as e:
            logger.error(f"队列处理失败: {e}")
            await self.stop_processing()
            raise
    
    async def _update_stats(self, worker_id: int, processed_count: int):
        """更新统计信息"""
        # 这里可以实现实时统计更新
        pass
    
    async def _collect_final_stats(self):
        """收集最终统计信息"""
        for worker in self.workers:
            worker_stats = worker.get_stats()
            self.stats['processed_pins'] += worker_stats['processed']
            self.stats['enhanced_pins'] += worker_stats['enhanced']
            self.stats['downloaded_pins'] += worker_stats['downloaded']
            self.stats['failed_pins'] += worker_stats['failed']
    
    async def stop_processing(self):
        """停止处理"""
        self.running = False
        
        # 停止所有工作线程
        for worker in self.workers:
            worker.stop()
        
        logger.info("队列处理已停止")
