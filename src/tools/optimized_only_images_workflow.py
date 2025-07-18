#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
重构后的--only-images工作流程

实现完全独立的四阶段处理：
1. 数据库修复与检测阶段：自动检测并修复损坏的数据库文件
2. Base64编码Pin转换阶段：将base64编码转换为真实Pin ID
3. Pin详情数据补全阶段：批量获取缺失的Pin详情信息
4. 图片文件下载阶段：并发下载缺失的图片文件

每个阶段都有独立的数据库连接管理和优雅退出机制。
"""

import os
import json
import time
from typing import Optional, Dict
from loguru import logger

from .stage_manager import WorkflowManager
from .stage_implementations import (
    DatabaseRepairStage,
    Base64ConversionStage,
    PinEnhancementStage,
    ImageDownloadStage
)


class OptimizedOnlyImagesWorkflow:
    """重构后的--only-images工作流程

    实现完全独立的四阶段处理逻辑，确保每个阶段都有独立的连接管理和优雅退出机制
    """

    def __init__(self, output_dir: str, max_concurrent: int = 15, proxy: Optional[str] = None):
        """初始化工作流程

        Args:
            output_dir: 输出目录
            max_concurrent: 最大并发数
            proxy: 代理设置
        """
        self.output_dir = output_dir
        self.max_concurrent = max_concurrent
        self.proxy = proxy

        # 创建工作流程管理器
        self.workflow_manager = WorkflowManager(output_dir)

        logger.info(f"🚀 初始化重构后的--only-images工作流程")
        logger.info(f"   - 输出目录: {output_dir}")
        logger.info(f"   - 最大并发: {max_concurrent}")
        logger.info(f"   - 代理设置: {proxy or '无'}")
        logger.info(f"   - 四阶段独立处理模式")

        # 工作流程统计
        self.workflow_stats = {
            "stage1_database_repair": {},
            "stage2_base64_conversion": {},
            "stage3_pin_enhancement": {},
            "stage4_image_download": {},
            "total_execution_time": 0
        }
    
    async def execute(self, target_keyword: Optional[str] = None) -> Dict:
        """执行优化后的三阶段工作流程
        
        Args:
            target_keyword: 目标关键词，None表示处理所有关键词
            
        Returns:
            工作流程执行结果
        """
        import time
        start_time = time.time()
        
        logger.info("🚀 开始执行优化后的--only-images工作流程")
        logger.info("=" * 60)
        
        try:
            # Phase 1: 实时Base64转换
            phase1_success = await self._execute_phase1_base64_conversion(target_keyword)
            if not phase1_success:
                logger.error("❌ Phase 1 失败，终止工作流程")
                return self._generate_failure_result("Phase 1 失败")
            
            # Phase 2: 全局Header准备
            phase2_success = await self._execute_phase2_header_preparation()
            if not phase2_success:
                logger.warning("⚠️ Phase 2 失败，将使用默认Headers继续")
            
            # Phase 3: 智能下载
            phase3_success = await self._execute_phase3_smart_download(target_keyword)
            if not phase3_success:
                logger.error("❌ Phase 3 失败")
                return self._generate_failure_result("Phase 3 失败")
            
            # 计算总执行时间
            self.workflow_stats["total_execution_time"] = time.time() - start_time
            
            logger.info("=" * 60)
            logger.info("🎉 优化后的--only-images工作流程执行完成")
            
            return self._generate_success_result()
            
        except Exception as e:
            logger.error(f"❌ 工作流程执行失败: {e}")
            return self._generate_failure_result(str(e))
    
    async def _execute_phase1_base64_conversion(self, target_keyword: Optional[str]) -> bool:
        """执行Phase 1: 实时Base64转换
        
        Args:
            target_keyword: 目标关键词
            
        Returns:
            是否执行成功
        """
        logger.info("🔄 Phase 1: 实时Base64转换阶段")
        logger.info("- 逐个检查数据库中的base64编码Pin")
        logger.info("- 每个转换都是原子事务")
        logger.info("- 持续处理直到没有base64编码Pin")
        
        try:
            conversion_stats = await self.base64_converter.process_all_databases(target_keyword)
            self.workflow_stats["phase1_base64_conversion"] = conversion_stats
            
            if conversion_stats["total_converted"] > 0:
                logger.info(f"✅ Phase 1 完成: 转换了 {conversion_stats['total_converted']} 个base64编码Pin")
            else:
                logger.info("✅ Phase 1 完成: 没有发现需要转换的base64编码Pin")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Phase 1 执行失败: {e}")
            return False
    
    async def _execute_phase2_header_preparation(self) -> bool:
        """执行Phase 2: 全局Header准备

        Returns:
            是否执行成功
        """
        logger.info("🌐 Phase 2: 全局Header准备阶段")
        logger.info("- 启动浏览器访问Pinterest官网")
        logger.info("- 获取并缓存浏览器headers")
        logger.info("- 为整个会话提供全局headers")

        try:
            # 强制获取新的headers
            logger.info("正在启动浏览器获取全局Headers...")
            headers_ready = await self.header_manager.ensure_headers_ready()

            header_info = self.header_manager.get_headers_info()
            self.workflow_stats["phase2_header_preparation"] = header_info

            if headers_ready:
                headers = self.header_manager.get_headers()
                logger.info(f"✅ Phase 2 完成: Headers准备就绪")
                logger.info(f"   - Headers数量: {len(headers)} 个字段")
                logger.info(f"   - 包含User-Agent: {'User-Agent' in headers}")
                logger.info(f"   - 包含Cookie: {'Cookie' in headers}")
                return True
            else:
                logger.warning("⚠️ Phase 2 失败: Headers准备失败，将使用默认Headers")
                return False

        except Exception as e:
            logger.error(f"❌ Phase 2 执行失败: {e}")
            return False
    
    async def _execute_phase3_smart_download(self, target_keyword: Optional[str]) -> bool:
        """执行Phase 3: 智能下载
        
        Args:
            target_keyword: 目标关键词
            
        Returns:
            是否执行成功
        """
        logger.info("📥 Phase 3: 智能下载阶段")
        logger.info("- 检查每个Pin是否缺少图片URL")
        logger.info("- 按需获取Pin详情数据")
        logger.info("- 立即更新数据库后进行图片下载")
        logger.info("- 使用全局缓存的headers")
        
        try:
            # 创建智能图片下载器
            smart_downloader = SmartImageDownloader(
                output_dir=self.output_dir,
                max_concurrent=self.max_concurrent,
                proxy=self.proxy,
                header_manager=self.header_manager,
                pin_enhancer=self.pin_enhancer
            )
            
            # 执行智能下载
            if target_keyword:
                download_stats = await smart_downloader.download_keyword_images(target_keyword)
            else:
                download_stats = await smart_downloader.download_all_images()
            
            self.workflow_stats["phase3_smart_download"] = download_stats
            
            logger.info(f"✅ Phase 3 完成: {download_stats}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Phase 3 执行失败: {e}")
            return False
    
    def _generate_success_result(self) -> Dict:
        """生成成功结果
        
        Returns:
            成功结果字典
        """
        return {
            "status": "success",
            "message": "优化后的--only-images工作流程执行成功",
            "stats": self.workflow_stats,
            "phases": {
                "phase1_base64_conversion": "completed",
                "phase2_header_preparation": "completed",
                "phase3_smart_download": "completed"
            }
        }
    
    def _generate_failure_result(self, error_message: str) -> Dict:
        """生成失败结果
        
        Args:
            error_message: 错误消息
            
        Returns:
            失败结果字典
        """
        return {
            "status": "failed",
            "message": f"工作流程执行失败: {error_message}",
            "stats": self.workflow_stats,
            "error": error_message
        }


class SmartImageDownloader:
    """智能图片下载器
    
    集成Pin增强功能的图片下载器
    """
    
    def __init__(self, output_dir: str, max_concurrent: int, proxy: Optional[str], 
                 header_manager: GlobalHeaderManager, pin_enhancer: SmartPinEnhancer):
        """初始化智能下载器
        
        Args:
            output_dir: 输出目录
            max_concurrent: 最大并发数
            proxy: 代理设置
            header_manager: Header管理器
            pin_enhancer: Pin增强器
        """
        self.output_dir = output_dir
        self.max_concurrent = max_concurrent
        self.proxy = proxy
        self.header_manager = header_manager
        self.pin_enhancer = pin_enhancer
        
        # 创建传统的图片下载器
        self.image_downloader = ImageDownloader(
            output_dir=output_dir,
            max_concurrent=max_concurrent,
            proxy=proxy,
            prefer_requests=True
        )
    
    async def download_keyword_images(self, keyword: str) -> Dict:
        """下载指定关键词的图片
        
        Args:
            keyword: 关键词
            
        Returns:
            下载统计信息
        """
        logger.info(f"开始智能下载关键词: {keyword}")
        
        # 使用增强的下载逻辑
        return await self._download_with_enhancement(keyword)
    
    async def download_all_images(self) -> Dict:
        """下载所有关键词的图片
        
        Returns:
            下载统计信息
        """
        logger.info("开始智能下载所有关键词")
        
        # 发现所有关键词
        keywords = self._discover_all_keywords()
        
        total_stats = {
            "keywords": len(keywords),
            "downloaded": 0,
            "failed": 0,
            "enhanced": 0
        }
        
        for keyword in keywords:
            keyword_stats = await self._download_with_enhancement(keyword)
            total_stats["downloaded"] += keyword_stats.get("downloaded", 0)
            total_stats["failed"] += keyword_stats.get("failed", 0)
            total_stats["enhanced"] += keyword_stats.get("enhanced", 0)
        
        return total_stats
    
    async def _download_with_enhancement(self, keyword: str) -> Dict:
        """使用增强逻辑下载图片 - 重构为生产者-消费者架构

        Args:
            keyword: 关键词

        Returns:
            下载统计信息
        """
        logger.info(f"开始智能下载关键词: {keyword} (使用生产者-消费者架构)")

        # 使用新的任务队列管理器
        from .pin_processing_queue import TaskQueueManager

        # 创建队列管理器
        queue_manager = TaskQueueManager(
            max_workers=self.max_concurrent,
            queue_size=200  # 队列大小，避免内存爆炸
        )

        try:
            # 启动生产者-消费者处理
            stats = await queue_manager.start_processing(
                keyword=keyword,
                output_dir=self.output_dir,
                pin_enhancer=self.pin_enhancer,
                header_manager=self.header_manager
            )

            # 转换统计格式以保持兼容性
            download_stats = {
                "keyword": keyword,
                "total_pins": stats.get("total_pins", 0),
                "enhanced_pins": stats.get("enhanced_pins", 0),
                "downloaded": stats.get("downloaded_pins", 0),
                "failed": stats.get("failed_pins", 0),
                "processing_time": stats.get("end_time", 0) - stats.get("start_time", 0) if stats.get("end_time") and stats.get("start_time") else 0
            }

            logger.info(f"生产者-消费者处理完成: {download_stats}")
            return download_stats

        except Exception as e:
            logger.error(f"生产者-消费者处理失败: {e}")
            # 返回错误统计
            return {
                "keyword": keyword,
                "total_pins": 0,
                "enhanced_pins": 0,
                "downloaded": 0,
                "failed": 0,
                "error": str(e)
            }
        finally:
            # 确保资源清理
            await queue_manager.stop_processing()



    async def _download_pin_images(self, pin: Dict, keyword: str, headers: Dict) -> bool:
        """下载单个Pin的图片

        Args:
            pin: Pin数据
            keyword: 关键词
            headers: HTTP请求头

        Returns:
            是否下载成功
        """
        try:
            pin_id = pin.get('id', '')
            if not pin_id:
                logger.debug("Pin缺少ID，跳过下载")
                return False

            # 提取可下载的URL
            image_urls = self._extract_downloadable_urls(pin)
            if not image_urls:
                logger.debug(f"Pin {pin_id} 没有可下载的图片URL")
                return False

            # 创建图片目录
            images_dir = os.path.join(self.output_dir, keyword, "images")
            os.makedirs(images_dir, exist_ok=True)

            # 生成文件路径
            file_extension = "jpg"  # 默认扩展名
            filename = f"{pin_id}.{file_extension}"
            output_path = os.path.join(images_dir, filename)

            # 检查文件是否已存在
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                logger.debug(f"图片已存在，跳过下载: {pin_id}")
                return True

            # 尝试下载第一个可用的URL
            for url in image_urls:
                try:
                    success = await self._download_single_image(url, output_path, headers)
                    if success:
                        logger.debug(f"图片下载成功: {pin_id}")
                        return True
                except Exception as e:
                    logger.debug(f"下载URL失败: {url[:100]}... 错误: {e}")
                    continue

            logger.debug(f"Pin {pin_id} 所有URL都下载失败")
            return False

        except Exception as e:
            logger.error(f"下载Pin图片失败: {pin.get('id', '')}, 错误: {e}")
            return False

    async def _download_single_image(self, url: str, output_path: str, headers: Dict) -> bool:
        """下载单张图片

        Args:
            url: 图片URL
            output_path: 输出路径
            headers: HTTP请求头

        Returns:
            是否下载成功
        """
        try:
            timeout = aiohttp.ClientTimeout(total=30)

            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        # 确保目录存在
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)

                        # 写入文件
                        async with aiofiles.open(output_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                await f.write(chunk)

                        # 验证文件大小
                        if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                            return True
                        else:
                            # 删除无效文件
                            if os.path.exists(output_path):
                                os.remove(output_path)
                            return False
                    else:
                        logger.debug(f"HTTP错误: {response.status} for {url[:100]}...")
                        return False

        except Exception as e:
            logger.debug(f"下载图片异常: {e}")
            return False

    def _extract_downloadable_urls(self, pin: Dict) -> list:
        """提取可下载的图片URL

        Args:
            pin: Pin数据

        Returns:
            可下载的URL列表
        """
        urls = []

        # 检查largest_image_url
        largest_url = pin.get('largest_image_url', '').strip()
        if largest_url and largest_url.startswith('http'):
            urls.append(largest_url)

        # 检查image_urls字典
        image_urls = pin.get('image_urls', {})
        if isinstance(image_urls, str):
            try:
                image_urls = json.loads(image_urls)
            except:
                image_urls = {}

        if isinstance(image_urls, dict):
            for key, url in image_urls.items():
                if url and isinstance(url, str) and url.strip().startswith('http'):
                    if url not in urls:
                        urls.append(url)

        return urls
    
    def _discover_all_keywords(self) -> list:
        """发现所有关键词"""
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
