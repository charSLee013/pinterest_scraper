"""
下载任务管理器
"""

import asyncio
import os
from typing import List, Dict, Any, Optional
from pathlib import Path
from urllib.parse import urlparse

from loguru import logger
from src.core.database.repository import SQLiteRepository
from .async_downloader import AsyncImageDownloader


class DownloadTaskManager:
    """下载任务管理器，负责协调数据库和异步下载器"""
    
    def __init__(self, max_concurrent: int = 10, auto_start: bool = True, prefer_requests: bool = False, keyword: str = None, output_dir: str = None):
        """初始化任务管理器

        Args:
            max_concurrent: 最大并发下载数
            auto_start: 是否自动启动下载器
            prefer_requests: 是否优先使用requests（性能优化）
            keyword: 搜索关键词，用于关键词特定的数据库路由
            output_dir: 输出目录，用于关键词特定的数据库路由
        """
        self.keyword = keyword
        self.output_dir = output_dir

        # 创建关键词特定的repository
        if keyword and output_dir:
            self.repository = SQLiteRepository(keyword=keyword, output_dir=output_dir)
        else:
            # 向后兼容：使用全局repository
            self.repository = SQLiteRepository()

        # 创建AsyncImageDownloader并传递repository实例
        self.downloader = AsyncImageDownloader(
            max_concurrent=max_concurrent,
            prefer_requests=prefer_requests,
            repository=self.repository
        )
        self.auto_start = auto_start
        self.started = False
    
    async def start(self):
        """启动任务管理器"""
        if self.started:
            return
        
        await self.downloader.start()
        self.started = True
        
        # 处理数据库中的待下载任务
        await self._process_pending_tasks()

        logger.debug("下载任务管理器启动完成")
    
    async def stop(self):
        """停止任务管理器"""
        if not self.started:
            return
        
        await self.downloader.stop()
        self.started = False
    
    async def schedule_pin_downloads(self, pins: List[Dict[str, Any]], output_dir: str) -> int:
        """为Pin列表调度下载任务 - 基于文件系统状态的智能恢复下载

        Args:
            pins: Pin数据列表
            output_dir: 输出目录

        Returns:
            调度的任务数量
        """
        if not self.started and self.auto_start:
            await self.start()

        # 创建图片输出目录
        images_dir = os.path.join(output_dir, "images")
        os.makedirs(images_dir, exist_ok=True)

        scheduled_count = 0
        missing_images = []
        existing_images = []

        logger.info(f"开始分析 {len(pins)} 个Pin的下载状态...")

        for pin in pins:
            pin_id = pin.get('id')
            image_url = pin.get('largest_image_url')

            if not pin_id or not image_url:
                logger.debug(f"跳过Pin {pin_id}: 缺少图片URL")
                continue

            # 转换为原图URL
            original_image_url = self._convert_to_original_url(image_url)
            if original_image_url != image_url:
                logger.debug(f"转换为原图URL: {pin_id}")
                logger.debug(f"  原URL: {image_url}")
                logger.debug(f"  原图URL: {original_image_url}")

            # 生成输出路径
            output_path = self._generate_output_path(pin, output_dir)

            # 检查文件是否已存在且有效
            if self._is_image_downloaded(output_path):
                existing_images.append(pin_id)
                logger.debug(f"图片已存在: {pin_id}")
                continue

            # 图片缺失，需要下载
            missing_images.append(pin_id)

            # 创建或更新下载任务（使用原图URL）
            task_id = self._ensure_download_task(pin_id, original_image_url)

            if task_id is None:
                logger.warning(f"跳过Pin {pin_id}: 无法创建下载任务")
                continue

            # 调度下载
            task_data = {
                'task_id': task_id,
                'pin_id': pin_id,
                'image_url': original_image_url,  # 使用原图URL
                'output_path': output_path
            }

            self.downloader.schedule_download(task_data)
            scheduled_count += 1

        # 详细的状态报告
        logger.debug(f"下载状态分析完成:")
        logger.debug(f"  已存在图片: {len(existing_images)} 个")
        logger.debug(f"  需要下载: {len(missing_images)} 个")
        logger.debug(f"  调度下载任务: {scheduled_count} 个")

        if missing_images:
            logger.debug(f"缺失的图片Pin ID: {missing_images[:10]}{'...' if len(missing_images) > 10 else ''}")

        return scheduled_count

    def _convert_to_original_url(self, image_url: str) -> str:
        """将Pinterest图片URL转换为原图URL，并提供回退选项

        Args:
            image_url: 原始图片URL

        Returns:
            原图URL
        """
        if not image_url or 'i.pinimg.com' not in image_url:
            return image_url

        # 检查是否包含尺寸标识（如 736x, 564x, 1200x 等）
        import re

        # 匹配模式：/数字x/ 或 /数字x数字/
        size_pattern = r'/\d+x\d*/'

        if re.search(size_pattern, image_url):
            # 替换为 /originals/
            original_url = re.sub(size_pattern, '/originals/', image_url)
            logger.debug(f"URL转换: {image_url} -> {original_url}")
            return original_url

        return image_url

    def _get_fallback_urls(self, image_url: str) -> List[str]:
        """获取图片URL的回退选项

        当原图URL访问失败时，提供不同尺寸的回退选项

        Args:
            image_url: 原始图片URL

        Returns:
            URL列表，按优先级排序
        """
        if not image_url or 'i.pinimg.com' not in image_url:
            return [image_url]

        import re

        urls = []

        # 如果是原图URL，添加不同尺寸的回退
        if '/originals/' in image_url:
            # 原图优先
            urls.append(image_url)

            # 添加高质量回退选项
            fallback_sizes = ['1200x', '736x', '564x', '474x']
            for size in fallback_sizes:
                fallback_url = image_url.replace('/originals/', f'/{size}/')
                urls.append(fallback_url)
        else:
            # 如果已经是尺寸URL，先尝试原图
            size_pattern = r'/\d+x\d*/'
            if re.search(size_pattern, image_url):
                original_url = re.sub(size_pattern, '/originals/', image_url)
                urls.append(original_url)

            # 然后是当前URL
            urls.append(image_url)

        # 去重但保持顺序
        seen = set()
        unique_urls = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

        logger.debug(f"生成 {len(unique_urls)} 个回退URL选项")
        return unique_urls

    def _is_image_downloaded(self, file_path: str) -> bool:
        """检查图片是否已下载且有效

        Args:
            file_path: 图片文件路径

        Returns:
            图片是否已存在且有效
        """
        if not os.path.exists(file_path):
            return False

        # 检查文件大小（至少1KB）
        try:
            file_size = os.path.getsize(file_path)
            if file_size < 1024:  # 小于1KB认为是无效文件
                logger.debug(f"文件过小，认为无效: {file_path} ({file_size} bytes)")
                return False
            return True
        except OSError as e:
            logger.debug(f"检查文件时出错: {file_path} - {e}")
            return False

    def _ensure_download_task(self, pin_id: str, image_url: str) -> str:
        """确保下载任务存在，如果不存在则创建

        Args:
            pin_id: Pin ID
            image_url: 图片URL

        Returns:
            任务ID
        """
        # 检查是否已存在任务
        existing_task = self.repository.get_download_task_by_pin_and_url(pin_id, image_url)

        if existing_task:
            # 如果任务已完成但文件不存在，重置任务状态
            if existing_task['status'] == 'completed':
                self.repository.update_download_task_status(existing_task['id'], 'pending')
                logger.debug(f"重置已完成但文件缺失的任务: {pin_id}")
            return existing_task['id']
        else:
            # 创建新任务
            task_id = self.repository.create_download_task(pin_id, image_url)
            if task_id is None:
                logger.error(f"创建下载任务失败: {pin_id}")
                return None
            logger.debug(f"创建新下载任务: {pin_id}")
            return task_id

    async def _process_pending_tasks(self):
        """处理数据库中的待下载任务"""
        # 注意：这个方法现在只在启动时调用一次，避免重复调度
        # 主要的任务调度通过 schedule_pin_downloads 方法进行

        pending_tasks = self.repository.get_pending_download_tasks(limit=1000)

        if not pending_tasks:
            logger.debug("没有待下载的任务")
            return

        logger.debug(f"发现 {len(pending_tasks)} 个待下载任务")

        # 只处理那些没有被其他方式调度的任务
        # 这里可以添加更复杂的逻辑来避免重复调度
        logger.debug("待下载任务将通过 schedule_pin_downloads 方法调度")
    
    def _generate_output_path(self, pin: Dict[str, Any], output_dir: str) -> str:
        """为Pin生成输出路径
        
        Args:
            pin: Pin数据
            output_dir: 输出目录
            
        Returns:
            输出文件路径
        """
        pin_id = pin.get('id', 'unknown')
        image_url = pin.get('largest_image_url', '')
        
        # 从URL获取文件扩展名
        parsed_url = urlparse(image_url)
        path = parsed_url.path
        ext = os.path.splitext(path)[1] if path else '.jpg'
        
        # 确保扩展名有效
        if not ext or ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
            ext = '.jpg'
        
        # 生成文件名
        filename = f"{pin_id}{ext}"
        
        # 生成完整路径
        images_dir = os.path.join(output_dir, 'images')
        return os.path.join(images_dir, filename)
    
    def _generate_output_path_from_task(self, task: Dict[str, Any]) -> str:
        """从下载任务生成输出路径
        
        Args:
            task: 下载任务数据
            
        Returns:
            输出文件路径
        """
        pin_id = task.get('pin_id', 'unknown')
        image_url = task.get('image_url', '')
        
        # 从URL获取文件扩展名
        parsed_url = urlparse(image_url)
        path = parsed_url.path
        ext = os.path.splitext(path)[1] if path else '.jpg'
        
        # 确保扩展名有效
        if not ext or ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
            ext = '.jpg'
        
        # 生成文件名
        filename = f"{pin_id}{ext}"
        
        # 使用默认的images目录
        # 注意：这里简化处理，实际应该从会话信息中获取正确的输出目录
        return os.path.join('output', 'images', filename)
    
    def get_download_stats(self) -> Dict[str, Any]:
        """获取下载统计信息"""
        return self.downloader.get_stats()
    
    async def wait_for_completion(self, timeout: Optional[float] = None):
        """等待所有下载任务完成"""
        if self.started:
            await self.downloader.wait_for_completion(timeout)
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.stop()
