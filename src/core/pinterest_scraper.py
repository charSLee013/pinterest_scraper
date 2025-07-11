#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫核心模块 - 激进重构版

极简的Pinterest数据采集工具，智能选择最优策略
"""

import os
import json
from typing import Dict, List, Optional

from loguru import logger

from .smart_scraper import SmartScraper
from ..utils import downloader, utils


class PinterestScraper:
    """Pinterest爬虫主类 - 激进重构版

    极简的Pinterest数据采集接口，自动智能策略选择
    """

    def __init__(
        self,
        output_dir: str = "output",
        download_images: bool = True,
        proxy: Optional[str] = None,
        debug: bool = False
    ):
        """初始化Pinterest爬虫

        Args:
            output_dir: 输出目录
            download_images: 是否下载图片
            proxy: 代理服务器
            debug: 调试模式
        """
        self.output_dir = output_dir
        self.download_images = download_images
        self.proxy = proxy
        self.debug = debug

        # 智能采集引擎
        self.scraper = SmartScraper(
            proxy=proxy,
            debug=debug
        )

        logger.info("Pinterest爬虫初始化完成")

    def scrape(
        self,
        query: Optional[str] = None,
        url: Optional[str] = None,
        count: int = 50
    ) -> List[Dict]:
        """统一的Pinterest数据采集接口

        Args:
            query: 搜索关键词
            url: Pinterest URL
            count: 目标数量

        Returns:
            采集到的Pin数据列表
        """
        if not query and not url:
            logger.error("必须提供query或url参数")
            return []

        logger.info(f"开始Pinterest数据采集")
        logger.info(f"参数: query={query}, url={url}, count={count}")

        # 设置工作目录
        work_name = utils.sanitize_filename(query or url.split('/')[-1] or 'scrape')
        work_dirs = utils.setup_directories(self.output_dir, work_name, self.debug)
        work_dir = work_dirs.get('term_root', work_dirs['root'])

        # 检查缓存
        cached_pins = self._load_cache(work_name)
        if len(cached_pins) >= count:
            logger.info(f"缓存中已有 {len(cached_pins)} 个pins，直接使用")
            return self._finalize_results(cached_pins[:count], work_dir)

        # 计算实际需要采集的数量（增量采集）
        cached_count = len(cached_pins)
        remaining_count = count - cached_count

        if cached_count > 0:
            logger.info(f"缓存中已有 {cached_count} 个pins，还需要采集 {remaining_count} 个")

        # 执行智能采集（只采集剩余数量）
        new_pins = self.scraper.scrape(query=query, url=url, target_count=remaining_count)

        # 合并缓存数据和新采集数据
        all_pins = self._merge_pins(cached_pins, new_pins)

        # 确保不超过目标数量
        final_pins = all_pins[:count]

        logger.info(f"数据合并: {cached_count} + {len(new_pins)} = {len(final_pins)} 个pins")

        return self._finalize_results(final_pins, work_dir)

    def _load_cache(self, work_name: str) -> List[Dict]:
        """加载缓存的Pin数据"""
        cache_file = os.path.join(self.output_dir, work_name, "pins.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                    if isinstance(cached_data, list):
                        logger.info(f"从缓存加载了 {len(cached_data)} 个pins")
                        return cached_data
            except Exception as e:
                logger.warning(f"加载缓存失败: {e}")
        return []

    def _merge_pins(self, cached_pins: List[Dict], new_pins: List[Dict]) -> List[Dict]:
        """合并缓存数据和新采集数据，去重处理

        Args:
            cached_pins: 缓存的Pin数据
            new_pins: 新采集的Pin数据

        Returns:
            合并后的Pin数据列表
        """
        # 使用Pin ID进行去重
        seen_ids = set()
        merged_pins = []

        # 首先添加缓存数据
        for pin in cached_pins:
            pin_id = pin.get('id')
            if pin_id and pin_id not in seen_ids:
                seen_ids.add(pin_id)
                merged_pins.append(pin)

        # 然后添加新数据，跳过重复的
        duplicates_count = 0
        for pin in new_pins:
            pin_id = pin.get('id')
            if pin_id and pin_id not in seen_ids:
                seen_ids.add(pin_id)
                merged_pins.append(pin)
            elif pin_id:
                duplicates_count += 1

        if duplicates_count > 0:
            logger.debug(f"去重: 跳过 {duplicates_count} 个重复Pin")

        return merged_pins

    def _finalize_results(self, pins: List[Dict], work_dir: str) -> List[Dict]:
        """完成结果处理：保存数据和下载图片"""
        if not pins:
            logger.warning("没有获取到任何Pin数据")
            return []

        # 保存JSON数据
        self._save_pins_json(pins, work_dir)

        # 下载图片（如果启用）
        if self.download_images:
            self._download_images(pins, work_dir)

        # 生成统计信息
        self._generate_stats(pins, work_dir)

        logger.info(f"采集完成: {len(pins)} 个Pin")
        return pins

    def _save_pins_json(self, pins: List[Dict], work_dir: str):
        """保存Pin数据为JSON文件"""
        json_file = os.path.join(work_dir, "pins.json")
        try:
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(pins, f, ensure_ascii=False, indent=2)
            logger.info(f"Pin数据已保存到: {json_file}")
        except Exception as e:
            logger.error(f"保存JSON文件失败: {e}")

    def _download_images(self, pins: List[Dict], work_dir: str):
        """下载Pin图片"""
        if not pins:
            return

        # 创建图片目录
        images_dir = os.path.join(work_dir, "images")
        os.makedirs(images_dir, exist_ok=True)

        # 准备下载任务
        download_tasks = []
        for pin in pins:
            image_url = pin.get('largest_image_url') or pin.get('image_urls', {}).get('original')
            if image_url:
                pin_id = pin.get('id', 'unknown')
                filename = f"{pin_id}.jpg"
                download_tasks.append({
                    'url': image_url,
                    'path': os.path.join(images_dir, filename),
                    'pin_id': pin_id
                })

        if download_tasks:
            # 使用下载器批量下载
            success_count = downloader.download_images_batch(download_tasks)
            logger.info(f"图片下载: {success_count}/{len(download_tasks)}")

            # 更新Pin数据中的下载状态
            for pin in pins:
                pin_id = pin.get('id')
                if pin_id:
                    image_path = os.path.join(images_dir, f"{pin_id}.jpg")
                    pin['downloaded'] = os.path.exists(image_path)
                    if pin['downloaded']:
                        pin['download_path'] = image_path

    def _generate_stats(self, pins: List[Dict], work_dir: str):
        """生成采集统计信息"""
        stats = {
            "total_pins": len(pins),
            "downloaded_images": sum(1 for pin in pins if pin.get('downloaded', False)),
            "unique_creators": len(set(pin.get('creator', {}).get('name', 'Unknown') for pin in pins)),
            "avg_saves": sum(pin.get('stats', {}).get('saves', 0) for pin in pins) / len(pins) if pins else 0,
            "scraper_stats": self.scraper.get_stats()
        }

        stats_file = os.path.join(work_dir, "stats.json")
        try:
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            logger.info(f"统计信息已保存到: {stats_file}")
        except Exception as e:
            logger.error(f"保存统计信息失败: {e}")

        # 输出关键统计
        logger.info(f"统计: {stats['total_pins']} pins, {stats['downloaded_images']} 图片, {stats['unique_creators']} 创作者")

    def get_stats(self) -> Dict:
        """获取采集器统计信息"""
        return self.scraper.get_stats()