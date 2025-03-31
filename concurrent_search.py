#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest并发搜索模块
"""

import concurrent.futures
import json
import os
import time
from typing import Dict, List, Optional

from loguru import logger

from pinterest import PinterestScraper


def search_single_term(
    term: str,
    count: int,
    output_dir: str = "output",
    proxy: Optional[str] = None,
    debug: bool = False,
    timeout: int = 30,
    max_workers: int = 0,
    download_images: bool = True,
) -> List[Dict]:
    """处理单个搜索词的函数，用于并发执行

    Args:
        term: 搜索关键词
        count: 需要获取的pin数量
        output_dir: 主输出目录
        proxy: 代理服务器地址
        debug: 是否启用调试模式
        timeout: 请求超时时间(秒)
        max_workers: 并发下载的最大线程数
        download_images: 是否下载图片

    Returns:
        搜索结果列表
    """
    try:
        # 为当前搜索词创建带有时间戳的日志
        log_suffix = time.strftime("%Y%m%d_%H%M%S")
        logger.info(f"开始处理搜索词: '{term}', 目标数量: {count}")

        # 创建爬虫实例
        scraper = PinterestScraper(
            output_dir=output_dir,
            proxy=proxy,
            debug=debug,
            timeout=timeout,
            max_workers=max_workers,
            download_images=download_images,
        )

        # 执行搜索
        start_time = time.time()
        pins = scraper.search(term, count)
        end_time = time.time()

        # 记录结果
        logger.info(
            f"搜索词 '{term}' 处理完成，获取了 {len(pins)} 个pins，耗时 {end_time - start_time:.2f} 秒"
        )
        return pins

    except Exception as e:
        logger.error(f"处理搜索词 '{term}' 时出错: {e}")
        return []


def concurrent_search(
    search_terms: List[str],
    count_per_term: int = 50,
    output_dir: str = "output",
    max_concurrent: int = 3,
    proxy: Optional[str] = None,
    debug: bool = False,
    timeout: int = 30,
    max_workers: int = 0,
    download_images: bool = True,
) -> Dict[str, List[Dict]]:
    """并发搜索多个关键词

    Args:
        search_terms: 搜索关键词列表
        count_per_term: 每个关键词爬取的数量
        output_dir: 输出目录
        max_concurrent: 最大并发搜索数
        proxy: 代理服务器地址
        debug: 是否启用调试模式
        timeout: 请求超时时间(秒)
        max_workers: 并发下载的最大线程数
        download_images: 是否下载图片

    Returns:
        关键词到搜索结果的映射字典
    """
    if not search_terms:
        logger.warning("没有提供搜索关键词")
        return {}

    logger.info(
        f"开始并发搜索 {len(search_terms)} 个关键词，每个关键词爬取 {count_per_term} 个pins"
    )
    logger.info(f"最大并发数: {max_concurrent}")

    # 创建主输出目录
    os.makedirs(output_dir, exist_ok=True)

    results = {}
    start_time = time.time()

    # 使用进程池执行并发搜索
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_concurrent) as executor:
        # 准备任务参数
        future_to_term = {}

        for term in search_terms:
            # 提交搜索任务
            future = executor.submit(
                search_single_term,
                term,
                count_per_term,
                output_dir,
                proxy,
                debug,
                timeout,
                max_workers,
                download_images,
            )
            future_to_term[future] = term

        # 处理完成的任务
        total_pins = 0
        success_count = 0

        for future in concurrent.futures.as_completed(future_to_term):
            term = future_to_term[future]
            try:
                pins = future.result()
                results[term] = pins
                total_pins += len(pins)
                success_count += 1
                logger.info(f"搜索词 '{term}' 已完成，获取了 {len(pins)} 个pins")
            except Exception as e:
                logger.error(f"获取搜索词 '{term}' 的结果时出错: {e}")
                results[term] = []

    # 记录总体结果
    end_time = time.time()
    logger.info(
        f"并发搜索完成，处理了 {len(search_terms)} 个关键词，成功 {success_count} 个"
    )
    logger.info(
        f"总共获取了 {total_pins} 个pins，总耗时 {end_time - start_time:.2f} 秒"
    )

    # 保存汇总结果
    summary = {
        "total_terms": len(search_terms),
        "successful_terms": success_count,
        "total_pins": total_pins,
        "time_taken": end_time - start_time,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "term_stats": {term: len(pins) for term, pins in results.items()},
    }

    summary_path = os.path.join(
        output_dir, f"search_summary_{time.strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    return results


if __name__ == "__main__":
    # 示例用法
    import sys

    from loguru import logger

    # 配置日志
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    # 示例搜索词
    terms = ["cat", "dog", "bird", "flower", "mountain"]

    # 执行并发搜索
    results = concurrent_search(
        search_terms=terms,
        count_per_term=20,
        output_dir="output",
        max_concurrent=2,  # 最多同时运行2个进程
        download_images=True,
    )

    # 输出结果数量
    for term, pins in results.items():
        print(f"搜索词 '{term}': {len(pins)} 个pins")
