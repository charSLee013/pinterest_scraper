#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫主程序
支持以下功能：
1. 从URL列表爬取图片
2. 搜索单个关键词
3. 从文件读取多个关键词并发搜索
4. 从目录读取多个关键词文件并发搜索
"""

import argparse
import json
import os
import sys
import time
import traceback
from typing import List

from loguru import logger

import config
from concurrent_search import concurrent_search
from pinterest import PinterestScraper


def setup_logger(log_level: str = "INFO", log_file: bool = True):
    """设置日志配置

    Args:
        log_level: 日志级别
        log_file: 是否输出到文件
    """
    # 移除默认处理器
    logger.remove()

    # 添加控制台处理器
    logger.add(sys.stderr, format=config.LOG_FORMAT, level=log_level, colorize=True)

    # 添加文件处理器
    if log_file:
        os.makedirs("logs", exist_ok=True)
        log_filename = f"logs/pinterest_scraper_{time.strftime('%Y%m%d_%H%M%S')}.log"
        logger.add(
            log_filename,
            format=config.LOG_FORMAT,
            level=log_level,
            rotation="10 MB",
            compression="zip",
            retention="1 week",
        )


def read_terms_from_file(filepath: str) -> List[str]:
    """从文件读取搜索关键词，每行一个"""
    terms = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                term = line.strip()
                if term and not term.startswith("#"):
                    terms.append(term)
        return terms
    except Exception as e:
        logger.error(f"读取关键词文件失败: {e}")
        return []


def read_terms_from_directory(directory_path: str) -> List[str]:
    """从目录中的所有文件读取搜索关键词"""
    all_terms = []
    try:
        if not os.path.isdir(directory_path):
            logger.error(f"指定的路径不是一个目录: {directory_path}")
            return []

        file_count = 0
        for filename in os.listdir(directory_path):
            filepath = os.path.join(directory_path, filename)
            if os.path.isfile(filepath):
                file_terms = read_terms_from_file(filepath)
                all_terms.extend(file_terms)
                file_count += 1
                logger.debug(f"从文件 {filename} 读取了 {len(file_terms)} 个关键词")

        logger.info(f"从 {file_count} 个文件中读取了关键词")
        return all_terms
    except Exception as e:
        logger.error(f"读取目录中的关键词文件失败: {e}")
        return []


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="Pinterest图片爬虫 - 高效地爬取Pinterest图片和元数据"
    )

    # 输入源参数组(互斥)
    input_group = parser.add_mutually_exclusive_group(required=False)
    input_group.add_argument(
        "-u",
        "--urls",
        nargs="+",
        help="要爬取的Pinterest URL列表",
    )
    input_group.add_argument(
        "-s",
        "--search",
        type=str,
        help="Pinterest搜索关键词",
    )
    input_group.add_argument(
        "-f",
        "--file",
        type=str,
        default="inputs/input_topics.txt",
        help="包含URL列表或关键词的文件路径，每行一个 (默认: inputs/test_urls.txt)",
    )
    input_group.add_argument(
        "-d",
        "--directory",
        type=str,
        help="包含关键词文件的目录路径，每个文件的每行都是一个关键词",
    )
    input_group.add_argument(
        "-m",
        "--multi-search",
        nargs="+",
        help="多个Pinterest搜索关键词，并发执行",
    )

    # 常规参数
    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=50,
        help="每个URL/关键词要下载的图片数量 (默认: 50)",
    )
    parser.add_argument(
        "-o", "--output", type=str, default="output", help='输出目录 (默认: "output")'
    )
    parser.add_argument(
        "-p", "--proxy", type=str, help="代理服务器 (格式: http://user:pass@host:port)"
    )

    # 高级选项
    advanced_group = parser.add_argument_group("高级选项")
    advanced_group.add_argument(
        "-l",
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="日志级别 (默认: INFO)",
    )
    advanced_group.add_argument(
        "--no-images", action="store_true", help="仅获取元数据，不下载图片"
    )
    advanced_group.add_argument(
        "--debug",
        action="store_true",
        help="启用调试模式，保存页面快照和网络请求",
        default=True,
    )
    advanced_group.add_argument(
        "--max-workers",
        type=int,
        default=0,
        help="设置并发下载的最大线程数 (默认: 自动)",
    )
    advanced_group.add_argument(
        "--timeout", type=int, default=30, help="设置请求超时时间 (默认: 30秒)"
    )
    advanced_group.add_argument(
        "--viewport-width",
        type=int,
        default=1920,
        help="设置浏览器视口宽度 (默认: 1920)",
    )
    advanced_group.add_argument(
        "--viewport-height",
        type=int,
        default=1080,
        help="设置浏览器视口高度 (默认: 1080)",
    )
    advanced_group.add_argument(
        "--max-concurrent",
        type=int,
        default=3,
        help="多关键词搜索时的最大并发数 (默认: 3)",
    )

    args = parser.parse_args()

    # 设置日志
    setup_logger(args.log_level)

    try:
        # 获取URL列表或关键词列表
        urls = []
        search_terms = []

        if args.urls:
            urls = args.urls
            logger.info(f"使用命令行提供的 {len(urls)} 个URL")
        elif args.search:
            search_terms = [args.search]
            logger.info(f"使用单个搜索关键词: '{args.search}'")
        elif args.multi_search:
            search_terms = args.multi_search
            logger.info(f"使用命令行提供的 {len(search_terms)} 个搜索关键词")
        elif args.directory:
            search_terms = read_terms_from_directory(args.directory)
            logger.info(
                f"从目录 {args.directory} 读取了 {len(search_terms)} 个搜索关键词"
            )
        else:
            # 默认使用文件输入
            if args.file.endswith(".txt"):
                # 尝试读取文件内容，判断是URL列表还是关键词列表
                content = read_terms_from_file(args.file)
                if content and any(url.startswith("http") for url in content):
                    urls = content
                    logger.info(f"从文件 {args.file} 读取了 {len(urls)} 个URL")
                else:
                    search_terms = content
                    logger.info(
                        f"从文件 {args.file} 读取了 {len(search_terms)} 个搜索关键词"
                    )
            else:
                logger.error("输入文件必须是.txt格式")
                return 1

        if not urls and not search_terms:
            logger.error("没有找到有效的URL或搜索关键词")
            return 1

        # 打印配置信息
        config_info = {
            "output_dir": args.output,
            "debug": args.debug,
            "timeout": args.timeout,
            "max_workers": args.max_workers,
            "download_images": not args.no_images,
            "viewport": f"{args.viewport_width}x{args.viewport_height}",
        }
        logger.info(f"爬虫配置: {json.dumps(config_info)}")

        # 执行爬取
        if search_terms:
            logger.info(
                f"开始并发搜索 {len(search_terms)} 个关键词，每个关键词爬取 {args.count} 个pins"
            )

            results = concurrent_search(
                search_terms=search_terms,
                count_per_term=args.count,
                output_dir=args.output,
                max_concurrent=args.max_concurrent,
                proxy=args.proxy,
                debug=args.debug,
                timeout=args.timeout,
                max_workers=args.max_workers,
                download_images=not args.no_images,
            )

            total_pins = sum(len(pins) for pins in results.values())
            success_terms = sum(1 for pins in results.values() if pins)
            logger.success(
                f"并发搜索完成! 处理了 {len(search_terms)} 个关键词，成功: {success_terms}"
            )
            logger.success(f"总共获取了 {total_pins} 个pins")

        else:
            # 初始化爬虫
            scraper = PinterestScraper(
                output_dir=args.output,
                proxy=args.proxy,
                debug=args.debug,
                timeout=args.timeout,
                max_workers=args.max_workers,
                download_images=not args.no_images,
            )

            # 开始爬取URL
            logger.info(f"开始爬取 {len(urls)} 个URL，每个URL爬取 {args.count} 个pins")
            result = scraper.scrape_urls(urls, args.count)
            total_pins = sum(len(pins) for pins in result.values())
            logger.success(f"成功爬取了 {total_pins} 个pins，来自 {len(urls)} 个URL")

        logger.success(f"爬取完成! 结果保存到 {args.output}")
        print(f"爬取完成! 结果保存到 {args.output}")

    except KeyboardInterrupt:
        logger.warning("用户中断爬取")
        print("\n用户中断爬取")
        return 1
    except Exception as e:
        logger.error(f"发生错误: {e}")
        logger.error(traceback.format_exc())
        print(f"发生错误: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
