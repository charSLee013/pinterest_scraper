#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫主程序
"""

import argparse
import json
import os
import sys
import traceback

from loguru import logger

import config
import utils
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
        logger.add(
            "logs/pinterest_scraper_{time}.log",
            format=config.LOG_FORMAT,
            level=log_level,
            rotation="10 MB",
            compression="zip",
            retention="1 week",
        )


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="Pinterest图片爬虫 - 高效地爬取Pinterest图片和元数据"
    )

    # 输入源参数组(互斥)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "-u", "--urls", nargs="+", help="要爬取的Pinterest URL列表"
    )
    input_group.add_argument("-s", "--search", type=str, help="Pinterest搜索关键词")
    input_group.add_argument(
        "-f", "--file", type=str, help="包含URL列表的文件路径，每行一个URL"
    )
    input_group.add_argument(
        "-m", "--multi-search", nargs="+", help="多个Pinterest搜索关键词，并发执行"
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
        "--debug", action="store_true", help="启用调试模式，保存页面快照和网络请求"
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
        "--zoom-level",
        type=int,
        default=30,
        help="设置浏览器页面缩放级别，百分比 (默认: 30%)",
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
        # 获取URL列表
        urls = []
        if args.file:
            try:
                urls = utils.load_url_list(args.file)
                logger.info(f"从文件 {args.file} 加载了 {len(urls)} 个URL")
            except Exception as e:
                logger.error(f"读取URL文件失败: {e}")
                return 1
        elif args.urls:
            urls = args.urls

        # 打印配置信息
        config_info = {
            "output_dir": args.output,
            "debug": args.debug,
            "timeout": args.timeout,
            "max_workers": args.max_workers,
            "download_images": not args.no_images,
            "viewport": f"{args.viewport_width}x{args.viewport_height}",
            "zoom_level": f"{args.zoom_level}%",
        }
        logger.info(f"爬虫配置: {json.dumps(config_info)}")

        # 多关键词并发搜索
        if args.multi_search:
            logger.info(
                f"开始并发搜索 {len(args.multi_search)} 个关键词，每个关键词爬取 {args.count} 个pins"
            )

            results = concurrent_search(
                search_terms=args.multi_search,
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
            logger.info(f"并发搜索完成，共获取了 {total_pins} 个pins")

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

            # 开始爬取
            if args.search:
                logger.info(f"开始搜索: '{args.search}'，爬取 {args.count} 个pins")
                pins = scraper.search(args.search, args.count)
                logger.info(f"成功爬取了 {len(pins)} 个pins，关键词: '{args.search}'")
            else:
                logger.info(
                    f"开始爬取 {len(urls)} 个URL，每个URL爬取 {args.count} 个pins"
                )
                result = scraper.scrape_urls(urls, args.count)
                total_pins = sum(len(pins) for pins in result.values())
                logger.info(f"成功爬取了 {total_pins} 个pins，来自 {len(urls)} 个URL")

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
