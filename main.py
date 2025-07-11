#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫主程序 - 激进重构版

极简的Pinterest数据采集工具，智能自动策略选择
"""

import argparse
import sys

from loguru import logger
from src.core.pinterest_scraper import PinterestScraper


def create_parser() -> argparse.ArgumentParser:
    """创建简化的命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="Pinterest智能爬虫 - 极简数据采集工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py --query "nature photography" --count 100
  python main.py --url "https://www.pinterest.com/pinterest/" --count 50
  python main.py --query "landscape" --count 2000  # 自动智能策略
        """
    )

    # 数据源参数（互斥）
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "-q", "--query",
        help="Pinterest搜索关键词"
    )
    source_group.add_argument(
        "-u", "--url",
        help="Pinterest URL"
    )

    # 核心参数
    parser.add_argument(
        "-c", "--count",
        type=int,
        default=50,
        help="目标采集数量 (默认: 50，自动选择最优策略)"
    )
    parser.add_argument(
        "-o", "--output",
        default="output",
        help="输出目录 (默认: output)"
    )

    # 简化的可选参数
    parser.add_argument(
        "--no-images",
        action="store_true",
        help="仅获取元数据，不下载图片"
    )
    parser.add_argument(
        "--proxy",
        help="代理服务器"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="启用调试模式（显示浏览器）"
    )

    return parser


def setup_logger(debug: bool = False):
    """设置极简的日志配置"""
    logger.remove()
    level = "DEBUG" if debug else "INFO"
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level=level,
        colorize=True
    )

def main():
    """主函数 - 极简重构版"""
    parser = create_parser()
    args = parser.parse_args()

    # 设置日志
    setup_logger(args.debug)

    try:
        logger.info("Pinterest爬虫启动")

        # 创建爬虫实例
        scraper = PinterestScraper(
            output_dir=args.output,
            download_images=not args.no_images,
            proxy=args.proxy,
            debug=args.debug
        )

        # 执行智能采集
        pins = scraper.scrape(
            query=args.query,
            url=args.url,
            count=args.count
        )

        # 输出结果
        if pins:
            print(f"采集完成: {len(pins)} 个Pin -> {args.output}")
        else:
            logger.error("采集失败，未获取到数据")
            return 1

    except KeyboardInterrupt:
        logger.warning("用户中断操作")
        return 1
    except Exception as e:
        logger.error(f"发生错误: {e}")
        if args.debug:
            import traceback
            logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
