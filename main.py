#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫主程序

集成异步下载的Pinterest数据采集工具，一站式完成数据采集和图片下载
"""

import argparse
import sys
import asyncio

from loguru import logger
from src.core.pinterest_scraper import PinterestScraper


def create_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="Pinterest智能爬虫 - 集成异步下载的数据采集工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py --query "nature photography" --count 100
  python main.py --url "https://www.pinterest.com/pinterest/" --count 50
  python main.py --query "landscape" --count 2000
  python main.py --query "cats" --count 100 --no-images  # 仅采集数据，不下载图片
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

async def async_main():
    """异步主函数 - 支持资源清理"""
    parser = create_parser()
    args = parser.parse_args()

    # 设置日志
    setup_logger(args.debug)

    scraper = None
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
        pins = await scraper.scrape(
            query=args.query,
            url=args.url,
            count=args.count
        )

        # 输出结果
        if pins:
            print(f"采集完成: {len(pins)} 个Pin -> {args.output}")

            # 如果启用了图片下载，等待所有下载完成
            if not args.no_images:
                logger.info("开始异步下载图片...")

                # 获取下载统计信息
                stats = scraper.get_stats()
                if 'download_stats' in stats:
                    download_stats = stats['download_stats']
                    total_tasks = download_stats.get('total_tasks', 0)

                    if total_tasks > 0:
                        logger.info(f"开始下载 {total_tasks} 张图片...")

                        # 等待所有下载完成
                        try:
                            await scraper.wait_for_downloads_completion()

                            # 获取最终统计
                            final_stats = scraper.get_stats()
                            if 'download_stats' in final_stats:
                                final_download_stats = final_stats['download_stats']
                                completed = final_download_stats.get('completed', 0)
                                failed = final_download_stats.get('failed', 0)
                                logger.info(f"图片下载完成: {completed} 成功, {failed} 失败")
                            else:
                                logger.info("图片下载完成")

                        except KeyboardInterrupt:
                            logger.info("用户中断下载")
                        except Exception as e:
                            logger.error(f"下载过程中出错: {e}")
                    else:
                        logger.info("没有图片需要下载")
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
    finally:
        # 确保资源清理
        if scraper:
            try:
                logger.info("正在清理资源...")
                await scraper.close()
                logger.info("资源清理完成")
            except Exception as e:
                logger.error(f"资源清理时发生错误: {e}")

    return 0


def main():
    """主函数入口 - 运行异步主函数"""
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.warning("用户中断操作")
        return 1


if __name__ == "__main__":
    sys.exit(main())
