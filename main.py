#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫主程序

集成异步下载的Pinterest数据采集工具，一站式完成数据采集和图片下载
"""

import argparse
import sys
import asyncio
import signal

from loguru import logger
from src.core.pinterest_scraper import PinterestScraper


def setup_signal_handlers():
    """设置信号处理器以优雅处理中断"""
    def signal_handler(signum, frame):
        logger.info(f"接收到信号 {signum}，正在优雅退出...")
        # 不直接退出，让KeyboardInterrupt异常处理
        raise KeyboardInterrupt()

    # 在Windows和Unix系统上设置信号处理
    if sys.platform != 'win32':
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    else:
        # Windows只支持SIGINT
        signal.signal(signal.SIGINT, signal_handler)


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
  python main.py --only-images --query "cats"  # 仅下载cats关键词的缺失图片
  python main.py --only-images  # 下载所有关键词的缺失图片
        """
    )

    # 数据源参数（互斥，但--only-images模式下可选）
    source_group = parser.add_mutually_exclusive_group(required=False)
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
        "--only-images",
        action="store_true",
        help="仅下载图片模式：从现有数据库中下载缺失的图片"
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
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="启用详细输出（开发模式）"
    )

    return parser


def setup_logger(debug: bool = False, verbose: bool = False):
    """设置三层日志配置

    Args:
        debug: 启用DEBUG级别（调试层）
        verbose: 启用INFO级别（开发层）
        默认: WARNING级别（用户层）
    """
    logger.remove()
    if debug:
        level = "DEBUG"
    elif verbose:
        level = "INFO"
    else:
        level = "WARNING"

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

    # 参数验证
    if args.only_images:
        # --only-images模式下，不需要--query或--url
        if args.no_images:
            logger.error("--only-images 和 --no-images 不能同时使用")
            return 1
        if args.url:
            logger.error("--only-images 模式下不支持 --url 参数")
            return 1
    else:
        # 普通模式下，必须提供--query或--url
        if not args.query and not args.url:
            logger.error("必须提供 --query 或 --url 参数（除非使用 --only-images 模式）")
            return 1

    # 设置日志
    setup_logger(args.debug, args.verbose)

    # --only-images 模式：仅下载图片
    if args.only_images:
        from src.tools.image_downloader import ImageDownloader

        downloader = ImageDownloader(
            output_dir=args.output,
            max_concurrent=15,
            proxy=args.proxy,
            prefer_requests=True
        )

        try:
            if args.query:
                # 下载指定关键词的图片
                logger.warning(f"开始下载关键词图片: {args.query}")
                stats = await downloader.download_missing_images_for_keyword(args.query)
                logger.warning(f"下载完成: {stats['downloaded']} 成功, {stats['failed']} 失败")
            else:
                # 下载所有关键词的图片
                logger.warning("开始下载所有关键词的缺失图片")
                stats = await downloader.download_all_missing_images()
                logger.warning(f"下载完成: {stats['keywords']} 个关键词, {stats['downloaded']} 成功, {stats['failed']} 失败")

            return 0

        except Exception as e:
            logger.error(f"图片下载失败: {e}")
            if args.debug:
                import traceback
                logger.error(traceback.format_exc())
            return 1

    # 普通模式：数据采集
    scraper = None
    try:
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
            logger.warning(f"采集完成: {len(pins)} 个Pin -> {args.output}")

            # 如果启用了图片下载，等待所有下载完成
            if not args.no_images:
                logger.debug("开始异步下载图片...")

                # 获取下载统计信息
                stats = scraper.get_stats()
                if 'download_stats' in stats:
                    download_stats = stats['download_stats']
                    total_tasks = download_stats.get('total_tasks', 0)

                    if total_tasks > 0:
                        logger.debug(f"开始下载 {total_tasks} 张图片...")

                        # 等待所有下载完成
                        try:
                            await scraper.wait_for_downloads_completion()

                            # 获取最终统计
                            final_stats = scraper.get_stats()
                            if 'download_stats' in final_stats:
                                final_download_stats = final_stats['download_stats']
                                completed = final_download_stats.get('completed', 0)
                                failed = final_download_stats.get('failed', 0)
                                logger.warning(f"图片下载完成: {completed} 成功, {failed} 失败")
                            else:
                                logger.warning("图片下载完成")

                        except KeyboardInterrupt:
                            logger.info("用户中断下载")
                        except Exception as e:
                            logger.error(f"下载过程中出错: {e}")
                    else:
                        logger.debug("没有图片需要下载")
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
                logger.debug("正在清理资源...")
                await scraper.close()
                logger.debug("资源清理完成")
            except Exception as e:
                logger.error(f"资源清理时发生错误: {e}")

    return 0


def main():
    """主函数入口 - 运行异步主函数"""
    try:
        # 设置信号处理器
        setup_signal_handlers()

        # 在Windows上使用更稳定的事件循环策略
        if sys.platform == 'win32':
            # 设置事件循环策略以减少清理警告
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        return asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.warning("用户中断操作")
        return 1
    finally:
        # 在Windows上显式清理事件循环以减少警告
        if sys.platform == 'win32':
            try:
                # 等待一小段时间让异步任务完成清理
                import time
                time.sleep(0.2)

                # 强制垃圾回收，清理未关闭的资源
                import gc
                gc.collect()

                # 等待更长时间让子进程完全退出
                time.sleep(0.3)

            except Exception:
                # 忽略清理时的异常，这些通常是无害的
                pass


if __name__ == "__main__":
    sys.exit(main())
