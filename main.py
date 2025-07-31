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
import warnings

from loguru import logger
from src.core.pinterest_scraper import PinterestScraper

# 屏蔽asyncio的Task警告
warnings.filterwarnings("ignore", message=".*Task was destroyed but it is pending.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, module="asyncio")


def setup_signal_handlers():
    """设置信号处理器以优雅处理中断"""
    def signal_handler(signum, frame):
        logger.info(f"接收到信号 {signum}，正在优雅退出...")

        # 设置全局中断状态
        from src.tools.stage_manager import _global_interrupt_manager
        _global_interrupt_manager.set_interrupted()

        # 抛出KeyboardInterrupt以确保异常传播
        raise KeyboardInterrupt()

    def force_exit_handler(signum, frame):
        """强制退出处理器 - 第二次Ctrl+C直接退出"""
        logger.warning("检测到强制退出信号，立即终止程序")
        import os
        os._exit(1)

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
  # 关键词搜索模式（阶段1+阶段2混合策略）
  python main.py --query "nature photography" --count 100
  python main.py -q "landscape" --count 2000 --max-concurrent 30
  python main.py -q "cats" --count 100 --no-images  # 仅采集数据，不下载图片

  # Pin扩展模式（纯阶段2深度扩展）
  python main.py --pin-id "123456789" --count 1000
  python main.py -p "123456789" --count 500 --max-concurrent 20

  # 优先级：-q > -p（同时提供时以关键词为准）
  python main.py -q "cats" -p "123456789" --count 100  # 使用关键词搜索

  # 仅下载图片模式
  python main.py --only-images --query "cats"  # 仅下载cats关键词的缺失图片
  python main.py --only-images --max-concurrent 100  # 下载所有关键词，高并发
  python main.py --only-images -j 5  # 下载所有关键词，低并发（网络慢时）
        """
    )

    # 数据源参数（非互斥，通过优先级处理）
    parser.add_argument(
        "-q", "--query",
        help="Pinterest搜索关键词（优先级最高）"
    )
    parser.add_argument(
        "-p", "--pin-id",
        help="Pinterest Pin ID起始点（直接扩展模式）"
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
    parser.add_argument(
        "--max-concurrent", "--max-workers", "-j",
        type=int,
        default=15,
        help="最大并发下载数 (默认: 15，范围: 1-65536)，批次大小自动等于并发数"
    )

    return parser


def validate_concurrent_value(value: int) -> int:
    """验证并发数值的有效性"""
    if value < 1:
        logger.warning(f"并发数过小 ({value})，设置为1")
        return 1
    elif value > 65536:
        logger.warning(f"并发数过大 ({value})，设置为65536")
        return 65536
    return value


def validate_batch_size(value: int) -> int:
    """验证批次大小的有效性"""
    if value < 10:
        logger.warning(f"批次大小过小 ({value})，设置为10")
        return 10
    elif value > 500:
        logger.warning(f"批次大小过大 ({value})，设置为500")
        return 500
    return value


def validate_pin_id(pin_id: str) -> bool:
    """验证Pin ID的有效性"""
    if not pin_id:
        return False
    if not pin_id.isdigit():
        return False
    if len(pin_id) < 10 or len(pin_id) > 20:  # Pinterest Pin ID长度范围
        return False
    return True


def validate_and_prioritize_args(args):
    """参数验证和优先级处理"""

    # --only-images模式的特殊处理
    if args.only_images:
        if args.pin_id:
            logger.error("--only-images 模式下不支持 --pin-id 参数")
            return None
        # --only-images模式下，--query是可选的
        return args

    # 普通模式：优先级处理
    if args.query:
        # 有关键词：使用关键词搜索模式
        if args.pin_id:
            logger.warning("同时提供了-q和-p参数，以-q关键词搜索为准，忽略Pin ID")
        args.mode = "keyword_search"
        args.effective_source = args.query

    elif args.pin_id:
        # 只有Pin ID：使用Pin扩展模式
        if not validate_pin_id(args.pin_id):
            logger.error(f"无效的Pin ID: {args.pin_id}（必须是10-20位数字）")
            return None
        args.mode = "pin_expansion"
        args.effective_source = args.pin_id

    else:
        # 都没有：错误
        logger.error("必须提供 -q/--query 或 -p/--pin-id 参数")
        return None

    return args


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

    # 参数验证和优先级处理
    args = validate_and_prioritize_args(args)
    if args is None:
        return 1

    # --only-images模式的额外验证
    if args.only_images:
        if args.no_images:
            logger.error("--only-images 和 --no-images 不能同时使用")
            return 1

    # 设置日志
    setup_logger(args.debug, args.verbose)

    # 验证并发参数
    max_concurrent = validate_concurrent_value(args.max_concurrent)

    # --only-images 模式：四阶段重构处理（数据库修复 + Base64转换 + Pin增强 + 图片下载）
    if args.only_images:
        from src.tools.refactored_workflow import RefactoredOnlyImagesWorkflow

        logger.info("🚀 开始四阶段重构--only-images处理流程")
        logger.info("阶段1: 数据库修复与检测")
        logger.info("阶段2: Base64编码Pin转换")
        logger.info("阶段3: Pin详情数据补全")
        logger.info("阶段4: 图片文件下载")

        # 创建重构后的工作流程
        workflow = RefactoredOnlyImagesWorkflow(
            output_dir=args.output,
            max_concurrent=max_concurrent,
            proxy=args.proxy
        )

        try:
            # 重置全局中断状态
            from src.tools.stage_manager import _global_interrupt_manager
            _global_interrupt_manager.reset()

            # 执行优化后的四阶段工作流程
            if args.query:
                logger.info(f"🎯 目标关键词: {args.query}")
            else:
                logger.info("🎯 处理所有关键词")

            # 执行工作流程
            result = await workflow.execute(target_keyword=args.query)

            if result["status"] == "success":
                logger.info("🎉 四阶段工作流程执行成功")

                # 显示详细统计
                stats = result.get("stats", {})

                # 阶段统计
                stage1_stats = stats.get("stage1_database_repair", {})
                stage2_stats = stats.get("stage2_base64_conversion", {})
                stage3_stats = stats.get("stage3_pin_enhancement", {})
                stage4_stats = stats.get("stage4_image_download", {})

                if stage2_stats.get("total_converted", 0) > 0:
                    logger.info(f"📊 阶段2: 转换了 {stage2_stats['total_converted']} 个base64编码Pin")

                if stage4_stats:
                    logger.info(f"📊 阶段4: 下载统计 {stage4_stats}")

                # 总执行时间
                total_time = stats.get("total_execution_time", 0)
                logger.info(f"⏱️ 总执行时间: {total_time:.2f} 秒")

                return 0
            elif result["status"] == "interrupted":
                logger.warning(f"🛑 工作流程被用户中断: {result.get('message', '用户中断')}")
                return 130  # 标准的中断退出码
            else:
                logger.error(f"❌ 工作流程执行失败: {result.get('message', '未知错误')}")
                return 1

        except KeyboardInterrupt:
            logger.warning("🛑 --only-images工作流程被用户中断")
            return 130  # 标准的中断退出码

        except Exception as e:
            logger.error(f"优化工作流程执行失败: {e}")
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
            debug=args.debug,
            max_concurrent=max_concurrent
        )

        # 执行智能采集 - 支持Pin ID模式
        if hasattr(args, 'mode') and args.mode == "pin_expansion":
            # Pin扩展模式
            pins = await scraper.scrape(
                pin_id=args.pin_id,
                count=args.count
            )
        else:
            # 关键词搜索模式（保持向后兼容）
            pins = await scraper.scrape(
                query=args.query,
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
        # 强制退出，避免asyncio清理警告
        import os
        os._exit(1)
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
