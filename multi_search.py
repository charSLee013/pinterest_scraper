#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest多关键词并发搜索脚本
此脚本可以从文件或文件夹读取多个关键词并执行并发搜索
"""

import argparse
import os
import sys
import time
from typing import List

from loguru import logger

import config
from concurrent_search import concurrent_search


def setup_logger(log_level: str = "INFO", log_file: bool = True):
    """设置日志配置"""
    logger.remove()
    logger.add(sys.stderr, format=config.LOG_FORMAT, level=log_level, colorize=True)

    if log_file:
        os.makedirs("logs", exist_ok=True)
        log_filename = f"logs/multi_search_{time.strftime('%Y%m%d_%H%M%S')}.log"
        logger.add(
            log_filename,
            format=config.LOG_FORMAT,
            level=log_level,
            rotation="10 MB",
            compression="zip",
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
    parser = argparse.ArgumentParser(description="Pinterest多关键词并发搜索工具")

    # 必选参数组 (文件或目录)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "-f",
        "--file",
        type=str,
        help="包含搜索关键词的文件路径，每行一个关键词",
    )
    input_group.add_argument(
        "-d",
        "--directory",
        type=str,
        help="包含搜索关键词文件的目录路径，每个文件的每行都是一个关键词",
    )

    # 常规参数
    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=50,
        help="每个关键词要获取的pin数量 (默认: 50)",
    )
    parser.add_argument(
        "-o", "--output", type=str, default="output", help="输出目录 (默认: output)"
    )
    parser.add_argument(
        "-p",
        "--proxy",
        type=str,
        help="代理服务器地址 (格式: http://user:pass@host:port)",
    )

    # 高级选项
    parser.add_argument(
        "--max-concurrent", type=int, default=3, help="最大并发搜索数 (默认: 3)"
    )
    parser.add_argument("--debug", action="store_true", help="启用调试模式")
    parser.add_argument(
        "--no-images", action="store_true", help="不下载图片，仅获取元数据"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=0,
        help="图片下载的最大并发线程数 (默认: 自动)",
    )
    parser.add_argument(
        "--timeout", type=int, default=30, help="请求超时时间(秒) (默认: 30)"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="日志级别 (默认: INFO)",
    )
    parser.add_argument(
        "--viewport-width", type=int, default=1920, help="浏览器视口宽度 (默认: 1920)"
    )
    parser.add_argument(
        "--viewport-height", type=int, default=1080, help="浏览器视口高度 (默认: 1080)"
    )
    parser.add_argument(
        "--zoom-level", type=int, default=67, help="浏览器页面缩放级别百分比 (默认: 30)"
    )

    args = parser.parse_args()

    # 设置日志
    setup_logger(args.log_level)

    # 读取关键词
    terms = []
    if args.file:
        terms = read_terms_from_file(args.file)
        source_desc = f"文件 {args.file}"
    else:
        terms = read_terms_from_directory(args.directory)
        source_desc = f"目录 {args.directory}"

    if not terms:
        logger.error(f"未能从{source_desc}中读取到任何关键词")
        return 1

    logger.info(f"从{source_desc}读取了 {len(terms)} 个搜索关键词")

    # 输出一些关键词作为示例
    if len(terms) > 5:
        preview_terms = terms[:5]
        logger.info(f"前5个关键词: {', '.join(preview_terms)}...")
    else:
        logger.info(f"关键词: {', '.join(terms)}")

    # 执行并发搜索
    try:
        start_time = time.time()

        results = concurrent_search(
            search_terms=terms,
            count_per_term=args.count,
            output_dir=args.output,
            max_concurrent=args.max_concurrent,
            proxy=args.proxy,
            debug=args.debug,
            timeout=args.timeout,
            max_workers=args.max_workers,
            download_images=not args.no_images,
        )

        end_time = time.time()

        # 输出统计信息
        total_pins = sum(len(pins) for pins in results.values())
        success_terms = sum(1 for pins in results.values() if pins)

        logger.success(
            f"并发搜索完成! 处理了 {len(terms)} 个关键词，成功: {success_terms}"
        )
        logger.success(
            f"总共获取了 {total_pins} 个pins，总耗时: {end_time - start_time:.2f} 秒"
        )
        logger.success(f"结果已保存到 {args.output} 目录")

        return 0

    except KeyboardInterrupt:
        logger.warning("用户中断搜索")
        return 1
    except Exception as e:
        logger.error(f"执行搜索过程中出错: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
