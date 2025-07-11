#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫基础使用示例 - 激进重构版

演示新的统一API的使用方法
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.pinterest_scraper import PinterestScraper
from loguru import logger


def basic_keyword_search():
    """基础关键词搜索示例"""
    logger.info("=== 基础关键词搜索示例 ===")

    # 创建爬虫实例
    scraper = PinterestScraper(
        output_dir="output/basic_example",
        download_images=True,
        debug=False
    )

    # 搜索关键词 - 使用统一的scrape方法
    query = "nature photography"
    count = 50

    logger.info(f"开始搜索关键词: '{query}'，目标数量: {count}")
    pins = scraper.scrape(query=query, count=count)
    
    if pins:
        logger.success(f"✅ 成功获取 {len(pins)} 个Pin")
        
        # 显示前3个Pin的信息
        for i, pin in enumerate(pins[:3]):
            logger.info(f"Pin {i+1}:")
            logger.info(f"  ID: {pin.get('id', 'N/A')}")
            logger.info(f"  标题: {pin.get('title', 'N/A')[:50]}...")
            logger.info(f"  图片URL: {pin.get('largest_image_url', 'N/A')}")
            logger.info(f"  已下载: {pin.get('downloaded', False)}")
    else:
        logger.error("❌ 未能获取到Pin数据")


def basic_url_scraping():
    """基础URL爬取示例"""
    logger.info("=== 基础URL爬取示例 ===")

    # 创建爬虫实例
    scraper = PinterestScraper(
        output_dir="output/url_example",
        download_images=False,  # 不下载图片，只获取元数据
        debug=False
    )

    # 爬取Pinterest官方页面 - 使用统一的scrape方法
    url = "https://www.pinterest.com/pinterest/"
    count = 30

    logger.info(f"开始爬取URL: {url}，目标数量: {count}")
    pins = scraper.scrape(url=url, count=count)
    
    if pins:
        logger.success(f"✅ 成功获取 {len(pins)} 个Pin")
        
        # 统计创作者信息
        creators = {}
        for pin in pins:
            creator_name = pin.get('creator', {}).get('name', 'Unknown')
            creators[creator_name] = creators.get(creator_name, 0) + 1
        
        logger.info("创作者统计:")
        for creator, count in sorted(creators.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"  {creator}: {count} 个Pin")
    else:
        logger.error("❌ 未能获取到Pin数据")


def smart_strategy_example():
    """智能策略选择示例"""
    logger.info("=== 智能策略选择示例 ===")

    scraper = PinterestScraper(
        output_dir="output/smart_strategy",
        download_images=False,
        debug=False
    )

    # 小量数据 - 自动选择基础策略
    logger.info("小量数据采集（自动选择基础策略）")
    pins_small = scraper.scrape(query="minimalist design", count=50)
    logger.info(f"获取到 {len(pins_small)} 个Pin")

    # 中量数据 - 自动选择增强策略
    logger.info("中量数据采集（自动选择增强策略）")
    pins_medium = scraper.scrape(query="vintage photography", count=500)
    logger.info(f"获取到 {len(pins_medium)} 个Pin")

    # 大量数据 - 自动选择混合策略（关键词搜索 + Pin详情页深度扩展）
    logger.info("大量数据采集（自动选择混合策略：关键词搜索 + Pin详情页深度扩展）")
    pins_large = scraper.scrape(query="landscape art", count=2000)
    logger.info(f"获取到 {len(pins_large)} 个Pin - 突破传统限制！")


def custom_configuration_example():
    """自定义配置示例"""
    logger.info("=== 自定义配置示例 ===")

    # 使用自定义配置
    scraper = PinterestScraper(
        output_dir="output/custom_config",
        proxy=None,  # 如果需要代理，设置为 "http://user:pass@host:port"
        debug=True,  # 启用调试模式
        download_images=True
    )

    # 搜索高质量摄影作品
    pins = scraper.scrape(query="professional photography", count=100)
    
    if pins:
        logger.success(f"✅ 使用自定义配置成功获取 {len(pins)} 个Pin")
        
        # 分析图片尺寸分布
        size_stats = {}
        for pin in pins:
            image_urls = pin.get('image_urls', {})
            available_sizes = list(image_urls.keys())
            size_key = f"{len(available_sizes)} 种尺寸"
            size_stats[size_key] = size_stats.get(size_key, 0) + 1
        
        logger.info("图片尺寸分布:")
        for size, count in size_stats.items():
            logger.info(f"  {size}: {count} 个Pin")


def main():
    """主函数 - 运行所有示例"""
    logger.info("🎯 Pinterest爬虫基础使用示例 - 激进重构版")
    logger.info("="*50)

    try:
        # 运行各种示例
        basic_keyword_search()
        print()

        basic_url_scraping()
        print()

        smart_strategy_example()
        print()

        custom_configuration_example()

        logger.success("🎉 所有示例运行完成！")

    except KeyboardInterrupt:
        logger.warning("⚠️ 用户中断执行")
    except Exception as e:
        logger.error(f"❌ 示例运行出错: {e}")


if __name__ == "__main__":
    main()
