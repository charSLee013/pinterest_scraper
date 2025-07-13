#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫网络连接测试工具

用于诊断和测试Pinterest爬虫的网络连接问题
"""

import asyncio
import argparse
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.network_diagnostics import run_diagnostics
from src.core.browser_manager import BrowserManager
from loguru import logger


async def test_basic_connection():
    """测试基础连接"""
    logger.info("🔍 开始基础连接测试...")
    
    try:
        browser = BrowserManager(
            timeout=60,  # 增加超时时间
            headless=True,
            enable_network_interception=False
        )
        
        if not await browser.start():
            logger.error("❌ 浏览器启动失败")
            return False
        
        # 测试访问Pinterest主页
        if await browser.navigate("https://www.pinterest.com/"):
            logger.info("✅ Pinterest主页访问成功")
            
            # 测试搜索页面
            search_url = "https://www.pinterest.com/search/pins/?q=test"
            if await browser.navigate(search_url):
                logger.info("✅ Pinterest搜索页面访问成功")
                result = True
            else:
                logger.error("❌ Pinterest搜索页面访问失败")
                result = False
        else:
            logger.error("❌ Pinterest主页访问失败")
            result = False
        
        await browser.stop()
        return result
        
    except Exception as e:
        logger.error(f"❌ 连接测试失败: {e}")
        return False


async def test_with_different_configs():
    """测试不同配置"""
    logger.info("🔧 测试不同浏览器配置...")
    
    configs = [
        {
            "name": "默认配置",
            "headless": True,
            "timeout": 30,
            "browser_type": "chromium"
        },
        {
            "name": "增加超时",
            "headless": True,
            "timeout": 60,
            "browser_type": "chromium"
        },
        {
            "name": "非无头模式",
            "headless": False,
            "timeout": 60,
            "browser_type": "chromium"
        }
    ]
    
    results = []
    
    for config in configs:
        logger.info(f"测试配置: {config['name']}")
        
        try:
            browser = BrowserManager(
                timeout=config["timeout"],
                headless=config["headless"],
                browser_type=config["browser_type"],
                enable_network_interception=False
            )
            
            if await browser.start():
                success = await browser.navigate("https://www.pinterest.com/search/pins/?q=building")
                await browser.stop()
                
                results.append({
                    "config": config["name"],
                    "success": success
                })
                
                if success:
                    logger.info(f"✅ {config['name']} 成功")
                else:
                    logger.error(f"❌ {config['name']} 失败")
            else:
                logger.error(f"❌ {config['name']} 浏览器启动失败")
                results.append({
                    "config": config["name"],
                    "success": False
                })
                
        except Exception as e:
            logger.error(f"❌ {config['name']} 测试异常: {e}")
            results.append({
                "config": config["name"],
                "success": False,
                "error": str(e)
            })
    
    return results


async def test_pinterest_scraper():
    """测试Pinterest爬虫"""
    logger.info("🕷️ 测试Pinterest爬虫...")
    
    try:
        from src.core.pinterest_scraper import PinterestScraper
        
        scraper = PinterestScraper(
            download_images=False,  # 不下载图片，只测试数据采集
            debug=True
        )
        
        # 测试小规模采集
        pins = await scraper.scrape(query="building", count=5)
        
        await scraper.close()
        
        if pins and len(pins) > 0:
            logger.info(f"✅ Pinterest爬虫测试成功，采集到 {len(pins)} 个Pin")
            return True
        else:
            logger.error("❌ Pinterest爬虫测试失败，未采集到数据")
            return False
            
    except Exception as e:
        logger.error(f"❌ Pinterest爬虫测试异常: {e}")
        return False


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Pinterest爬虫网络连接测试")
    parser.add_argument("--proxy", help="代理服务器地址")
    parser.add_argument("--full", action="store_true", help="运行完整诊断")
    parser.add_argument("--basic", action="store_true", help="只运行基础连接测试")
    parser.add_argument("--configs", action="store_true", help="测试不同配置")
    parser.add_argument("--scraper", action="store_true", help="测试Pinterest爬虫")
    
    args = parser.parse_args()
    
    logger.info("🚀 Pinterest爬虫网络连接测试工具")
    logger.info("=" * 60)
    
    success_count = 0
    total_tests = 0
    
    if args.full or not any([args.basic, args.configs, args.scraper]):
        # 运行完整诊断
        logger.info("🔍 运行完整网络诊断...")
        await run_diagnostics(args.proxy)
        total_tests += 1
        success_count += 1  # 诊断总是"成功"的
    
    if args.basic or not any([args.full, args.configs, args.scraper]):
        # 基础连接测试
        total_tests += 1
        if await test_basic_connection():
            success_count += 1
    
    if args.configs:
        # 配置测试
        total_tests += 1
        results = await test_with_different_configs()
        if any(r["success"] for r in results):
            success_count += 1
    
    if args.scraper:
        # 爬虫测试
        total_tests += 1
        if await test_pinterest_scraper():
            success_count += 1
    
    # 输出总结
    logger.info("=" * 60)
    logger.info(f"📊 测试完成: {success_count}/{total_tests} 项测试通过")
    
    if success_count == total_tests:
        logger.info("🎉 所有测试通过！网络连接正常")
        return 0
    else:
        logger.warning("⚠️  部分测试失败，请检查网络配置")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
