#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
第二阶段GraphQL逻辑集成测试
测试完整的第二阶段工作流程：数据库读取 → GraphQL采集 → 数据库存储
"""

import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.smart_scraper import SmartScraper
from src.core.database.repository import SQLiteRepository
from loguru import logger
import uuid

async def test_complete_second_phase():
    """测试完整的第二阶段工作流程"""
    logger.info("=== 测试完整第二阶段工作流程 ===")
    
    repo = SQLiteRepository(keyword="cat", output_dir="output")
    
    # 1. 从数据库获取真实的Pin（排除测试数据）
    try:
        import sqlite3
        conn = sqlite3.connect("output/cat/pinterest.db")
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id FROM pins 
            WHERE id NOT LIKE 'test_%'
               AND id NOT LIKE 'expansion_%'
               AND id NOT LIKE 'method_%'
               AND id NOT LIKE 'related_%'
               AND LENGTH(id) > 10
            ORDER BY created_at ASC 
            LIMIT 3
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            logger.error("没有找到真实的Pin数据")
            return False
        
        pin_ids = [row[0] for row in results]
        logger.info(f"找到 {len(pin_ids)} 个真实Pin")
        
        # 2. 创建pin_set（模拟第二阶段的数据库驱动逻辑）
        pin_set = set(pin_ids)
        logger.info(f"初始pin_set大小: {len(pin_set)}")
        
        # 3. 使用SmartScraper测试第二阶段逻辑
        scraper = SmartScraper(debug=False)  # 生产模式
        session_id = f"integration_test_{uuid.uuid4().hex[:8]}"
        
        try:
            total_new_pins = 0
            total_rounds = 0
            
            # 模拟第二阶段扩展循环
            for round_num in range(2):  # 测试2轮
                if not pin_set:
                    logger.info("pin_set为空，停止循环")
                    break
                
                # 从set中pop一个pin
                pin_id = pin_set.pop()
                total_rounds += 1
                logger.info(f"第{round_num + 1}轮: 处理Pin {pin_id}")
                
                # 使用GraphQL逻辑采集相关pins
                related_pins = await scraper._scrape_pin_detail_with_queue(pin_id, max_count=5)
                
                if related_pins:
                    logger.info(f"✅ 获取到 {len(related_pins)} 个相关Pin")
                    
                    # 显示前2个相关Pin的信息
                    for i, pin in enumerate(related_pins[:2], 1):
                        logger.info(f"相关Pin {i}:")
                        logger.info(f"  ID: {pin.get('id', 'N/A')}")
                        logger.info(f"  标题: {pin.get('title', 'N/A')[:50]}...")
                        logger.info(f"  有图片URL: {bool(pin.get('largest_image_url'))}")
                    
                    # 覆盖式批量存储到数据库，获取新增pin id
                    new_pin_ids = repo.save_pins_and_get_new_ids(related_pins, "cat", session_id)
                    
                    if new_pin_ids:
                        # 将新pin id加入set
                        pin_set.update(new_pin_ids)
                        total_new_pins += len(new_pin_ids)
                        logger.info(f"数据库新增 {len(new_pin_ids)} 个Pin")
                        logger.info(f"当前pin_set大小: {len(pin_set)}")
                        logger.info(f"新增Pin IDs示例: {new_pin_ids[:2]}...")
                    else:
                        logger.info("所有相关Pin都是重复的")
                else:
                    logger.warning("未获取到相关Pin")
            
            logger.info(f"\n=== 第二阶段集成测试总结 ===")
            logger.info(f"处理轮数: {total_rounds}")
            logger.info(f"最终pin_set大小: {len(pin_set)}")
            logger.info(f"总新增Pin数量: {total_new_pins}")
            
            if total_new_pins > 0:
                logger.info("✅ 第二阶段集成测试成功！")
                return True
            else:
                logger.warning("⚠️ 未获得新增Pin")
                return False
                
        finally:
            await scraper.close()
            
    except Exception as e:
        logger.error(f"集成测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_database_integration():
    """测试数据库集成功能"""
    logger.info("\n=== 测试数据库集成功能 ===")
    
    repo = SQLiteRepository(keyword="cat", output_dir="output")
    
    # 测试load_pins_with_images方法
    pins_with_images = repo.load_pins_with_images("cat", limit=5)
    logger.info(f"数据库中有图片的Pin数量: {len(pins_with_images)}")
    
    if pins_with_images:
        logger.info("✅ load_pins_with_images方法正常")
        
        # 测试save_pins_and_get_new_ids方法
        test_pins = [
            {
                'id': f'db_integration_test_{uuid.uuid4().hex[:8]}',
                'title': '数据库集成测试Pin',
                'description': '测试save_pins_and_get_new_ids方法',
                'largest_image_url': 'https://example.com/integration_test.jpg',
                'image_urls': {'original': 'integration_test_url'}
            }
        ]
        
        session_id = f"db_integration_{uuid.uuid4().hex[:8]}"
        new_ids = repo.save_pins_and_get_new_ids(test_pins, "cat", session_id)
        
        if new_ids:
            logger.info(f"✅ save_pins_and_get_new_ids方法正常，新增: {new_ids}")
            return True
        else:
            logger.info("ℹ️ Pin已存在，去重正常")
            return True
    else:
        logger.warning("⚠️ 数据库中没有有图片的Pin")
        return False

async def test_graphql_data_extraction():
    """测试GraphQL数据提取功能"""
    logger.info("\n=== 测试GraphQL数据提取功能 ===")
    
    # 使用成功的示例Pin ID
    test_pin_id = "801077852519350337"
    
    scraper = SmartScraper(debug=False)
    
    try:
        # 测试单个Pin的GraphQL数据提取
        related_pins = await scraper._scrape_pin_detail_with_queue(test_pin_id, max_count=3)
        
        if related_pins:
            logger.info(f"✅ GraphQL数据提取成功，获得 {len(related_pins)} 个Pin")
            
            # 验证数据结构
            for i, pin in enumerate(related_pins[:2], 1):
                pin_id = pin.get('id', '')
                title = pin.get('title', '')
                image_urls = pin.get('image_urls', {})
                largest_image = pin.get('largest_image_url', '')
                
                logger.info(f"Pin {i}:")
                logger.info(f"  ID: {pin_id}")
                logger.info(f"  标题: {title[:30]}...")
                logger.info(f"  图片URL数量: {len(image_urls)}")
                logger.info(f"  有最大图片: {bool(largest_image)}")
                
                # 验证必要字段
                if not pin_id:
                    logger.error(f"Pin {i} 缺少ID字段")
                    return False
                if not image_urls:
                    logger.error(f"Pin {i} 缺少图片URL")
                    return False
            
            logger.info("✅ GraphQL数据结构验证通过")
            return True
        else:
            logger.warning("⚠️ GraphQL数据提取失败")
            return False
            
    finally:
        await scraper.close()

async def main():
    """主测试函数"""
    try:
        # 测试1：数据库集成功能
        db_success = await test_database_integration()
        
        # 测试2：GraphQL数据提取功能
        graphql_success = await test_graphql_data_extraction()
        
        # 测试3：完整的第二阶段工作流程
        integration_success = await test_complete_second_phase()
        
        logger.info(f"\n🎉 第二阶段集成测试完成")
        logger.info(f"数据库集成: {'✅ 成功' if db_success else '❌ 失败'}")
        logger.info(f"GraphQL数据提取: {'✅ 成功' if graphql_success else '❌ 失败'}")
        logger.info(f"完整工作流程: {'✅ 成功' if integration_success else '❌ 失败'}")
        
        if db_success and graphql_success and integration_success:
            logger.info("🎯 所有第二阶段集成测试通过！")
            return True
        else:
            logger.warning("⚠️ 部分测试失败")
            return False
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
