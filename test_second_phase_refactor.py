#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试第二阶段重构后的逻辑
验证新的第二阶段实现是否正常工作
"""

import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.smart_scraper import SmartScraper
from src.core.database.repository import SQLiteRepository
from loguru import logger
import uuid

async def test_second_phase_refactor():
    """测试重构后的第二阶段逻辑"""
    logger.info("=== 测试第二阶段重构逻辑 ===")
    
    # 初始化Repository和SmartScraper
    repo = SQLiteRepository(keyword="cat", output_dir="output")
    session_id = f"test_refactor_{uuid.uuid4().hex[:8]}"
    
    scraper = SmartScraper(
        repository=repo,
        session_id=session_id,
        debug=True
    )
    
    # 测试参数
    query = "cat"
    target_count = 10  # 小数量测试
    
    logger.info(f"开始测试，目标: {target_count} 个Pin")
    
    try:
        # 执行采集
        pins = await scraper.scrape(
            query=query,
            target_count=target_count,
            repository=repo,
            session_id=session_id
        )
        
        logger.info(f"采集完成，获得 {len(pins)} 个Pin")
        
        # 验证结果
        if pins:
            logger.info("✅ 第二阶段重构测试成功")
            
            # 显示前3个Pin的信息
            for i, pin in enumerate(pins[:3], 1):
                logger.info(f"Pin {i}:")
                logger.info(f"  ID: {pin.get('id', 'N/A')}")
                logger.info(f"  标题: {pin.get('title', 'N/A')[:50]}...")
                logger.info(f"  有图片URL: {bool(pin.get('largest_image_url'))}")
            
            # 检查数据库中的数据
            db_pins = repo.load_pins_with_images(query, limit=20)
            logger.info(f"数据库中有图片的Pin数量: {len(db_pins)}")
            
            # 获取统计信息
            stats = scraper.get_stats()
            logger.info(f"采集统计: {stats}")
            
        else:
            logger.warning("⚠️ 未获取到Pin数据")
            
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await scraper.close()

async def test_database_driven_expansion():
    """测试数据库驱动的扩展逻辑"""
    logger.info("\n=== 测试数据库驱动扩展逻辑 ===")
    
    repo = SQLiteRepository(keyword="cat", output_dir="output")
    
    # 1. 检查数据库中有图片的pins
    pins_with_images = repo.load_pins_with_images("cat", limit=10)
    logger.info(f"数据库中有图片的Pin数量: {len(pins_with_images)}")
    
    if pins_with_images:
        # 2. 模拟从set中pop一个pin
        pin_set = set([pin['id'] for pin in pins_with_images])
        logger.info(f"Pin set大小: {len(pin_set)}")
        
        if pin_set:
            test_pin_id = pin_set.pop()
            logger.info(f"测试Pin ID: {test_pin_id}")
            
            # 3. 模拟保存新pins并获取新增id
            test_new_pins = [
                {
                    'id': f'expansion_test_{uuid.uuid4().hex[:8]}',
                    'title': f'扩展测试Pin for {test_pin_id}',
                    'description': '数据库驱动扩展测试',
                    'largest_image_url': 'https://example.com/expansion_test.jpg',
                    'image_urls': {'1': 'expansion_url1'}
                }
            ]
            
            session_id = f"expansion_test_{uuid.uuid4().hex[:8]}"
            new_pin_ids = repo.save_pins_and_get_new_ids(test_new_pins, "cat", session_id)
            
            logger.info(f"新增Pin IDs: {new_pin_ids}")
            
            if new_pin_ids:
                # 4. 将新pin id加入set
                pin_set.update(new_pin_ids)
                logger.info(f"更新后Pin set大小: {len(pin_set)}")
                logger.info("✅ 数据库驱动扩展逻辑测试成功")
            else:
                logger.warning("⚠️ 未获得新增Pin ID")
        else:
            logger.warning("⚠️ Pin set为空")
    else:
        logger.warning("⚠️ 数据库中没有有图片的Pin")

async def test_new_methods():
    """测试新的Repository方法"""
    logger.info("\n=== 测试新的Repository方法 ===")
    
    repo = SQLiteRepository(keyword="cat", output_dir="output")
    
    # 测试load_pins_with_images
    pins = repo.load_pins_with_images("cat", limit=5)
    logger.info(f"load_pins_with_images: {len(pins)} 个Pin")
    
    if pins:
        logger.info(f"第一个Pin: {pins[0]['id']}")
        logger.info(f"有largest_image_url: {bool(pins[0].get('largest_image_url'))}")
        logger.info(f"有image_urls: {bool(pins[0].get('image_urls'))}")
    
    # 测试save_pins_and_get_new_ids
    test_pins = [
        {
            'id': f'method_test_{uuid.uuid4().hex[:8]}',
            'title': '方法测试Pin',
            'description': '测试save_pins_and_get_new_ids方法',
            'largest_image_url': 'https://example.com/method_test.jpg',
            'image_urls': {'1': 'method_test_url1'}
        }
    ]
    
    session_id = f"method_test_{uuid.uuid4().hex[:8]}"
    new_ids = repo.save_pins_and_get_new_ids(test_pins, "cat", session_id)
    logger.info(f"save_pins_and_get_new_ids: {new_ids}")
    
    if new_ids:
        logger.info("✅ 新方法测试成功")
    else:
        logger.warning("⚠️ 新方法测试失败")

async def main():
    """主测试函数"""
    try:
        # 测试新的Repository方法
        await test_new_methods()
        
        # 测试数据库驱动扩展逻辑
        await test_database_driven_expansion()
        
        # 测试完整的第二阶段重构逻辑
        await test_second_phase_refactor()
        
        logger.info("\n🎉 所有测试完成")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
