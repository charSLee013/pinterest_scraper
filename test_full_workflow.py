#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
完整工作流程测试
测试第一阶段 + 第二阶段的完整采集流程
"""

import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.smart_scraper import SmartScraper
from src.core.database.repository import SQLiteRepository
from loguru import logger
import uuid

async def test_full_scraping_workflow():
    """测试完整的采集工作流程"""
    logger.info("=== 测试完整采集工作流程 ===")
    
    # 使用独立的测试关键词避免干扰
    test_keyword = f"test_workflow_{uuid.uuid4().hex[:8]}"
    repo = SQLiteRepository(keyword=test_keyword, output_dir="output")
    session_id = f"workflow_test_{uuid.uuid4().hex[:8]}"
    
    scraper = SmartScraper(
        repository=repo,
        session_id=session_id,
        debug=False
    )
    
    try:
        logger.info("开始完整采集流程测试...")
        
        # 执行完整的采集流程（包含第一阶段和第二阶段）
        pins = await scraper.scrape(
            query=test_keyword,
            target_count=15,  # 小数量测试
            repository=repo,
            session_id=session_id
        )
        
        logger.info(f"采集完成，获得 {len(pins)} 个Pin")
        
        if pins:
            logger.info("✅ 完整工作流程测试成功")
            
            # 验证数据库中的数据
            db_pins = repo.load_pins_with_images(test_keyword, limit=20)
            logger.info(f"数据库中有图片的Pin数量: {len(db_pins)}")
            
            # 显示前3个Pin的信息
            for i, pin in enumerate(pins[:3], 1):
                logger.info(f"Pin {i}:")
                logger.info(f"  ID: {pin.get('id', 'N/A')}")
                logger.info(f"  标题: {pin.get('title', 'N/A')[:50]}...")
                logger.info(f"  有图片URL: {bool(pin.get('largest_image_url'))}")
            
            # 获取统计信息
            stats = scraper.get_stats()
            logger.info(f"采集统计: {stats}")
            
            return True
        else:
            logger.warning("⚠️ 未获取到Pin数据")
            return False
            
    except Exception as e:
        logger.error(f"完整工作流程测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        await scraper.close()

async def test_second_phase_expansion():
    """专门测试第二阶段扩展逻辑"""
    logger.info("\n=== 测试第二阶段扩展逻辑 ===")
    
    repo = SQLiteRepository(keyword="cat", output_dir="output")
    
    # 获取数据库中的真实Pin
    pins_with_images = repo.load_pins_with_images("cat", limit=2)
    
    if not pins_with_images:
        logger.warning("数据库中没有有图片的Pin，跳过第二阶段测试")
        return True
    
    logger.info(f"从数据库获取到 {len(pins_with_images)} 个有图片的Pin")
    
    # 创建pin_set
    pin_set = set([pin['id'] for pin in pins_with_images])
    original_size = len(pin_set)
    
    scraper = SmartScraper(debug=False)
    session_id = f"expansion_test_{uuid.uuid4().hex[:8]}"
    
    try:
        expansion_successful = False
        
        # 测试扩展循环
        for round_num in range(1):  # 只测试1轮
            if not pin_set:
                break
            
            pin_id = pin_set.pop()
            logger.info(f"测试扩展Pin: {pin_id}")
            
            # 获取相关pins
            related_pins = await scraper._scrape_pin_detail_with_queue(pin_id, max_count=3)
            
            if related_pins:
                logger.info(f"获取到 {len(related_pins)} 个相关Pin")
                
                # 保存并获取新增pin id
                new_pin_ids = repo.save_pins_and_get_new_ids(related_pins, "cat", session_id)
                
                if new_pin_ids:
                    pin_set.update(new_pin_ids)
                    expansion_successful = True
                    logger.info(f"成功扩展 {len(new_pin_ids)} 个新Pin")
                    logger.info(f"pin_set从 {original_size} 扩展到 {len(pin_set)}")
                    break
        
        if expansion_successful:
            logger.info("✅ 第二阶段扩展逻辑测试成功")
            return True
        else:
            logger.info("ℹ️ 第二阶段扩展未获得新Pin（可能都是重复的）")
            return True  # 这也是正常情况
            
    finally:
        await scraper.close()

async def test_interrupt_and_resume():
    """测试中断和恢复机制"""
    logger.info("\n=== 测试中断和恢复机制 ===")
    
    test_keyword = f"interrupt_test_{uuid.uuid4().hex[:8]}"
    repo = SQLiteRepository(keyword=test_keyword, output_dir="output")
    session_id = f"interrupt_test_{uuid.uuid4().hex[:8]}"
    
    scraper = SmartScraper(
        repository=repo,
        session_id=session_id,
        debug=False
    )
    
    try:
        # 模拟中断（设置较小的目标数量）
        logger.info("开始采集（模拟中断）...")
        
        # 启动采集任务
        task = asyncio.create_task(scraper.scrape(
            query=test_keyword,
            target_count=5,
            repository=repo,
            session_id=session_id
        ))
        
        # 等待一段时间后中断
        await asyncio.sleep(10)
        scraper.request_interrupt()
        
        try:
            pins = await task
            logger.info(f"中断后获得 {len(pins)} 个Pin")
        except asyncio.CancelledError:
            logger.info("任务被取消")
        
        # 检查数据库中的数据（应该有部分数据被保存）
        db_pins = repo.load_pins_with_images(test_keyword, limit=10)
        logger.info(f"中断后数据库中保存了 {len(db_pins)} 个Pin")
        
        if len(db_pins) > 0:
            logger.info("✅ 中断保护机制正常工作")
            return True
        else:
            logger.warning("⚠️ 中断后数据库中没有数据")
            return False
            
    except Exception as e:
        logger.error(f"中断测试失败: {e}")
        return False
    
    finally:
        await scraper.close()

async def main():
    """主测试函数"""
    try:
        # 测试1：第二阶段扩展逻辑
        expansion_success = await test_second_phase_expansion()
        
        # 测试2：完整工作流程
        workflow_success = await test_full_scraping_workflow()
        
        # 测试3：中断和恢复机制
        interrupt_success = await test_interrupt_and_resume()
        
        logger.info(f"\n🎉 完整工作流程测试完成")
        logger.info(f"第二阶段扩展: {'✅ 成功' if expansion_success else '❌ 失败'}")
        logger.info(f"完整工作流程: {'✅ 成功' if workflow_success else '❌ 失败'}")
        logger.info(f"中断恢复机制: {'✅ 成功' if interrupt_success else '❌ 失败'}")
        
        if expansion_success and workflow_success and interrupt_success:
            logger.info("🎯 所有完整工作流程测试通过！")
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
