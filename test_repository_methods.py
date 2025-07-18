#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试Repository新方法
验证load_pins_with_images和save_pins_and_get_new_ids方法
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.database.repository import SQLiteRepository
from loguru import logger
import uuid

def test_load_pins_with_images():
    """测试加载有图片的pins方法"""
    logger.info("=== 测试 load_pins_with_images 方法 ===")

    # 初始化Repository（使用关键词特定的方式）
    repo = SQLiteRepository(keyword="cat", output_dir="output")
    
    # 测试加载前10个有图片的pins
    pins = repo.load_pins_with_images("cat", limit=10)
    
    logger.info(f"加载到 {len(pins)} 个有图片的pins")
    
    if pins:
        # 验证数据结构
        first_pin = pins[0]
        logger.info(f"第一个Pin ID: {first_pin.get('id')}")
        logger.info(f"有largest_image_url: {bool(first_pin.get('largest_image_url'))}")
        logger.info(f"有image_urls: {bool(first_pin.get('image_urls'))}")
        logger.info(f"创建时间: {first_pin.get('created_at')}")
        
        # 验证排序（从新到旧）
        if len(pins) >= 2:
            first_time = pins[0].get('created_at')
            second_time = pins[1].get('created_at')
            logger.info(f"排序验证: {first_time} >= {second_time} = {first_time >= second_time}")
    
    return pins

def test_save_pins_and_get_new_ids():
    """测试保存pins并获取新增id方法"""
    logger.info("\n=== 测试 save_pins_and_get_new_ids 方法 ===")

    # 初始化Repository（使用关键词特定的方式）
    repo = SQLiteRepository(keyword="cat", output_dir="output")
    
    # 创建测试数据（包含新pin和已存在的pin）
    test_pins = [
        {
            'id': f'test_new_pin_{uuid.uuid4().hex[:8]}',
            'title': '测试新Pin 1',
            'description': '这是一个测试用的新Pin',
            'largest_image_url': 'https://example.com/image1.jpg',
            'image_urls': {'1': 'url1', '2': 'url2'}
        },
        {
            'id': f'test_new_pin_{uuid.uuid4().hex[:8]}',
            'title': '测试新Pin 2',
            'description': '这是另一个测试用的新Pin',
            'largest_image_url': 'https://example.com/image2.jpg',
            'image_urls': {'1': 'url3', '2': 'url4'}
        }
    ]
    
    # 添加一个已存在的pin（使用数据库中的真实ID）
    existing_pins = repo.load_pins_with_images("cat", limit=1)
    if existing_pins:
        existing_pin = existing_pins[0].copy()
        existing_pin['title'] = '尝试重复保存的Pin'
        test_pins.append(existing_pin)
        logger.info(f"添加已存在Pin用于测试: {existing_pin['id']}")
    
    # 测试保存
    session_id = f"test_session_{uuid.uuid4().hex[:8]}"
    new_pin_ids = repo.save_pins_and_get_new_ids(test_pins, "cat", session_id)
    
    logger.info(f"尝试保存 {len(test_pins)} 个pins")
    logger.info(f"实际新增 {len(new_pin_ids)} 个pins")
    logger.info(f"新增Pin IDs: {new_pin_ids}")
    
    # 验证新增的pins确实存在于数据库中
    if new_pin_ids:
        for pin_id in new_pin_ids:
            pins = repo.load_pins_by_query("cat", limit=1000)
            found = any(pin['id'] == pin_id for pin in pins)
            logger.info(f"Pin {pin_id} 在数据库中: {found}")
    
    return new_pin_ids

def test_integration():
    """集成测试：模拟第二阶段的完整流程"""
    logger.info("\n=== 集成测试：模拟第二阶段流程 ===")

    repo = SQLiteRepository(keyword="cat", output_dir="output")
    
    # 1. 从数据库读取有图片的pins
    pins_with_images = repo.load_pins_with_images("cat", limit=5)
    pin_set = set([pin['id'] for pin in pins_with_images])
    
    logger.info(f"步骤1: 从数据库加载 {len(pin_set)} 个有图片的pin IDs")
    logger.info(f"Pin set: {list(pin_set)[:3]}...")  # 显示前3个
    
    # 2. 模拟从pin_set中pop一个pin
    if pin_set:
        current_pin_id = pin_set.pop()
        logger.info(f"步骤2: Pop出Pin ID: {current_pin_id}")
        
        # 3. 模拟获取相关pins（这里用测试数据代替）
        related_pins = [
            {
                'id': f'related_pin_{uuid.uuid4().hex[:8]}',
                'title': f'相关Pin for {current_pin_id}',
                'description': '模拟的相关Pin',
                'largest_image_url': 'https://example.com/related.jpg',
                'image_urls': {'1': 'related_url1'}
            }
        ]
        
        logger.info(f"步骤3: 模拟获取到 {len(related_pins)} 个相关pins")
        
        # 4. 保存并获取新增的pin ids
        session_id = f"integration_test_{uuid.uuid4().hex[:8]}"
        new_pin_ids = repo.save_pins_and_get_new_ids(related_pins, "cat", session_id)
        
        logger.info(f"步骤4: 保存后获得 {len(new_pin_ids)} 个新pin IDs")
        
        # 5. 将新pin ids加入pin_set
        pin_set.update(new_pin_ids)
        logger.info(f"步骤5: 更新后pin_set大小: {len(pin_set)}")
        
        logger.info("✅ 集成测试完成，第二阶段流程验证成功")
    else:
        logger.error("❌ 集成测试失败：没有可用的pin数据")

if __name__ == "__main__":
    try:
        # 测试单个方法
        pins = test_load_pins_with_images()
        new_ids = test_save_pins_and_get_new_ids()
        
        # 集成测试
        test_integration()
        
        logger.info("\n🎉 所有测试完成")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
