#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库检查测试脚本
检查cat数据库中的数据情况，为第二阶段重构做准备
"""

import sqlite3
import json
from typing import Dict, List, Tuple
from loguru import logger

def check_database_status(db_path: str = "output/cat/pinterest.db") -> Dict:
    """检查数据库状态"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查总Pin数量
        cursor.execute("SELECT COUNT(*) FROM pins")
        total_pins = cursor.fetchone()[0]
        
        # 检查有图片链接的Pin数量
        cursor.execute("""
            SELECT COUNT(*) FROM pins 
            WHERE (largest_image_url IS NOT NULL AND largest_image_url != '') 
               OR (image_urls IS NOT NULL AND image_urls != '' AND image_urls != '[]')
        """)
        pins_with_images = cursor.fetchone()[0]
        
        # 检查查询关键词
        cursor.execute("SELECT DISTINCT query FROM pins")
        queries = [row[0] for row in cursor.fetchall()]
        
        # 获取最新的10个有图片的Pin，排除测试数据
        cursor.execute("""
            SELECT id, title, largest_image_url, image_urls, created_at
            FROM pins
            WHERE (largest_image_url IS NOT NULL AND largest_image_url != '')
               OR (image_urls IS NOT NULL AND image_urls != '' AND image_urls != '[]')
               AND id NOT LIKE 'test_%'
               AND id NOT LIKE 'expansion_%'
               AND id NOT LIKE 'method_%'
               AND id NOT LIKE 'related_%'
            ORDER BY created_at ASC
            LIMIT 10
        """)
        sample_pins = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_pins': total_pins,
            'pins_with_images': pins_with_images,
            'queries': queries,
            'sample_pins': sample_pins
        }
        
    except Exception as e:
        logger.error(f"检查数据库失败: {e}")
        return {}

def analyze_pin_image_urls(db_path: str = "output/cat/pinterest.db", limit: int = 5) -> List[Dict]:
    """分析Pin的图片URL结构"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, largest_image_url, image_urls 
            FROM pins 
            WHERE (largest_image_url IS NOT NULL AND largest_image_url != '') 
               OR (image_urls IS NOT NULL AND image_urls != '' AND image_urls != '[]')
            ORDER BY created_at DESC 
            LIMIT ?
        """, (limit,))
        
        results = []
        for row in cursor.fetchall():
            pin_id, largest_url, image_urls_str = row
            
            # 解析image_urls JSON
            image_urls = {}
            if image_urls_str:
                try:
                    image_urls = json.loads(image_urls_str)
                except:
                    pass
            
            results.append({
                'id': pin_id,
                'largest_image_url': largest_url,
                'image_urls': image_urls,
                'has_largest': bool(largest_url),
                'has_image_urls': bool(image_urls),
                'image_urls_count': len(image_urls) if isinstance(image_urls, dict) else 0
            })
        
        conn.close()
        return results
        
    except Exception as e:
        logger.error(f"分析Pin图片URL失败: {e}")
        return []

if __name__ == "__main__":
    logger.info("=== 数据库状态检查 ===")
    
    # 检查数据库状态
    status = check_database_status()
    if status:
        logger.info(f"总Pin数量: {status['total_pins']}")
        logger.info(f"有图片链接的Pin数量: {status['pins_with_images']}")
        logger.info(f"查询关键词: {status['queries']}")
        
        logger.info("\n=== 样本Pin数据 ===")
        for i, pin in enumerate(status['sample_pins'][:3], 1):
            pin_id, title, largest_url, image_urls, created_at = pin
            logger.info(f"{i}. Pin ID: {pin_id}")
            logger.info(f"   标题: {title[:50] if title else 'N/A'}...")
            logger.info(f"   最大图片URL: {'有' if largest_url else '无'}")
            logger.info(f"   图片URLs: {'有' if image_urls and image_urls != '[]' else '无'}")
            logger.info(f"   创建时间: {created_at}")
    
    # 分析图片URL结构
    logger.info("\n=== 图片URL结构分析 ===")
    url_analysis = analyze_pin_image_urls()
    for i, pin in enumerate(url_analysis, 1):
        logger.info(f"{i}. Pin {pin['id']}")
        logger.info(f"   有largest_image_url: {pin['has_largest']}")
        logger.info(f"   有image_urls: {pin['has_image_urls']}")
        logger.info(f"   image_urls数量: {pin['image_urls_count']}")
        if pin['image_urls']:
            logger.info(f"   image_urls键: {list(pin['image_urls'].keys())}")
