#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查数据库中是否还有base64编码的Pin
"""

import sqlite3
import base64
import os
from loguru import logger


def decode_base64_pin_id(encoded_pin_id):
    """解码base64编码的Pin ID"""
    try:
        if encoded_pin_id.startswith('UGlu'):
            decoded = base64.b64decode(encoded_pin_id).decode('utf-8')
            if decoded.startswith('Pin:'):
                return decoded[4:]
        return None
    except Exception as e:
        return None


def check_database_base64_pins(keyword):
    """检查指定数据库中的base64编码Pin"""
    try:
        db_path = f'output/{keyword}/pinterest.db'
        
        if not os.path.exists(db_path):
            logger.warning(f"数据库不存在: {db_path}")
            return
        
        conn = sqlite3.connect(db_path, timeout=10.0)
        cursor = conn.cursor()
        
        # 查询base64编码的Pin
        cursor.execute("SELECT COUNT(*) FROM pins WHERE id LIKE 'UGlu%'")
        base64_count = cursor.fetchone()[0]
        
        # 查询总Pin数
        cursor.execute('SELECT COUNT(*) FROM pins')
        total_count = cursor.fetchone()[0]
        
        logger.info(f"{keyword}: 总Pin={total_count:,}, base64编码Pin={base64_count:,}")
        
        if base64_count > 0:
            # 显示一些base64编码Pin的例子
            cursor.execute("SELECT id, title FROM pins WHERE id LIKE 'UGlu%' LIMIT 10")
            examples = cursor.fetchall()
            
            logger.warning(f"发现 {base64_count:,} 个base64编码Pin:")
            for pin_id, title in examples:
                decoded_id = decode_base64_pin_id(pin_id)
                logger.info(f"  {pin_id} -> {decoded_id} | {title[:50]}...")
        
        conn.close()
        return base64_count
        
    except Exception as e:
        logger.error(f"{keyword}: 检查失败 - {e}")
        return 0


def check_all_databases():
    """检查所有数据库"""
    logger.info("🔍 检查所有数据库中的base64编码Pin")
    
    keywords = ['building', 'interior design', 'room', 'sofa']
    total_base64_pins = 0
    
    for keyword in keywords:
        count = check_database_base64_pins(keyword)
        total_base64_pins += count
    
    logger.info(f"📊 总计发现 {total_base64_pins:,} 个base64编码Pin")
    
    if total_base64_pins > 0:
        logger.warning("⚠️ 仍有base64编码Pin未转换！")
        return False
    else:
        logger.info("✅ 所有Pin都已正确转换")
        return True


def check_repaired_databases():
    """检查修复的数据库文件"""
    logger.info("🔍 检查修复的数据库文件")
    
    keywords = ['building', 'interior design', 'room', 'sofa']
    
    for keyword in keywords:
        repaired_db_path = f'output/{keyword}/pinterest.db.repaired_ready'
        
        if os.path.exists(repaired_db_path):
            logger.info(f"📁 发现修复的数据库: {repaired_db_path}")
            
            try:
                conn = sqlite3.connect(repaired_db_path, timeout=10.0)
                cursor = conn.cursor()
                
                # 查询base64编码的Pin
                cursor.execute("SELECT COUNT(*) FROM pins WHERE id LIKE 'UGlu%'")
                base64_count = cursor.fetchone()[0]
                
                # 查询总Pin数
                cursor.execute('SELECT COUNT(*) FROM pins')
                total_count = cursor.fetchone()[0]
                
                logger.info(f"  {keyword} (修复版): 总Pin={total_count:,}, base64编码Pin={base64_count:,}")
                
                conn.close()
                
            except Exception as e:
                logger.error(f"  检查修复数据库失败: {e}")


def main():
    """主函数"""
    logger.info("🚀 开始检查base64编码Pin状态")
    
    # 检查当前数据库
    all_converted = check_all_databases()
    
    # 检查修复的数据库
    check_repaired_databases()
    
    if not all_converted:
        print("\n" + "="*60)
        print("⚠️ 发现未转换的base64编码Pin")
        print("可能的原因:")
        print("1. 转换过程中出现错误")
        print("2. 数据库文件替换失败")
        print("3. 新数据在转换后被添加")
        print("="*60)
        return 1
    else:
        print("\n" + "="*60)
        print("✅ 所有Pin都已正确转换")
        print("="*60)
        return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
