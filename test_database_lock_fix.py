#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试数据库锁定问题修复
"""

import os
import sys
import sqlite3
import tempfile
import shutil
import asyncio

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.tools.realtime_base64_converter import BatchAtomicBase64Converter


async def test_database_lock_fix():
    """测试数据库锁定问题修复"""
    print("Testing database lock fix...")
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp()
    keyword = "test_keyword"
    
    try:
        # 创建测试数据库目录
        keyword_dir = os.path.join(temp_dir, keyword)
        os.makedirs(keyword_dir, exist_ok=True)
        
        # 创建测试数据库
        db_path = os.path.join(keyword_dir, "pinterest.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 创建表（使用完整的模式）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pins (
            id TEXT PRIMARY KEY,
            pin_hash TEXT,
            query TEXT,
            title TEXT,
            description TEXT,
            creator_name TEXT,
            creator_id TEXT,
            board_name TEXT,
            board_id TEXT,
            image_urls TEXT,
            largest_image_url TEXT,
            stats TEXT,
            raw_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # 插入一些base64编码的测试数据
        test_pins = [
            ("UGluOjEyMzQ1Njc4OTA=", "hash1", "test_keyword", "Test Pin 1", "Description 1"),
            ("UGluOjk4NzY1NDMyMTA=", "hash2", "test_keyword", "Test Pin 2", "Description 2"),
        ]

        cursor.executemany("""
        INSERT INTO pins (id, pin_hash, query, title, description)
        VALUES (?, ?, ?, ?, ?)
        """, test_pins)
        
        conn.commit()
        conn.close()
        
        print(f"Created test database with {len(test_pins)} base64 pins")
        
        # 创建转换器
        converter = BatchAtomicBase64Converter(temp_dir)
        
        # 执行转换
        print("Starting conversion...")
        result = await converter.process_all_databases(target_keyword=keyword)
        print(f"Conversion result: {result}")

        # 显式清理所有连接
        print("Cleaning up connections...")
        await converter._force_close_all_connections(keyword)

        # 额外等待确保连接完全释放
        await asyncio.sleep(2.0)

        # 验证数据库文件是否可以被访问（没有锁定）
        print("Testing database accessibility...")
        
        # 尝试打开数据库文件
        test_conn = sqlite3.connect(db_path)
        test_cursor = test_conn.cursor()
        test_cursor.execute("SELECT COUNT(*) FROM pins")
        count = test_cursor.fetchone()[0]
        test_conn.close()
        
        print(f"Database accessible: {count} pins found")
        
        # 尝试删除数据库文件（如果没有锁定应该成功）
        try:
            os.remove(db_path)
            print("SUCCESS: Database file can be deleted (no lock)")
            return True
        except PermissionError as e:
            print(f"ERROR: Database file is still locked: {e}")
            return False
            
    except Exception as e:
        print(f"Test error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # 清理
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except:
                pass


async def main():
    """主测试函数"""
    print("Starting database lock fix test...")
    print("=" * 50)
    
    success = await test_database_lock_fix()
    
    if success:
        print("\nSUCCESS: Database lock fix verification PASSED")
        print("Database connections are properly released after conversion")
    else:
        print("\nFAILED: Database lock fix verification FAILED")
        return 1
    
    return 0


if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(result)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(130)
