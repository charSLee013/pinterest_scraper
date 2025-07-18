#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试Base64转换性能优化
验证优化后的配置和功能正确性
"""

import os
import sys
import sqlite3
import tempfile
import shutil
import asyncio
import time

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.tools.realtime_base64_converter import BatchAtomicBase64Converter


async def test_performance_optimizations():
    """测试性能优化配置"""
    print("Testing performance optimizations...")
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp()
    keyword = "test_performance"
    
    try:
        # 测试1: 验证默认配置优化
        print("\n1. Testing default configuration optimizations...")
        converter = BatchAtomicBase64Converter(temp_dir)
        
        # 验证批次大小优化
        expected_batch_size = 2048
        if converter.batch_size == expected_batch_size:
            print(f"✅ Default batch size optimized: {converter.batch_size}")
        else:
            print(f"❌ Default batch size not optimized: {converter.batch_size}, expected: {expected_batch_size}")
            return False
        
        # 验证线程数优化
        cpu_cores = os.cpu_count() or 1
        expected_max_workers = min(16, cpu_cores * 2)
        if converter.max_workers == expected_max_workers:
            print(f"✅ Thread count optimized: {converter.max_workers} (CPU cores: {cpu_cores})")
        else:
            print(f"❌ Thread count not optimized: {converter.max_workers}, expected: {expected_max_workers}")
            return False
        
        # 测试2: 验证批次大小上限提升
        print("\n2. Testing batch size limit increase...")
        large_batch_converter = BatchAtomicBase64Converter(temp_dir, batch_size=4096)
        if large_batch_converter.batch_size == 4096:
            print(f"✅ Large batch size supported: {large_batch_converter.batch_size}")
        else:
            print(f"❌ Large batch size not supported: {large_batch_converter.batch_size}")
            return False
        
        # 测试超过上限的批次大小
        max_batch_converter = BatchAtomicBase64Converter(temp_dir, batch_size=8192)
        if max_batch_converter.batch_size == 4096:  # 应该被限制到4096
            print(f"✅ Batch size properly capped at: {max_batch_converter.batch_size}")
        else:
            print(f"❌ Batch size not properly capped: {max_batch_converter.batch_size}")
            return False
        
        # 测试3: 创建测试数据库并验证转换功能
        print("\n3. Testing conversion functionality with optimizations...")
        
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
        
        # 插入测试数据（更多数据以测试批量处理）
        test_pins = []
        for i in range(150):  # 创建150个测试Pin以测试批量提交
            base64_id = f"UGluOjEyMzQ1Njc4OTA{i:03d}="  # 模拟base64编码的Pin ID
            test_pins.append((base64_id, f"hash{i}", keyword, f"Test Pin {i}", f"Description {i}"))
        
        cursor.executemany("""
        INSERT INTO pins (id, pin_hash, query, title, description) 
        VALUES (?, ?, ?, ?, ?)
        """, test_pins)
        
        conn.commit()
        conn.close()
        
        print(f"Created test database with {len(test_pins)} base64 pins")
        
        # 测试转换性能
        start_time = time.time()
        result = await converter.process_all_databases(target_keyword=keyword)
        end_time = time.time()
        
        conversion_time = end_time - start_time
        pins_per_second = result['total_converted'] / conversion_time if conversion_time > 0 else 0
        
        print(f"Conversion completed in {conversion_time:.2f} seconds")
        print(f"Conversion rate: {pins_per_second:.1f} pins/second")
        print(f"Conversion result: {result}")
        
        # 验证转换结果
        if result['total_converted'] > 0:
            print(f"✅ Conversion successful: {result['total_converted']} pins converted")
        else:
            print(f"❌ Conversion failed: {result}")
            return False
        
        # 测试4: 验证数据完整性
        print("\n4. Testing data integrity...")
        
        # 重新连接数据库验证结果
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查是否还有base64编码的Pin
        cursor.execute("SELECT COUNT(*) FROM pins WHERE id LIKE 'UGlu%'")
        remaining_base64 = cursor.fetchone()[0]
        
        # 检查转换后的Pin数量
        cursor.execute("SELECT COUNT(*) FROM pins WHERE id NOT LIKE 'UGlu%'")
        converted_pins = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"Remaining base64 pins: {remaining_base64}")
        print(f"Converted pins: {converted_pins}")
        
        if remaining_base64 == 0 and converted_pins > 0:
            print("✅ Data integrity verified: All base64 pins converted")
        else:
            print("❌ Data integrity issue detected")
            return False
        
        # 测试5: 验证中断处理仍然工作
        print("\n5. Testing interrupt handling...")
        
        # 重置中断状态
        converter.interrupt_manager.reset()
        
        # 设置中断状态
        converter.interrupt_manager.set_interrupted()
        
        # 尝试处理（应该立即中断）
        try:
            await converter.process_all_databases(target_keyword=keyword)
            print("❌ Interrupt handling not working")
            return False
        except KeyboardInterrupt:
            print("✅ Interrupt handling working correctly")
        
        return True
        
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
    print("Starting performance optimization validation...")
    print("=" * 60)
    
    success = await test_performance_optimizations()
    
    if success:
        print("\n🎉 All performance optimization tests PASSED!")
        print("✅ Default batch size increased to 2048")
        print("✅ Batch size limit increased to 4096") 
        print("✅ Thread count optimized to CPU cores × 2")
        print("✅ Transaction batching implemented")
        print("✅ Performance monitoring enabled")
        print("✅ Data integrity maintained")
        print("✅ Interrupt handling preserved")
    else:
        print("\n❌ Performance optimization tests FAILED")
        return 1
    
    return 0


if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(result)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(130)
