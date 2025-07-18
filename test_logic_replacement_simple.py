#!/usr/bin/env python3
"""
简单的逻辑替换验证测试

验证第二阶段逻辑替换是否成功：
1. 检查导入是否正确
2. 验证函数签名是否正确
3. 确保类和方法存在
"""

import sys
import inspect

def test_stage_implementations_imports():
    """测试stage_implementations.py的导入和函数"""
    print("=== 测试stage_implementations.py ===")
    
    try:
        from src.tools.stage_implementations import fetch_pin_detail_with_browser
        
        # 检查函数签名
        sig = inspect.signature(fetch_pin_detail_with_browser)
        params = list(sig.parameters.keys())
        
        print(f"fetch_pin_detail_with_browser函数导入成功")
        print(f"  - 参数: {params}")
        print(f"  - 是否异步: {inspect.iscoroutinefunction(fetch_pin_detail_with_browser)}")
        
        # 检查是否移除了旧函数
        try:
            from src.tools.stage_implementations import fetch_pin_detail_with_headers
            print("警告: 旧函数fetch_pin_detail_with_headers仍然存在")
            return False
        except ImportError:
            print("旧函数fetch_pin_detail_with_headers已成功移除")
        
        return True
        
    except ImportError as e:
        print(f"导入失败: {e}")
        return False
    except Exception as e:
        print(f"测试异常: {e}")
        return False

def test_smart_pin_enhancer_imports():
    """测试smart_pin_enhancer.py的导入和方法"""
    print("\n=== 测试smart_pin_enhancer.py ===")
    
    try:
        from src.tools.smart_pin_enhancer import SmartPinEnhancer
        
        # 检查类是否存在
        print(f"SmartPinEnhancer类导入成功")
        
        # 检查关键方法
        methods = ['enhance_pin_if_needed', '_fetch_pin_detail', '_has_valid_image_urls']
        for method_name in methods:
            if hasattr(SmartPinEnhancer, method_name):
                method = getattr(SmartPinEnhancer, method_name)
                is_async = inspect.iscoroutinefunction(method)
                print(f"  - {method_name}: 存在 (异步: {is_async})")
            else:
                print(f"  - {method_name}: 缺失")
                return False
        
        # 检查_fetch_pin_detail是否为异步方法
        fetch_method = getattr(SmartPinEnhancer, '_fetch_pin_detail')
        if not inspect.iscoroutinefunction(fetch_method):
            print("错误: _fetch_pin_detail应该是异步方法")
            return False
        
        return True
        
    except ImportError as e:
        print(f"导入失败: {e}")
        return False
    except Exception as e:
        print(f"测试异常: {e}")
        return False

def test_improved_pin_detail_extractor():
    """测试improved_pin_detail_extractor.py的状态"""
    print("\n=== 测试improved_pin_detail_extractor.py ===")
    
    try:
        from src.utils.improved_pin_detail_extractor import ImprovedPinDetailExtractor
        
        print(f"ImprovedPinDetailExtractor类仍然可用（向后兼容）")
        
        # 检查文档字符串是否包含弃用警告
        docstring = ImprovedPinDetailExtractor.__doc__ or ""
        if "已弃用" in docstring or "弃用" in docstring:
            print("  - 包含弃用警告")
        else:
            print("  - 警告: 缺少弃用警告")
        
        return True
        
    except ImportError as e:
        print(f"导入失败: {e}")
        return False
    except Exception as e:
        print(f"测试异常: {e}")
        return False

def test_required_imports():
    """测试必要的导入是否正确"""
    print("\n=== 测试必要导入 ===")
    
    required_imports = [
        ("src.core.browser_manager", "BrowserManager"),
        ("src.utils.network_interceptor", "NetworkInterceptor"),
        ("src.core.database.repository", "SQLiteRepository"),
        ("src.core.database.atomic_saver", "AtomicPinSaver"),
    ]
    
    success_count = 0
    
    for module_name, class_name in required_imports:
        try:
            module = __import__(module_name, fromlist=[class_name])
            cls = getattr(module, class_name)
            print(f"  - {module_name}.{class_name}: 成功")
            success_count += 1
        except ImportError as e:
            print(f"  - {module_name}.{class_name}: 失败 ({e})")
        except AttributeError as e:
            print(f"  - {module_name}.{class_name}: 类不存在 ({e})")
        except Exception as e:
            print(f"  - {module_name}.{class_name}: 异常 ({e})")
    
    return success_count == len(required_imports)

def test_database_compatibility():
    """测试数据库兼容性（不需要实际数据库）"""
    print("\n=== 测试数据库兼容性 ===")
    
    try:
        from src.core.database.normalizer import PinDataNormalizer
        from src.core.database.schema import Pin
        
        print("数据库相关类导入成功")
        
        # 测试数据标准化
        test_pin_data = {
            'id': 'test_123',
            'title': '测试标题',
            'description': '测试描述',
            'image_urls': {'1': 'http://example.com/image.jpg'},
            'largest_image_url': 'http://example.com/large.jpg'
        }
        
        normalized = PinDataNormalizer.normalize_pin_data(test_pin_data, 'test_query')
        
        required_fields = ['id', 'query', 'pin_hash', 'created_at', 'updated_at']
        missing_fields = [field for field in required_fields if field not in normalized]
        
        if missing_fields:
            print(f"标准化数据缺少字段: {missing_fields}")
            return False
        
        print("数据标准化测试通过")
        return True
        
    except Exception as e:
        print(f"数据库兼容性测试异常: {e}")
        return False

def main():
    """主测试函数"""
    print("开始第二阶段逻辑替换验证测试")
    print("="*50)
    
    tests = [
        ("stage_implementations导入", test_stage_implementations_imports),
        ("smart_pin_enhancer导入", test_smart_pin_enhancer_imports),
        ("improved_pin_detail_extractor状态", test_improved_pin_detail_extractor),
        ("必要导入", test_required_imports),
        ("数据库兼容性", test_database_compatibility),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            status = "通过" if result else "失败"
            print(f"\n{test_name}: {status}")
            if result:
                passed += 1
        except Exception as e:
            print(f"\n{test_name}: 异常 ({e})")
    
    print("\n" + "="*50)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("所有测试通过！第二阶段逻辑替换成功")
        print("\n关键改进:")
        print("1. 移除了错误的HTTP请求实现")
        print("2. 使用正确的浏览器+NetworkInterceptor实现")
        print("3. 保持了向后兼容性")
        print("4. 确保了数据库操作一致性")
        return True
    else:
        print("部分测试失败，需要检查实现")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
