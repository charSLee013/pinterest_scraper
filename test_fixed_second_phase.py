#!/usr/bin/env python3
"""
修复后的第二阶段Pin有效性测试
直接修改了NetworkInterceptor源码，添加了RelatedModulesResource支持
"""

import subprocess
import json
import sqlite3
import requests
import time
import random
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os

class FixedSecondPhaseTest:
    def __init__(self):
        self.headers = self.load_headers()
        self.lock = threading.Lock()
        self.completed = 0
        
    def load_headers(self) -> Dict[str, str]:
        """加载headers"""
        try:
            with open('browser_session.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'headers' in data:
                    return data['headers']
        except:
            pass
        
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def step1_run_fixed_scraper(self, query: str = "test_fixed", count: int = 100):
        """第一步：运行修复后的爬虫"""
        print(f"=== 第一步：运行修复后的爬虫采集 {count} 个Pin ===")
        print("已直接修改NetworkInterceptor源码，添加RelatedModulesResource支持")
        
        # 清理之前的数据库
        db_path = f"output/{query}/pinterest.db"
        if os.path.exists(db_path):
            os.remove(db_path)
            print(f"已清理旧数据库: {db_path}")
        
        # 运行爬虫
        cmd = f"uv run python main.py -q {query} -c {count} --no-images"
        print(f"执行命令: {cmd}")
        
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
            print(f"爬虫执行完成，返回码: {result.returncode}")
            
            if result.stdout:
                print("标准输出:")
                print(result.stdout[-1000:])  # 显示最后1000字符
                
            if result.stderr:
                print("错误输出:")
                print(result.stderr[-1000:])  # 显示最后1000字符
                
        except subprocess.TimeoutExpired:
            print("爬虫执行超时")
        except Exception as e:
            print(f"爬虫执行异常: {e}")
        
        return db_path
    
    def step2_analyze_collected_pins(self, db_path: str) -> Dict:
        """第二步：分析采集到的Pin数据"""
        print(f"=== 第二步：分析数据库中的Pin数据 ===")
        
        if not os.path.exists(db_path):
            print(f"数据库文件不存在: {db_path}")
            return {}
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 获取所有Pin
        cursor.execute("SELECT id, title, description, image_urls FROM pins")
        all_pins = cursor.fetchall()
        
        # 分析Pin格式
        numeric_pins = []
        alphanumeric_pins = []
        no_image_pins = []
        
        for pin_id, title, desc, image_urls in all_pins:
            if pin_id.isdigit():
                numeric_pins.append(pin_id)
            else:
                alphanumeric_pins.append(pin_id)
                
            if not image_urls or image_urls == '' or image_urls == '[]':
                no_image_pins.append(pin_id)
        
        conn.close()
        
        analysis = {
            'total_pins': len(all_pins),
            'numeric_pins': len(numeric_pins),
            'alphanumeric_pins': len(alphanumeric_pins),
            'no_image_pins': len(no_image_pins),
            'numeric_examples': numeric_pins[:10],
            'alphanumeric_examples': alphanumeric_pins[:10],
            'no_image_examples': no_image_pins[:10]
        }
        
        print(f"修复后数据库分析结果:")
        print(f"  总Pin数: {analysis['total_pins']}")
        print(f"  纯数字Pin: {analysis['numeric_pins']} ({analysis['numeric_pins']/analysis['total_pins']*100:.1f}%)")
        print(f"  字母数字Pin: {analysis['alphanumeric_pins']} ({analysis['alphanumeric_pins']/analysis['total_pins']*100:.1f}%)")
        print(f"  无图片Pin: {analysis['no_image_pins']} ({analysis['no_image_pins']/analysis['total_pins']*100:.1f}%)")
        
        return analysis
    
    def step3_validate_sample_pins(self, db_path: str, sample_size: int = 50) -> Dict:
        """第三步：验证样本Pin的有效性"""
        print(f"=== 第三步：验证 {sample_size} 个样本Pin的有效性 ===")
        
        if not os.path.exists(db_path):
            print(f"数据库文件不存在: {db_path}")
            return {}
        
        # 从数据库随机获取样本Pin
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM pins ORDER BY RANDOM() LIMIT ?", (sample_size,))
        sample_pins = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print(f"获取到 {len(sample_pins)} 个样本Pin")
        
        # 多线程验证
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_pin = {
                executor.submit(self.validate_single_pin, pin_id): pin_id 
                for pin_id in sample_pins
            }
            
            for future in as_completed(future_to_pin):
                result = future.result()
                results.append(result)
                self.update_progress(len(sample_pins))
        
        print()  # 换行
        
        # 分析结果
        valid_pins = [r for r in results if r['is_valid']]
        invalid_pins = [r for r in results if not r['is_valid']]
        
        numeric_pins = [r for r in results if r['is_numeric']]
        alphanumeric_pins = [r for r in results if not r['is_numeric']]
        
        numeric_valid = [r for r in valid_pins if r['is_numeric']]
        alphanumeric_valid = [r for r in valid_pins if not r['is_numeric']]
        
        login_page_pins = [r for r in results if r['has_login_page']]
        
        validation_summary = {
            'total_tested': len(results),
            'valid_pins': len(valid_pins),
            'invalid_pins': len(invalid_pins),
            'validity_rate': len(valid_pins) / len(results) * 100 if results else 0,
            'numeric_pins': len(numeric_pins),
            'alphanumeric_pins': len(alphanumeric_pins),
            'numeric_valid': len(numeric_valid),
            'alphanumeric_valid': len(alphanumeric_valid),
            'login_page_pins': len(login_page_pins),
            'valid_examples': [r['pin_id'] for r in valid_pins[:5]],
            'invalid_examples': [r['pin_id'] for r in invalid_pins[:10]]
        }
        
        print(f"修复后验证结果:")
        print(f"  测试Pin数: {validation_summary['total_tested']}")
        print(f"  有效Pin: {validation_summary['valid_pins']}")
        print(f"  无效Pin: {validation_summary['invalid_pins']}")
        print(f"  有效率: {validation_summary['validity_rate']:.1f}%")
        print(f"  纯数字Pin: {validation_summary['numeric_pins']} (有效: {validation_summary['numeric_valid']})")
        print(f"  字母数字Pin: {validation_summary['alphanumeric_pins']} (有效: {validation_summary['alphanumeric_valid']})")
        print(f"  登录页面重定向: {validation_summary['login_page_pins']}")
        
        return validation_summary
    
    def validate_single_pin(self, pin_id: str) -> Dict:
        """验证单个Pin"""
        result = {
            'pin_id': pin_id,
            'is_numeric': pin_id.isdigit(),
            'is_valid': False,
            'has_login_page': True,
            'has_pin_content': False,
            'has_image_urls': False,
            'error': None
        }
        
        try:
            session = requests.Session()
            session.headers.update(self.headers)
            
            pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
            response = session.get(pin_url, timeout=30)
            
            if response.status_code == 200:
                html_lower = response.text.lower()
                
                result['has_login_page'] = 'log in' in html_lower and 'sign up' in html_lower
                result['has_pin_content'] = f'"{pin_id}"' in response.text or f"'{pin_id}'" in response.text
                result['has_image_urls'] = 'pinimg.com' in response.text and ('.jpg' in response.text or '.png' in response.text)
                
                result['is_valid'] = not result['has_login_page'] and result['has_image_urls']
            else:
                result['error'] = f"HTTP {response.status_code}"
                
        except Exception as e:
            result['error'] = str(e)
        finally:
            if 'session' in locals():
                session.close()
        
        return result
    
    def update_progress(self, total: int):
        """更新进度"""
        with self.lock:
            self.completed += 1
            progress = (self.completed / total) * 100
            print(f"\r验证进度: {self.completed}/{total} ({progress:.1f}%)", end="", flush=True)
    
    def step4_compare_results(self, analysis: Dict, validation: Dict):
        """第四步：对比修复前后的结果"""
        print(f"\n=== 第四步：修复效果对比分析 ===")
        
        # 读取之前的测试结果
        try:
            with open("test_second_phase_results/simple_test_report.json", 'r', encoding='utf-8') as f:
                old_report = json.load(f)
                old_analysis = old_report.get("database_analysis", {})
                old_validation = old_report.get("validation_results", {})
        except:
            print("无法读取之前的测试结果，跳过对比")
            old_analysis = {}
            old_validation = {}
        
        print(f"修复效果对比:")
        print(f"  采集Pin数量:")
        print(f"    修复前: {old_analysis.get('total_pins', 0)}")
        print(f"    修复后: {analysis.get('total_pins', 0)}")
        print(f"    变化: {analysis.get('total_pins', 0) - old_analysis.get('total_pins', 0):+d}")
        
        print(f"  有效Pin数量:")
        print(f"    修复前: {old_validation.get('valid_pins', 0)}")
        print(f"    修复后: {validation.get('valid_pins', 0)}")
        print(f"    变化: {validation.get('valid_pins', 0) - old_validation.get('valid_pins', 0):+d}")
        
        print(f"  有效率:")
        print(f"    修复前: {old_validation.get('validity_rate', 0):.1f}%")
        print(f"    修复后: {validation.get('validity_rate', 0):.1f}%")
        print(f"    变化: {validation.get('validity_rate', 0) - old_validation.get('validity_rate', 0):+.1f}%")
        
        # 保存修复后的结果
        fixed_report = {
            "test_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "fix_applied": "RelatedModulesResource已添加到NetworkInterceptor",
            "database_analysis": analysis,
            "validation_results": validation,
            "comparison": {
                "pin_count_change": analysis.get('total_pins', 0) - old_analysis.get('total_pins', 0),
                "valid_count_change": validation.get('valid_pins', 0) - old_validation.get('valid_pins', 0),
                "validity_rate_change": validation.get('validity_rate', 0) - old_validation.get('validity_rate', 0)
            }
        }
        
        try:
            os.makedirs("test_second_phase_results", exist_ok=True)
            with open("test_second_phase_results/fixed_test_report.json", 'w', encoding='utf-8') as f:
                json.dump(fixed_report, f, indent=2, ensure_ascii=False)
            print(f"\n修复后测试报告已保存到: test_second_phase_results/fixed_test_report.json")
        except Exception as e:
            print(f"保存报告失败: {e}")
        
        # 结论
        print(f"\n修复效果结论:")
        if validation.get('validity_rate', 0) > old_validation.get('validity_rate', 0):
            print("RelatedModulesResource修复有效，Pin有效率有所提升")
        elif analysis.get('total_pins', 0) > old_analysis.get('total_pins', 0):
            print("RelatedModulesResource修复部分有效，采集数量有所提升")
        else:
            print("RelatedModulesResource修复效果不明显，可能存在其他问题")

def main():
    """主函数"""
    print("开始修复后的第二阶段Pin有效性测试")
    print("="*60)
    print("已直接修改NetworkInterceptor源码添加RelatedModulesResource")
    print("="*60)
    
    tester = FixedSecondPhaseTest()
    
    try:
        # 第一步：运行修复后的爬虫
        db_path = tester.step1_run_fixed_scraper("test_fixed", 100)
        
        # 第二步：分析采集到的数据
        analysis = tester.step2_analyze_collected_pins(db_path)
        
        if not analysis:
            print("数据分析失败，测试终止")
            return
        
        # 第三步：验证样本Pin有效性
        validation = tester.step3_validate_sample_pins(db_path, 50)
        
        # 第四步：对比修复效果
        tester.step4_compare_results(analysis, validation)
        
    except KeyboardInterrupt:
        print("\n用户中断测试")
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
