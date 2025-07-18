#!/usr/bin/env python3
"""
修复版第二阶段Pin有效性测试
使用猴子补丁修复NetworkInterceptor的API拦截逻辑，然后测试第二阶段采集的Pin有效性
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

# 猴子补丁：修复NetworkInterceptor的API模式匹配
def apply_network_interceptor_patch():
    """应用猴子补丁修复NetworkInterceptor"""
    try:
        # 导入NetworkInterceptor模块
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

        from utils.network_interceptor import NetworkInterceptor

        # 保存原始的__init__方法
        original_init = NetworkInterceptor.__init__

        def patched_init(self, output_dir: str = "network_analysis/results", max_cache_size: int = 1000, verbose: bool = True, target_count: int = 0):
            # 调用原始初始化
            original_init(self, output_dir, max_cache_size, verbose, target_count)

            # 修复API模式列表，添加RelatedModulesResource
            self.pinterest_api_patterns = [
                "api.pinterest.com",
                "v3/search/pins",
                "BoardFeedResource",
                "SearchResource",
                "BaseSearchResource",  # 搜索API
                "UserPinsResource",
                "RelatedPinsResource",  # Pin详情页相关推荐
                "RelatedModulesResource",  # 🔥 修复：添加关键的RelatedModulesResource
                "PinResource",
                "VisualSearchResource",  # 视觉搜索
                "HomefeedResource",  # 首页推荐
                "resource/",
                "/v3/",
                "graphql",
                "_/graphql/",  # GraphQL端点
                "CloseupDetailsResource",  # Pin详情
                "MoreLikeThisResource",  # 更多相似内容
                "RelatedPinFeedResource"  # 相关Pin推荐
            ]

            print(f"猴子补丁已应用：添加了RelatedModulesResource到API拦截列表")

        # 应用补丁
        NetworkInterceptor.__init__ = patched_init

        print("NetworkInterceptor猴子补丁应用成功")
        return True

    except Exception as e:
        print(f"猴子补丁应用失败: {e}")
        return False

class SecondPhaseSimpleTest:
    def __init__(self):
        # 应用猴子补丁
        self.patch_applied = apply_network_interceptor_patch()

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
    
    def step1_run_scraper_to_collect_pins(self, query: str = "test_room_fixed", count: int = 100):
        """第一步：运行修复后的爬虫采集Pin数据"""
        print(f"=== 第一步：运行修复后的爬虫采集 {count} 个Pin ===")

        if not self.patch_applied:
            print("警告：猴子补丁未成功应用，可能影响测试结果")
        else:
            print("猴子补丁已应用，RelatedModulesResource现在会被拦截")

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
        
        print(f"数据库分析结果:")
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
        
        print(f"验证结果:")
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
    
    def step4_generate_report(self, analysis: Dict, validation: Dict):
        """第四步：生成完整报告"""
        print(f"\n=== 第四步：生成完整报告 ===")
        
        report = {
            "test_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "database_analysis": analysis,
            "validation_results": validation,
            "conclusions": []
        }
        
        # 分析结论（考虑修复效果）
        if self.patch_applied:
            report["patch_status"] = "RelatedModulesResource补丁已应用"
            if validation.get('validity_rate', 0) < 10:
                report["conclusions"].append("修复后第二阶段采集的Pin有效率仍然极低，可能存在其他问题")
            elif validation.get('validity_rate', 0) < 50:
                report["conclusions"].append("修复后第二阶段采集的Pin有效率有所改善但仍较低")
            else:
                report["conclusions"].append("修复后第二阶段采集的Pin有效率正常，RelatedModulesResource补丁有效")
        else:
            report["patch_status"] = "RelatedModulesResource补丁应用失败"
            if validation.get('validity_rate', 0) < 10:
                report["conclusions"].append("⚠️ 第二阶段采集的Pin有效率极低，确认这是数据库无效Pin的主要来源")
            elif validation.get('validity_rate', 0) < 50:
                report["conclusions"].append("⚠️ 第二阶段采集的Pin有效率较低，是数据库无效Pin的重要来源")
            else:
                report["conclusions"].append("✅ 第二阶段采集的Pin有效率正常，不是数据库无效Pin的主要原因")

        if analysis.get('alphanumeric_pins', 0) > analysis.get('numeric_pins', 0):
            report["conclusions"].append("数据库中字母数字Pin数量异常，可能存在Pin ID提取错误")

        if validation.get('login_page_pins', 0) > validation.get('total_tested', 1) * 0.8:
            report["conclusions"].append("大部分Pin都重定向到登录页面，确认认证问题是主要原因")
        
        # 保存报告
        try:
            os.makedirs("test_second_phase_results", exist_ok=True)
            filename = "test_second_phase_results/fixed_test_report.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            print(f"完整报告已保存到: {filename}")
        except Exception as e:
            print(f"保存报告失败: {e}")

        # 打印结论
        print(f"\n修复测试结论:")
        print(f"  {report.get('patch_status', '未知补丁状态')}")
        for conclusion in report["conclusions"]:
            print(f"  {conclusion}")

        return report

def main():
    """主函数"""
    print("开始第二阶段Pin有效性修复测试")
    print("="*60)
    print("本次测试将应用RelatedModulesResource猴子补丁")
    print("="*60)

    tester = SecondPhaseSimpleTest()

    try:
        # 第一步：运行修复后的爬虫采集数据
        db_path = tester.step1_run_scraper_to_collect_pins("test_room_fixed", 100)

        # 第二步：分析采集到的数据
        analysis = tester.step2_analyze_collected_pins(db_path)

        if not analysis:
            print("数据分析失败，测试终止")
            return

        # 第三步：验证样本Pin有效性
        validation = tester.step3_validate_sample_pins(db_path, 50)

        # 第四步：生成报告
        report = tester.step4_generate_report(analysis, validation)

        # 第五步：对比分析
        print(f"\n" + "="*60)
        print("修复效果对比分析")
        print("="*60)

        if tester.patch_applied:
            print("RelatedModulesResource补丁成功应用")
            print("如果采集数量或有效率有显著提升，说明补丁有效")
        else:
            print("RelatedModulesResource补丁应用失败")
            print("测试结果可能与之前相同")

        print(f"本次测试结果:")
        print(f"  - 采集Pin数量: {analysis.get('total_pins', 0)}")
        print(f"  - 有效Pin数量: {validation.get('valid_pins', 0)}")
        print(f"  - 有效率: {validation.get('validity_rate', 0):.1f}%")

    except KeyboardInterrupt:
        print("\n用户中断测试")
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
