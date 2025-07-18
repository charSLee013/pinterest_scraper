#!/usr/bin/env python3
"""
多线程测试1000个无图片PIN的有效性
"""

import sqlite3
import json
import requests
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
from dataclasses import dataclass
import random
import re

@dataclass
class PinTestResult:
    pin_id: str
    is_numeric: bool
    status_code: int = 0
    has_login_page: bool = True
    has_pin_content: bool = False
    has_image_urls: bool = False
    image_url_count: int = 0
    response_size: int = 0
    error: str = None
    is_valid: bool = False

class PinTester:
    def __init__(self, max_workers: int = 15):
        self.max_workers = max_workers
        self.headers = self.load_headers()
        self.results = []
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
    
    def get_random_no_image_pins(self, db_path: str, limit: int = 1000) -> List[str]:
        """随机获取无图片链接的PIN ID"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 获取所有无图片PIN
        query = """
        SELECT id FROM pins 
        WHERE (image_urls IS NULL OR image_urls = '' OR image_urls = '[]')
        ORDER BY RANDOM()
        LIMIT ?
        """
        
        cursor.execute(query, (limit,))
        pin_ids = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return pin_ids
    
    def analyze_html_content(self, html: str, pin_id: str) -> Tuple[bool, bool, int]:
        """分析HTML内容"""
        html_lower = html.lower()
        
        # 检查是否是登录页面
        has_login = 'log in' in html_lower and 'sign up' in html_lower
        
        # 检查是否包含PIN数据
        has_pin_content = f'"{pin_id}"' in html or f"'{pin_id}'" in html
        
        # 检查图片URL
        image_url_patterns = [
            r'https://i\.pinimg\.com/[^"\']*\.jpg',
            r'https://i\.pinimg\.com/[^"\']*\.png',
            r'https://i\.pinimg\.com/[^"\']*\.webp',
            r'https://s\.pinimg\.com/[^"\']*\.jpg',
            r'https://s\.pinimg\.com/[^"\']*\.png'
        ]
        
        image_urls = set()
        for pattern in image_url_patterns:
            matches = re.findall(pattern, html)
            image_urls.update(matches)
        
        # 过滤掉图标和小图片
        valid_image_urls = []
        for url in image_urls:
            if not any(skip in url for skip in ['favicon', 'logo', 'icon', 'avatar', 'profile']):
                if any(size in url for size in ['736x', '564x', '474x', '236x', 'originals']):
                    valid_image_urls.append(url)
        
        has_images = len(valid_image_urls) > 0
        
        return has_pin_content, has_images, len(valid_image_urls)
    
    def test_single_pin(self, pin_id: str) -> PinTestResult:
        """测试单个PIN"""
        result = PinTestResult(
            pin_id=pin_id,
            is_numeric=pin_id.isdigit()
        )
        
        try:
            session = requests.Session()
            session.headers.update(self.headers)
            
            pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
            response = session.get(pin_url, timeout=30)
            
            result.status_code = response.status_code
            result.response_size = len(response.text)
            
            if response.status_code == 200:
                # 分析HTML内容
                has_pin_content, has_images, image_count = self.analyze_html_content(response.text, pin_id)
                
                result.has_login_page = 'log in' in response.text.lower() and 'sign up' in response.text.lower()
                result.has_pin_content = has_pin_content
                result.has_image_urls = has_images
                result.image_url_count = image_count
                
                # 判断是否有效：不是登录页面且包含图片
                result.is_valid = not result.has_login_page and result.has_image_urls
                
                # 保存有效PIN的HTML
                if result.is_valid:
                    self.save_html(pin_id, response.text, "valid")
                elif result.has_pin_content and not result.has_login_page:
                    self.save_html(pin_id, response.text, "no_images")
            
        except Exception as e:
            result.error = str(e)
        finally:
            if 'session' in locals():
                session.close()
        
        return result
    
    def save_html(self, pin_id: str, html: str, category: str):
        """保存HTML文件"""
        try:
            debug_dir = f"debug_html_1000_{category}"
            os.makedirs(debug_dir, exist_ok=True)
            
            html_file = os.path.join(debug_dir, f"pin_{pin_id}.html")
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html)
        except Exception as e:
            print(f"保存HTML失败 {pin_id}: {e}")
    
    def update_progress(self, total: int):
        """更新进度"""
        with self.lock:
            self.completed += 1
            progress = (self.completed / total) * 100
            print(f"\r进度: {self.completed}/{total} ({progress:.1f}%)", end="", flush=True)
    
    def test_pins_multithread(self, pin_ids: List[str]) -> List[PinTestResult]:
        """多线程测试PIN"""
        print(f"开始多线程测试 {len(pin_ids)} 个PIN (并发数: {self.max_workers})")
        print("=" * 60)
        
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_pin = {
                executor.submit(self.test_single_pin, pin_id): pin_id 
                for pin_id in pin_ids
            }
            
            # 收集结果
            for future in as_completed(future_to_pin):
                result = future.result()
                results.append(result)
                self.update_progress(len(pin_ids))
        
        print()  # 换行
        return results

def analyze_results(results: List[PinTestResult]):
    """分析测试结果"""
    print("\n" + "=" * 60)
    print("测试结果分析")
    print("=" * 60)
    
    total = len(results)
    valid_pins = [r for r in results if r.is_valid]
    invalid_pins = [r for r in results if not r.is_valid]
    
    # 基本统计
    print(f"总测试PIN数: {total}")
    print(f"有效PIN数: {len(valid_pins)} ({len(valid_pins)/total*100:.1f}%)")
    print(f"无效PIN数: {len(invalid_pins)} ({len(invalid_pins)/total*100:.1f}%)")
    
    # 格式分析
    numeric_total = sum(1 for r in results if r.is_numeric)
    numeric_valid = sum(1 for r in valid_pins if r.is_numeric)
    alphanumeric_total = total - numeric_total
    alphanumeric_valid = len(valid_pins) - numeric_valid
    
    print(f"\n格式分析:")
    print(f"纯数字PIN: {numeric_total} (有效: {numeric_valid})")
    print(f"字母数字PIN: {alphanumeric_total} (有效: {alphanumeric_valid})")
    
    # 错误分析
    error_pins = [r for r in results if r.error]
    login_page_pins = [r for r in results if r.has_login_page and not r.error]
    no_content_pins = [r for r in results if not r.has_pin_content and not r.has_login_page and not r.error]
    
    print(f"\n详细分析:")
    print(f"网络错误: {len(error_pins)}")
    print(f"显示登录页面: {len(login_page_pins)}")
    print(f"无PIN内容: {len(no_content_pins)}")
    
    # 有效PIN示例
    if valid_pins:
        print(f"\n有效PIN示例 (前10个):")
        for i, pin in enumerate(valid_pins[:10], 1):
            print(f"{i}. {pin.pin_id} - 图片数: {pin.image_url_count}")
    
    # 保存详细结果
    save_detailed_results(results)

def save_detailed_results(results: List[PinTestResult]):
    """保存详细结果到文件"""
    try:
        with open('pin_test_results_1000.json', 'w', encoding='utf-8') as f:
            results_data = []
            for r in results:
                results_data.append({
                    'pin_id': r.pin_id,
                    'is_numeric': r.is_numeric,
                    'is_valid': r.is_valid,
                    'status_code': r.status_code,
                    'has_login_page': r.has_login_page,
                    'has_pin_content': r.has_pin_content,
                    'has_image_urls': r.has_image_urls,
                    'image_url_count': r.image_url_count,
                    'response_size': r.response_size,
                    'error': r.error
                })
            json.dump(results_data, f, indent=2, ensure_ascii=False)
        print(f"\n详细结果已保存到: pin_test_results_1000.json")
    except Exception as e:
        print(f"保存结果失败: {e}")

def main():
    """主函数"""
    print("=== 1000个PIN多线程有效性测试 ===")
    
    # 获取PIN列表
    db_path = "output/interior design/pinterest.db"
    tester = PinTester(max_workers=15)
    
    print("正在从数据库随机获取1000个无图片PIN...")
    pin_ids = tester.get_random_no_image_pins(db_path, 1000)
    
    if len(pin_ids) < 1000:
        print(f"警告: 只找到 {len(pin_ids)} 个无图片PIN")
    
    print(f"获取到 {len(pin_ids)} 个PIN")
    
    # 多线程测试
    start_time = time.time()
    results = tester.test_pins_multithread(pin_ids)
    end_time = time.time()
    
    print(f"测试完成，耗时: {end_time - start_time:.1f} 秒")
    
    # 分析结果
    analyze_results(results)

if __name__ == "__main__":
    main()
