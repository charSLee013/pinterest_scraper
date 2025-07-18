#!/usr/bin/env python3
"""
测试HTML保存功能
"""

import os
import sys
import json
import requests
from typing import Dict, Optional

# 添加src目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def load_headers() -> Dict[str, str]:
    """加载保存的headers"""
    try:
        with open('browser_session.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            if 'headers' in data:
                print("使用已保存的headers")
                return data['headers']
    except Exception as e:
        print(f"加载headers失败: {e}")

    # 默认headers
    print("使用默认headers")
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

def fetch_and_save_html(pin_id: str, headers: Dict[str, str]) -> bool:
    """获取Pin页面HTML并保存到本地"""
    try:
        # 创建requests会话
        session = requests.Session()
        session.headers.update(headers)

        # 访问Pin详情页面
        pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
        print(f"正在访问: {pin_url}")
        
        response = session.get(pin_url, timeout=30)
        response.raise_for_status()

        # 保存HTML到本地文件
        debug_dir = "debug_html"
        os.makedirs(debug_dir, exist_ok=True)
        
        html_file = os.path.join(debug_dir, f"pin_{pin_id}.html")
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print(f"HTML已保存到: {html_file}")
        print(f"   文件大小: {len(response.text)} 字符")
        print(f"   状态码: {response.status_code}")
        print(f"   Content-Type: {response.headers.get('Content-Type', 'N/A')}")

        # 检查HTML内容的基本信息
        html_lower = response.text.lower()
        if 'pinterest' in html_lower:
            print("   包含Pinterest内容")
        if 'pin' in html_lower:
            print("   包含Pin相关内容")
        if 'image' in html_lower:
            print("   包含图片相关内容")
        if 'json' in html_lower:
            print("   包含JSON数据")
        
        return True

    except Exception as e:
        print(f"获取Pin {pin_id} HTML失败: {e}")
        return False
    finally:
        if 'session' in locals():
            session.close()

def main():
    """主函数"""
    print("开始测试HTML保存功能")
    print("=" * 50)

    # 加载headers
    headers = load_headers()

    # 测试几个Pin ID
    test_pins = ['b70fC3uk', '5379106336332111605', 'sa6tgU9C']

    success_count = 0
    for i, pin_id in enumerate(test_pins, 1):
        print(f"\n测试 {i}/{len(test_pins)}: Pin {pin_id}")
        print("-" * 30)

        if fetch_and_save_html(pin_id, headers):
            success_count += 1

    print("\n" + "=" * 50)
    print(f"测试完成: {success_count}/{len(test_pins)} 成功")
    print(f"HTML文件保存在: debug_html/ 目录")

if __name__ == "__main__":
    main()
