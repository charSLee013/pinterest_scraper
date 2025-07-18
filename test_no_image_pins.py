#!/usr/bin/env python3
"""
测试数据库中无图片PIN的HTML内容
"""

import sqlite3
import json
import requests
import os
from typing import Dict, List

def get_no_image_pins(db_path: str, limit: int = 10) -> List[str]:
    """获取无图片链接的PIN ID"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 获取纯数字的无图片PIN
    query = """
    SELECT id FROM pins 
    WHERE (image_urls IS NULL OR image_urls = '' OR image_urls = '[]')
    AND id GLOB '[0-9]*'
    LIMIT ?
    """
    
    cursor.execute(query, (limit,))
    pin_ids = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return pin_ids

def load_headers() -> Dict[str, str]:
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

def save_pin_html(pin_id: str, headers: Dict[str, str]) -> bool:
    """保存PIN的HTML内容"""
    try:
        session = requests.Session()
        session.headers.update(headers)
        
        pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
        response = session.get(pin_url, timeout=30)
        
        # 保存HTML
        debug_dir = "debug_html_no_images"
        os.makedirs(debug_dir, exist_ok=True)
        
        html_file = os.path.join(debug_dir, f"pin_{pin_id}.html")
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print(f"保存 {pin_id}: {html_file}")
        print(f"   文件大小: {len(response.text)} 字符")
        print(f"   状态码: {response.status_code}")

        # 检查内容
        html_lower = response.text.lower()
        has_login = 'log in' in html_lower and 'sign up' in html_lower
        has_pin_data = f'"{pin_id}"' in response.text
        has_images = 'pinimg.com' in response.text and ('.jpg' in response.text or '.png' in response.text)

        print(f"   登录页面: {'是' if has_login else '否'}")
        print(f"   包含PIN数据: {'是' if has_pin_data else '否'}")
        print(f"   包含图片: {'是' if has_images else '否'}")
        print()

        return True

    except Exception as e:
        print(f"保存 {pin_id} 失败: {e}")
        return False
    finally:
        if 'session' in locals():
            session.close()

def main():
    """主函数"""
    print("=== 测试无图片PIN的HTML内容 ===")
    
    # 获取无图片PIN
    db_path = "output/interior design/pinterest.db"
    pin_ids = get_no_image_pins(db_path, limit=10)
    
    print(f"获取到 {len(pin_ids)} 个无图片的纯数字PIN:")
    for i, pin_id in enumerate(pin_ids, 1):
        print(f"{i}. {pin_id}")
    print()
    
    # 加载headers
    headers = load_headers()
    
    # 保存HTML
    success_count = 0
    for pin_id in pin_ids:
        if save_pin_html(pin_id, headers):
            success_count += 1
    
    print(f"成功保存: {success_count}/{len(pin_ids)}")
    print(f"HTML文件保存在: debug_html_no_images/ 目录")

if __name__ == "__main__":
    main()
