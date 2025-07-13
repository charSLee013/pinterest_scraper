#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫项目一键安装脚本

自动处理依赖安装和浏览器配置
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description=""):
    """运行命令并处理错误"""
    print(f"🔧 {description}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, 
                              capture_output=True, text=True)
        print(f"✅ {description} - 成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - 失败")
        print(f"错误信息: {e.stderr}")
        return False


def main():
    """主安装流程"""
    print("🚀 Pinterest爬虫项目一键安装")
    print("=" * 50)
    
    # 检查uv是否安装
    try:
        subprocess.run(["uv", "--version"], check=True, 
                      capture_output=True)
        print("✅ UV已安装")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ 请先安装UV: https://docs.astral.sh/uv/getting-started/installation/")
        return 1
    
    # 安装依赖
    if not run_command("uv sync", "安装项目依赖"):
        return 1
    
    # 安装浏览器
    if not run_command("uv run python -m patchright install", "安装Patchright浏览器"):
        return 1
    
    # 测试安装
    test_cmd = '''uv run python -c "
from patchright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print('浏览器测试成功!')
    browser.close()
"'''
    
    if not run_command(test_cmd, "测试浏览器安装"):
        return 1
    
    print("\n🎉 安装完成!")
    print("\n📖 使用方法:")
    print("  # 基础使用")
    print("  uv run python main.py -q cats -c 10")
    print("\n  # 详细模式")  
    print("  uv run python main.py -q cats -c 10 --verbose")
    print("\n  # 调试模式")
    print("  uv run python main.py -q cats -c 10 --debug")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
