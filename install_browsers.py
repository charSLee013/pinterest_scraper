#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Patchright浏览器自动安装脚本

解决Pinterest爬虫启动时的浏览器缺失问题
"""

import subprocess
import sys
from pathlib import Path


def install_patchright_browsers():
    """安装Patchright浏览器二进制文件"""
    print("🔧 正在安装Patchright浏览器...")
    
    try:
        # 使用uv运行patchright安装命令
        result = subprocess.run([
            "uv", "run", "python", "-m", "patchright", "install"
        ], capture_output=True, text=True, cwd=Path.cwd())
        
        if result.returncode == 0:
            print("✅ Patchright浏览器安装成功!")
            return True
        else:
            print(f"❌ 安装失败: {result.stderr}")
            return False
            
    except FileNotFoundError:
        print("❌ 未找到uv命令，请确保已安装uv")
        return False
    except Exception as e:
        print(f"❌ 安装过程中出错: {e}")
        return False


def test_browser():
    """测试浏览器是否能正常启动"""
    print("🧪 测试浏览器启动...")
    
    try:
        result = subprocess.run([
            "uv", "run", "python", "-c",
            """
from patchright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print('浏览器测试成功!')
    browser.close()
"""
        ], capture_output=True, text=True, cwd=Path.cwd())
        
        if result.returncode == 0:
            print("✅ 浏览器测试通过!")
            return True
        else:
            print(f"❌ 浏览器测试失败: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        return False


def main():
    """主函数"""
    print("🚀 Pinterest爬虫浏览器安装工具")
    print("=" * 50)
    
    # 安装浏览器
    if install_patchright_browsers():
        # 测试浏览器
        if test_browser():
            print("\n🎉 安装完成! Pinterest爬虫现在可以正常使用了")
            print("\n使用示例:")
            print("  uv run python main.py -q cats -c 10")
        else:
            print("\n⚠️  安装完成但测试失败，请检查环境配置")
            return 1
    else:
        print("\n❌ 安装失败，请手动执行以下命令:")
        print("  uv run python -m patchright install")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
