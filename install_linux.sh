#!/bin/bash

# Pinterest爬虫Linux一键安装脚本
# 自动处理系统依赖和浏览器安装

set -e  # 遇到错误立即退出

echo "🚀 Pinterest爬虫Linux一键安装"
echo "=================================================="

# 检查是否为root用户
if [ "$EUID" -eq 0 ]; then
    echo "⚠️  检测到root用户，将直接安装系统依赖"
    SUDO=""
else
    echo "ℹ️  检测到普通用户，将使用sudo安装系统依赖"
    SUDO="sudo"
fi

# 检查UV是否安装
echo "🔧 检查UV安装状态..."
if ! command -v uv &> /dev/null; then
    echo "❌ UV未安装，请先安装UV: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi
echo "✅ UV已安装"

# 安装项目依赖
echo "🔧 安装项目依赖..."
if uv sync; then
    echo "✅ 项目依赖安装成功"
else
    echo "❌ 项目依赖安装失败"
    exit 1
fi

# 安装Patchright浏览器
echo "🔧 安装Patchright浏览器..."
if uv run python -m patchright install; then
    echo "✅ Patchright浏览器安装成功"
else
    echo "❌ Patchright浏览器安装失败"
    exit 1
fi

# 安装系统依赖
echo "🔧 安装浏览器系统依赖..."
if uv run python -m patchright install-deps; then
    echo "✅ 浏览器系统依赖安装成功"
elif $SUDO uv run python -m patchright install-deps; then
    echo "✅ 浏览器系统依赖安装成功（使用sudo）"
else
    echo "⚠️  自动安装系统依赖失败，尝试手动安装..."
    
    # 检测Linux发行版
    if command -v apt-get &> /dev/null; then
        echo "🔧 检测到Debian/Ubuntu系统，使用apt-get安装依赖..."
        $SUDO apt-get update
        $SUDO apt-get install -y \
            libnss3 \
            libnspr4 \
            libatk1.0-0 \
            libatk-bridge2.0-0 \
            libatspi2.0-0 \
            libxdamage1 \
            libxrandr2 \
            libxss1 \
            libgtk-3-0 \
            libasound2
        echo "✅ 系统依赖安装完成"
    elif command -v yum &> /dev/null; then
        echo "🔧 检测到CentOS/RHEL系统，使用yum安装依赖..."
        $SUDO yum install -y \
            nss \
            nspr \
            atk \
            at-spi2-atk \
            gtk3 \
            alsa-lib
        echo "✅ 系统依赖安装完成"
    else
        echo "❌ 无法识别Linux发行版，请手动安装浏览器依赖"
        echo "参考命令："
        echo "  Debian/Ubuntu: sudo apt-get install libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 libxdamage1"
        echo "  CentOS/RHEL: sudo yum install nss nspr atk at-spi2-atk gtk3 alsa-lib"
        exit 1
    fi
fi

# 测试浏览器安装
echo "🔧 测试浏览器安装..."
if uv run python -c "
from patchright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    print('浏览器测试成功!')
    browser.close()
"; then
    echo "✅ 浏览器测试通过!"
else
    echo "❌ 浏览器测试失败"
    echo "请检查系统依赖是否正确安装"
    exit 1
fi

echo ""
echo "🎉 安装完成!"
echo ""
echo "📖 使用方法:"
echo "  # 基础使用"
echo "  uv run python main.py -q cats -c 10"
echo ""
echo "  # 详细模式"  
echo "  uv run python main.py -q cats -c 10 --verbose"
echo ""
echo "  # 调试模式"
echo "  uv run python main.py -q cats -c 10 --debug"
