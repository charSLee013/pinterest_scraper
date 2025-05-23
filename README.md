# 🎨 Pinterest 图片爬虫

> 一个简单高效的 Pinterest 图片爬虫工具，支持关键词搜索和 URL 爬取，让你的素材收集变得轻松愉快！

<div align="center">
  <img src="assets/demo1.png" alt="爬取结果展示" width="800"/>
  <br>
  <br><br>
  <img src="assets/demo2.png" width="600"/>
  <br>
  <em>一个晚上爬取了70w张图片</em>
</div>

## ✨ 特性

- 🔍 支持关键词搜索和 URL 直接爬取
- 🌊 支持无限滚动加载
- 🚀 多关键词并发搜索
- 📦 自动下载图片和元数据
- 🎯 支持代理设置
- 📝 详细的日志记录
- 🛠️ 高度可配置

## 🚀 快速开始

### 环境要求

- Python 3.10+
- Chrome/Chromium 浏览器

### 安装

推荐使用 [uv](https://github.com/astral-sh/uv) 进行环境管理：

```bash
# 克隆项目
git clone https://github.com/creeponsky/image_scraper.git
cd image_scraper

# 创建虚拟环境并安装依赖
uv sync
```

### 使用方法

0. 直接使用
   我已经内置了一个测试的文件 `inputs/input_topics.txt` 可以直接运行项目来测试结果

```bash
uv run python main.py
```

1. 关键词搜索：

```bash
uv run python main.py -s "nature landscape" -c 100
```

2. 多关键词并发搜索：

```bash
uv run python main.py -m "nature" "landscape" "city" -c 50
```

3. 从文件读取关键词：

```bash
uv run python main.py -f inputs/input_topics.txt -c 50
```

4. 从目录读取多个关键词文件：

```bash
uv run python main.py -d inputs/topics/ -c 50
```

5. 直接爬取 URL：

```bash
uv run python main.py -u "https://www.pinterest.com/pin/xxx" -c 50
```

### 参数说明

- `-s, --search`: 单个搜索关键词
- `-m, --multi-search`: 多个搜索关键词（并发执行）
- `-f, --file`: 包含关键词的文件路径
- `-d, --directory`: 包含关键词文件的目录
- `-u, --urls`: Pinterest URL 列表
- `-c, --count`: 每个关键词/URL 要下载的图片数量（默认：50）
- `-o, --output`: 输出目录（默认：output）
- `-p, --proxy`: 代理服务器（格式：http://user:pass@host:port）
- `--max-concurrent`: 多关键词搜索时的最大并发数（默认：3）
- `--no-images`: 仅获取元数据，不下载图片
- `--debug`: 启用调试模式

## 📁 项目结构

- `main.py`: 主程序入口
- `pinterest.py`: Pinterest 爬虫核心类
- `browser.py`: 浏览器自动化管理
- `downloader.py`: 图片下载器
- `concurrent_search.py`: 并发搜索实现
- `parser.py`: 页面解析器
- `utils.py`: 工具函数
- `config.py`: 配置文件

## ⚠️ 免责声明

本项目仅供学习和研究使用，请勿用于商业用途。使用本项目时请遵守 Pinterest 的使用条款和相关法律法规。开发者不对使用本项目产生的任何问题负责。

## 📝 开源协议

MIT License

---

<div align="center">
  <sub>Built with ❤️ by <a href="https://github.com/creeponsky">CreepOnSky</a></sub>
</div>
