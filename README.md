# Pinterest Scraper v3.0 - 激进重构版

🎯 **极简设计，智能采集** - 完全重新设计的Pinterest数据采集工具

## ✨ 核心特性

### 🧠 智能策略选择
- **自动优化**: 根据目标数量智能选择最优采集策略
- **无需配置**: 告别复杂的模式选择，一个接口处理所有场景
- **突破限制**: 自动突破传统800个Pin限制，支持大规模数据采集

### 🎯 极简API设计
```python
from src.core.pinterest_scraper import PinterestScraper

scraper = PinterestScraper()
pins = scraper.scrape(query="nature", count=2000)  # 就这么简单！
```

### 🚀 统一混合采集策略
- **统一策略**: 所有数据量级都使用优化的混合策略
- **智能调整**: 根据目标数量动态调整采集参数
- **多阶段采集**: 关键词搜索 + Pin详情页深度扩展，突破传统限制
- **精确控制**: 严格按照用户指定数量采集，达到目标立即停止

### 💎 技术优势
- 基于Patchright的现代浏览器自动化
- 统一混合策略突破Pinterest传统800个Pin限制
- 精确目标控制，用户要多少就采集多少
- 智能去重和数据质量保证
- 多线程并发图片下载
- 自动缓存和断点续传
- Pin详情页深度扩展，发现更多相关内容

## 📦 安装

### 环境要求
- Python 3.10+
- Windows/Linux/macOS

### 快速安装
```bash
# 克隆项目
git clone <repository-url>
cd pinterest_scraper

# 安装依赖
pip install -e .

# 或使用uv（推荐）
uv sync
```

## 🔧 配置

### Cookie认证设置
为了有效爬取并避免Pinterest登录墙限制，需要使用已登录会话的cookies：

1. **登录Pinterest**: 在浏览器中登录你的Pinterest账户
2. **导出Cookies**: 使用浏览器扩展（如Cookie-Editor）导出cookies为JSON格式
3. **保存文件**: 将文件重命名为`cookies.json`并放在项目根目录

### 代理配置（可选）
```python
# 在代码中设置代理
scraper = PinterestScraper(proxy="http://user:pass@host:port")
```

## 🎯 使用方法

### 1. 基础使用 - 关键词搜索

```python
from src.core.pinterest_scraper import PinterestScraper

# 创建爬虫实例
scraper = PinterestScraper()

# 搜索关键词 - 使用统一混合策略
pins = scraper.scrape(query="nature photography", count=100)
print(f"获取到 {len(pins)} 个Pin")
```

### 2. URL爬取

```python
# 爬取特定URL
pins = scraper.scrape(
    url="https://www.pinterest.com/pinterest/",
    count=50
)
```

### 3. 大规模数据采集

```python
# 大量数据采集 - 统一混合策略
# 第一阶段：关键词搜索页面滚动采集
# 第二阶段：Pin详情页深度扩展，发现相关推荐
# 精确控制：采集到5000个就停止
pins = scraper.scrape(query="landscape", count=5000)
print(f"精确采集了 {len(pins)} 个Pin")
```

### 4. 自定义配置

```python
# 自定义配置
scraper = PinterestScraper(
    output_dir="my_output",
    download_images=False,  # 仅获取元数据
    proxy="http://proxy:8080",
    debug=True
)

pins = scraper.scrape(query="art", count=200)
```

### 5. 命令行使用

```bash
# 基础使用
python main.py --query "nature photography" --count 100

# 大规模采集
python main.py --query "landscape" --count 5000

# URL爬取
python main.py --url "https://www.pinterest.com/pinterest/" --count 50

# 仅获取元数据
python main.py --query "art" --count 200 --no-images
```

## 📊 数据输出

### JSON数据结构
```json
{
  "id": "pin_id",
  "title": "Pin标题",
  "description": "Pin描述",
  "image_urls": {
    "original": "原图URL",
    "1200": "1200px图片URL",
    "736": "736px图片URL"
  },
  "largest_image_url": "最大尺寸图片URL",
  "creator": {
    "name": "创作者名称",
    "url": "创作者链接"
  },
  "stats": {
    "saves": 保存次数,
    "comments": 评论数
  },
  "url": "Pin详情页URL",
  "downloaded": true,
  "download_path": "本地图片路径"
}
```

## 🔥 混合采集策略详解

当请求大量数据（>1000个Pin）时，系统自动启用混合策略，突破Pinterest传统限制：

### 第一阶段：关键词搜索
- 访问Pinterest搜索页面
- 滚动采集直到连续10次无新数据
- 通常可获得200-800个Pin（取决于关键词热度）

### 第二阶段：Pin详情页深度扩展
- 使用第一阶段的Pin作为种子
- 逐个访问Pin详情页
- 从每个详情页的相关推荐中发现新Pin
- 新发现的Pin自动加入扩展队列
- 直到达到目标数量或连续30个Pin无新数据

### 智能去重机制
- 实时监控去重率
- 当去重率>30%时自动停止策略切换
- 确保数据质量和采集效率

### 输出目录结构
```
output/
├── {关键词}/
│   ├── json/           # JSON数据文件
│   ├── images/         # 下载的图片
│   └── cache/          # 缓存文件
└── ultimate_collection/
    ├── collected_pins_{session_id}.json
    ├── progress_{session_id}.json
    └── final_report_{session_id}.json
```

## ⚡ 性能特性

- **突破限制**: 终极模式可突破传统800个Pin限制，采集9999+个Pin
- **智能去重**: 基于Pin ID的高效去重机制
- **断点续传**: 支持中断后继续采集
- **并发下载**: 多线程并发图片下载，提升效率
- **内存优化**: 滑动窗口缓存，防止内存泄漏
- **错误恢复**: 完善的异常处理和重试机制

## 🔍 高级功能

### 网络请求拦截
```python
from src.utils.network_interceptor import NetworkInterceptor

# 创建网络拦截器进行深度分析
interceptor = NetworkInterceptor()
summary = interceptor.start_analysis(
    url="https://www.pinterest.com/search/pins/?q=nature",
    scroll_count=10
)
```

### 自定义配置
```python
# 自定义浏览器配置
scraper = PinterestScraper(
    viewport_width=1920,
    viewport_height=1080,
    timeout=60,
    debug=True  # 启用调试模式
)
```

## 📝 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--query` / `-q` | 搜索关键词 | - |
| `--url` / `-u` | Pinterest URL | - |
| `--count` / `-c` | 目标采集数量 | 50 |
| `--output` / `-o` | 输出目录 | output |
| `--proxy` | 代理服务器 | - |
| `--no-images` | 仅获取元数据，不下载图片 | False |
| `--debug` | 调试模式 | False |
| `--log-level` | 日志级别 | INFO |

## 🛠️ 开发

### 项目结构
```
pinterest_scraper/
├── src/
│   ├── core/              # 核心功能模块
│   │   ├── scraper.py     # 主要爬虫类
│   │   ├── browser.py     # 浏览器管理
│   │   ├── parser.py      # 数据解析
│   │   └── config.py      # 配置文件
│   └── utils/             # 工具模块
│       ├── downloader.py  # 图片下载
│       ├── utils.py       # 通用工具
│       └── network_interceptor.py  # 网络拦截
├── tests/                 # 测试文件
├── examples/              # 使用示例
├── docs/                  # 文档
├── main.py               # 命令行入口
└── pyproject.toml        # 项目配置
```

### 运行测试
```bash
# 安装开发依赖
pip install -e ".[dev]"

# 运行测试
pytest tests/

# 代码格式化
black src/ tests/
isort src/ tests/
```

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个项目！

## ⚠️ 免责声明

本工具仅供学习和研究使用。使用时请遵守Pinterest的服务条款和robots.txt规则，尊重网站的使用政策。
