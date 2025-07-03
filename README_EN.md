# 🎨 Pinterest Image Scraper

> A simple yet powerful Pinterest image scraper that supports keyword search and URL crawling, making your material collection effortless and enjoyable!

<div align="center">
  <img src="assets/demo1.png" alt="Scraping Results" width="800"/>
  <br>
  <br><br>
  <img src="assets/demo2.png" alt="Folder Structure" width="600"/>
  <br>
  <em>one night's results</em>
</div>

## ✨ Features

- 🚀 Robust browser automation with Playwright
- 🔍 Support for keyword search and direct URL crawling
- 🌊 Infinite scroll support
- 🚀 Concurrent multi-keyword search
- 📦 Automatic image and metadata download
- 🎯 Proxy support
- 📝 Detailed logging
- 🛠️ Highly configurable

## 🚀 Quick Start

### Requirements

- Python 3.10+
- Playwright (automatically manages browser binaries like Chromium)

### Installation

We recommend using [uv](https://github.com/astral-sh/uv) for environment management:

```bash
# Clone the repository
git clone https://github.com/creeponsky/pinterest_scraper.git
cd pinterest_scraper

# Create virtual environment and install dependencies
uv sync
```

### Usage

0. Quick Start
   I've included a test file `inputs/input_topics.txt` that you can use to test the project immediately:

```bash
uv run python main.py
```

1. Keyword search:

```bash
uv run python main.py -s "nature landscape" -c 100
```

2. Multi-keyword concurrent search:

```bash
uv run python main.py -m "nature" "landscape" "city" -c 50
```

3. Read keywords from file:

```bash
uv run python main.py -f inputs/input_topics.txt -c 50
```

4. Read keywords from directory:

```bash
uv run python main.py -d inputs/topics/ -c 50
```

5. Direct URL crawling:

```bash
uv run python main.py -u "https://www.pinterest.com/pin/xxx" -c 50
```

### Parameters

- `-s, --search`: Single search keyword
- `-m, --multi-search`: Multiple search keywords (executed concurrently)
- `-f, --file`: Path to file containing keywords
- `-d, --directory`: Directory containing keyword files
- `-u, --urls`: List of Pinterest URLs
- `-c, --count`: Number of images to download per keyword/URL (default: 50)
- `-o, --output`: Output directory (default: output)
- `-p, --proxy`: Proxy server (format: http://user:pass@host:port)
- `--max-concurrent`: Maximum concurrency for multi-keyword search (default: 3)
- `--no-images`: Only fetch metadata, skip image download
- `--debug`: Enable debug mode

## 📁 Project Structure

- `main.py`: Main program entry
- `pinterest.py`: Pinterest scraper core class
- `browser.py`: Browser automation management (powered by Playwright)
- `downloader.py`: Image downloader
- `concurrent_search.py`: Concurrent search implementation
- `parser.py`: Page parser
- `utils.py`: Utility functions
- `config.py`: Configuration file

## ⚠️ Disclaimer

This project is for learning and research purposes only. Please do not use it for commercial purposes. When using this project, please comply with Pinterest's terms of service and relevant laws and regulations. The developer is not responsible for any issues arising from the use of this project.

## 📝 License

MIT License

---

<div align="center">
  <sub>Built with ❤️ by <a href="https://github.com/creeponsky">CreepOnSky</a></sub>
</div>
