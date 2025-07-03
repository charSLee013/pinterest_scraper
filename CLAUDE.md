# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Core Functionality Updates

The Pinterest scraper has been successfully migrated from Selenium to Playwright. This significantly improves performance and stability, and simplifies dependency management by automatically handling browser binaries.

## Common Commands

- **Install Dependencies**: `uv sync`
- **Run the scraper (default)**: `uv run python main.py`
- **Run with single keyword search**: `uv run python main.py -s "your keyword" -c 100`
- **Run with multiple concurrent keyword search**: `uv run python main.py -m "keyword1" "keyword2" -c 50`
- **Run with keywords from a file**: `uv run python main.py -f inputs/input_topics.txt -c 50`
- **Run with keywords from a directory**: `uv run python main.py -d inputs/topics/ -c 50`
- **Run with specific URLs**: `uv run python main.py -u "https://www.pinterest.com/pin/xxx" -c 50`

## High-Level Code Architecture

The project is a Pinterest image scraper built in Python. It uses `playwright` for browser automation and `loguru` for logging. The core functionality revolves around scraping images and metadata from Pinterest based on keywords or direct URLs.

- `main.py`: This is the entry point of the application. It parses command-line arguments, sets up logging, and orchestrates the scraping process. It handles different input methods (single keyword, multiple keywords, file input, directory input, URL list).
- `pinterest.py`: Contains the `PinterestScraper` class, which is the core logic for interacting with Pinterest, navigating pages, and extracting data.
- `browser.py`: Manages browser automation, now using `playwright` to control a Chrome/Chromium instance.
- `downloader.py`: Handles the actual downloading of images and metadata to the local filesystem.
- `concurrent_search.py`: Implements logic for performing searches with multiple keywords concurrently.
- `parser.py`: Responsible for parsing the HTML content of Pinterest pages to extract relevant information (image URLs, metadata, etc.).
- `utils.py`: A collection of utility functions used across the project.
- `config.py`: Stores configuration settings for the scraper.

The application supports various command-line arguments for customization, including output directory, proxy settings, maximum concurrency, and debug mode. It's designed to be highly configurable for different scraping needs.
