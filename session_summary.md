# Project Context for Pinterest Scraper

This document summarizes the current state of the Pinterest Scraper project, detailing the architecture, previous work, and remaining tasks. This context is crucial for seamlessly continuing development in a new session.

## Project Overview

The Pinterest scraper is a Python application built for scraping images and metadata from Pinterest. It leverages `Playwright` for browser automation and `loguru` for logging. The core functionality includes scraping based on keywords or direct URLs and handling concurrent searches.

Key components:
- `main.py`: Entry point, handles CLI arguments, logging, and orchestrates scraping.
- `pinterest.py`: Contains the `PinterestScraper` class, managing interaction with Pinterest.
- `browser.py`: Manages Playwright browser automation, including scrolling and element interaction.
- `parser.py`: Handles HTML parsing and data extraction from Pinterest pages.
- `downloader.py`: Manages downloading of images and metadata.
- `concurrent_search.py`: Implements logic for concurrent keyword searches.
- `config.py`: Stores configuration settings.
- `utils.py`: Provides utility functions.

## Previous Work and Current Status

The primary task was to fix an issue where the scraper was stopping prematurely, failing to collect the target number of pins from a given URL.

**Problem Identified**:
The `test_integration.py` test `test_scrape_url` failed, reporting "AssertionError: 27 not greater than or equal to 50 : 获取的pin数量不足, 预期: 50, 实际: 27". This indicated that the scraper collected only 27 pins out of the expected 50 when scraping a URL. Debug logs from the previous session showed "多次尝试后仍无法获取新数据，当前已收集 27 项", suggesting the scraper stopped because it believed no new data was available.

**Investigation in this session**:
- Reviewed `browser.py` and `pinterest.py` to understand the scrolling and extraction logic, specifically `simple_scroll_and_extract` and `scrape_url`.
- Identified a potential bug in `parser.py:392-411` where `json_data` might not be properly initialized or handled for all cases within the JSON-LD script parsing. The error `name 'json_data' is not defined` from the `STDERR` confirmed this.

**Fixes Applied**:
- **parser.py**: Addressed the `json_data` not defined bug by ensuring `script.string` is checked for existence before processing and adjusting the warning message in `parser.py:411` to `script.prettify()[:200] if script.string else 'None'`. This ensures robust handling of JSON-LD script content.

## Remaining Tasks (from Todo List)

1. **Enhance `extract_pins_from_html` in `parser.py` to be more robust.** (ID: 2, Priority: high)
   - This involves further improving the parsing logic in `parser.py` to handle various Pinterest HTML structures and ensure more reliable pin extraction. This might include:
     - Improving selectors.
     - Handling different JSON data structures within the HTML.
     - More comprehensive error handling during parsing.
     - Considering alternative data extraction methods if HTML parsing proves unreliable for certain page types.

2. **Adjust `consecutive_no_new_data` threshold or special scrolling strategies in `browser.py` if needed.** (ID: 3, Priority: medium)
   - The scraper stopped prematurely even after fixing the parsing bug, suggesting the scrolling logic might still be too aggressive in determining when to stop. This task involves:
     - Re-evaluating the `consecutive_no_new_data` threshold.
     - Adjusting the `max_stuck_count`.
     - Exploring and implementing more advanced or adaptive scrolling strategies to ensure all available content is loaded. This might involve:
       - Detecting scrollable elements more accurately.
       - Simulating user-like scrolling patterns more closely.
       - Waiting for network requests to complete before determining if new content has loaded.

## Next Steps for New Session

The next session should focus on addressing the remaining high-priority task of making `extract_pins_from_html` more robust, and then revisiting the scrolling logic if the issue persists.

**Recommended Actions:**
1. Start by reviewing the `extract_pins_from_html` function in `parser.py` again, considering common Pinterest page structures (e.g., search results, individual pin pages, user profiles).
2. Look for patterns in Pinterest's HTML/JSON data that might provide more stable extraction points.
3. Once the parsing is improved, re-run the integration tests (`python -m unittest /Users/charslee/Repo/private/pinterest_scraper/test_integration.py`) to verify if the scraping count issue is resolved.
4. If the issue persists, then focus on adjusting the scrolling strategies in `browser.py`.
