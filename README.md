# Pinterest Scraper

This is a Python-based web scraper for downloading images and metadata from Pinterest. It uses Playwright for browser automation and can be configured to scrape based on keywords or direct URLs.

## Features

- Scrape pins by search keyword
- Scrape pins from a specific URL (e.g., a user's board)
- Concurrent image downloading
- Caching of scraped data to avoid re-scraping
- Headless browser operation
- Proxy support
- Cookie-based authentication to bypass login walls

## Key Logic and Performance

The scraper has been recently updated with significant improvements to its core logic and performance:

-   **Smarter Scrolling Logic**: The scraper no longer stops prematurely. It employs an intelligent termination strategy based on dynamic conditions, such as detecting when no new content has been loaded for several consecutive scrolls or when the rate of new data drops significantly. This allows for much deeper and more comprehensive scraping sessions.
-   **Optimized Parsing**: The parsing engine has been streamlined by removing its dependency on outdated and unreliable `JSON-LD` data structures. It now directly and robustly extracts data from primary HTML elements, making the process faster and less prone to errors from Pinterest's front-end changes.
-   **Refined Logging System**: The logging output has been completely overhauled for clarity. The default `INFO` level now provides a clean, high-level summary of the scraping process, including statistics for each scroll (new items, duplicates, totals). Verbose debugging details are reserved for the `DEBUG` level, ensuring the console remains readable during normal operation.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd pinterest_scraper
    ```

2.  **Install dependencies:**
    It is recommended to use a virtual environment.
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```

3.  **Install Playwright browsers:**
    The scraper uses Playwright's Chromium browser.
    ```bash
    playwright install chromium
    ```

## Authentication with Cookies

To scrape effectively and avoid being blocked by Pinterest's login wall, you need to use cookies from a logged-in session.

**How to get your `cookies.json` file:**

1.  **Log in to Pinterest:**
    Open your regular web browser (e.g., Chrome) and log in to your Pinterest account.

2.  **Install a cookie exporter extension:**
    Install a browser extension that can export cookies in the `Netscape` or `JSON` format. A recommended extension for Chrome is [Cookie-Editor](https://chrome.google.com/webstore/detail/cookie-editor/hlkenndednhfkekhgcdicdfddnkalmdm).

3.  **Export your cookies:**
    -   With the extension installed, navigate to `pinterest.com`.
    -   Click the Cookie-Editor icon in your browser's toolbar.
    -   Click the "Export" button in the extension's popup.
    -   Choose "Export as JSON" and save the file.

4.  **Save the file:**
    -   Rename the downloaded file to `cookies.json`.
    -   Place this `cookies.json` file in the root directory of the `pinterest_scraper` project.

The scraper will automatically detect and use this file if it's named `cookies.json` and placed in the root directory.

## Usage

You can use the scraper via `main.py` with command-line arguments.

**Example: Scrape by keyword**
```bash
python main.py --query "nature photography" --limit 100
```

**Example: Scrape by URL**
```bash
python main.py --url "https://www.pinterest.com/pinterest/official-news/" --limit 50
```

**Command-line arguments:**

-   `--query`: The search term to scrape for.
-   `--url`: The Pinterest URL to scrape.
-   `--limit`: The number of pins to scrape (default: 50).
-   `--output`: The directory to save results to (default: `output`).
-   `--proxy`: The proxy server to use (e.g., `http://user:pass@host:port`).
-   `--cookie-file`: Path to a specific cookie file (overrides the default `cookies.json`).
-   `--no-download`: Disable image downloading.
-   `--debug`: Enable debug mode (saves screenshots and HTML).
