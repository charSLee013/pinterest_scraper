# Pinterest Image Scraper

A powerful Python scraper for downloading Pinterest images in various sizes, including original/largest size.

## Features

- Scrape Pinterest by search terms or specific URLs
- Extract all available image sizes (144px, 236px, original, etc.)
- Save full data to JSON, including all image URLs
- Download original/largest size images automatically
- Show progress with nice progress bars
- Configurable output directory and proxy support

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/pinterest-image-scraper.git
cd pinterest-image-scraper
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Command Line Interface

The scraper can be used from the command line with the following options:

```bash
# Search for images with a query
python main.py -s "cute cats" -c 100 -o output

# Scrape from specific Pinterest URLs
python main.py -u https://www.pinterest.com/username/boardname/ -c 50 -o output

# Use with a proxy
python main.py -s "landscape photography" -c 200 -o output -p http://user:pass@host:port
```

### Command Line Options

- `-s, --search`: Search query for Pinterest
- `-u, --urls`: List of Pinterest URLs to scrape
- `-c, --count`: Number of images to download (default: 50)
- `-o, --output`: Output directory (default: "output")
- `-p, --proxy`: Proxy to use (format: http://user:pass@host:port)

### Using as a Module

You can also use the scraper as a module in your Python code:

```python
from pinterest_scraper import PinterestScraper

# Initialize the scraper
scraper = PinterestScraper(output_dir="output")

# Scrape by search
results = scraper.scrape_search("cute cats", count=100)

# Scrape by URLs
urls = [
    "https://www.pinterest.com/username/boardname/",
    "https://www.pinterest.com/username/another-board/"
]
results = scraper.scrape_urls(urls, count_per_url=50)
```

## Output Structure

The scraper creates the following directory structure:

```
output/
├── images/
│   ├── cute_cats_1234567890.jpg
│   ├── cute_cats_0987654321.jpg
│   └── ...
└── json/
    ├── pinterest_search_cute_cats.json
    ├── pinterest_url_username_boardname.json
    └── ...
```

### JSON Output Format

The JSON files contain detailed information about each pin:

```json
[
  {
    "pin_id": "1234567890",
    "description": "Cute kitten playing with yarn",
    "image_urls": {
      "144": "https://i.pinimg.com/144x/ab/cd/ef/abcdef1234567890.jpg",
      "236": "https://i.pinimg.com/236x/ab/cd/ef/abcdef1234567890.jpg",
      "474": "https://i.pinimg.com/474x/ab/cd/ef/abcdef1234567890.jpg",
      "736": "https://i.pinimg.com/736x/ab/cd/ef/abcdef1234567890.jpg",
      "original": "https://i.pinimg.com/originals/ab/cd/ef/abcdef1234567890.jpg"
    },
    "largest_image_url": "https://i.pinimg.com/originals/ab/cd/ef/abcdef1234567890.jpg",
    "downloaded": true,
    "download_path": "output/images/cute_cats_1234567890.jpg"
  },
  // More pins...
]
```

## Requirements

- Python 3.7+
- Chrome or Chromium browser (for Selenium)

## License

MIT License

## Disclaimer

This tool is for educational purposes only. Please respect Pinterest's terms of service and use responsibly. 