import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from event_scraping.spiders.wellness_mind.mindspace_spider import MindspaceSpider

if __name__ == "__main__":
    from datetime import datetime
    from pathlib import Path
    
    spider_name = "mindspace"
    # Save JSON files to scraped_data folder with date in filename
    scraped_data_dir = Path(__file__).parent / "scraped_data"
    scraped_data_dir.mkdir(exist_ok=True)
    # Format: spidername_YYYY-MM-DD.json
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_file = str(scraped_data_dir / f"{spider_name}_{date_str}.json")
    
    print(f"Running {MindspaceSpider.__name__} spider")
    print(f"Spider name: {MindspaceSpider.name}")
    print(f"Output file: {output_file}")

    process = CrawlerProcess({
        **get_project_settings(),
        "FEEDS": {
            output_file: {
                "format": "json", 
                "encoding": "utf8",
                "overwrite": True,
                "indent": 2
            }
        },
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "LOG_LEVEL": "INFO",
        "HTTPERROR_ALLOWED_CODES": [403, 404],  # Allow 403 and 404 responses
        "DEFAULT_REQUEST_HEADERS": {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    })
    process.crawl(MindspaceSpider)
    process.start()

