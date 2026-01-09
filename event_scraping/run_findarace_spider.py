import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from event_scraping.spiders.fitness_training.findarace_spider import FindARaceSpider

if __name__ == "__main__":
    from datetime import datetime
    from pathlib import Path
    
    spider_name = "findarace"
    # Save JSON files to scraped_data folder with date in filename
    scraped_data_dir = Path(__file__).parent / "scraped_data"
    scraped_data_dir.mkdir(exist_ok=True)
    # Format: spidername_YYYY-MM-DD.json
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_file = str(scraped_data_dir / f"{spider_name}_{date_str}.json")
    
    # Debug: Print which spider is being imported
    print(f"Running {FindARaceSpider.__name__} spider")
    print(f"Spider name: {FindARaceSpider.name}")
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
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
        "RANDOMIZE_DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "LOG_LEVEL": "INFO"
    })
    # Use the spider class directly instead of the name string
    process.crawl(FindARaceSpider)
    process.start()

