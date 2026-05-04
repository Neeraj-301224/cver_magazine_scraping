import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from event_scraping.spiders.fitness_training.timeoutdoors_cycling_swim_spider import (
    TimeOutdoorsCyclingSwimSpider,
)

if __name__ == "__main__":
    from datetime import datetime
    from pathlib import Path

    spider_name = "timeoutdoors_cycling_swim"
    scraped_data_dir = Path(__file__).parent / "scraped_data"
    scraped_data_dir.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_file = str(scraped_data_dir / f"{spider_name}_{date_str}.json")

    print(f"Running {TimeOutdoorsCyclingSwimSpider.__name__} spider")
    print(f"Spider name: {TimeOutdoorsCyclingSwimSpider.name}")
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
    process.crawl(TimeOutdoorsCyclingSwimSpider)
    process.start()
