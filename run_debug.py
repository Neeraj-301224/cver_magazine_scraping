#!/usr/bin/env python3
"""
Simple script to run the spider with debugging enabled.

This script will run the spider and stop at each breakpoint,
allowing you to inspect variables and step through the code.
"""

import os
import sys
from scrapy.crawler import CrawlerProcess

# Add the project directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'event_scraping'))

def run_spider_with_debug():
    """Run the spider with debugging enabled."""
    print("🚀 Starting Spider with Line-by-Line Debugging")
    print("="*60)
    print("The spider will stop at each breakpoint (#pdb.set_trace())")
    print("You can inspect variables and step through the code.")
    print("="*60)
    
    # Configure Scrapy settings for debugging
    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
        'AUTOTHROTTLE_DEBUG': True,
        'FEEDS': {
            'debug_output.json': {
                'format': 'json',
                'overwrite': True,
            }
        },
        'LOG_LEVEL': 'DEBUG',
    }
    
    # Create crawler process
    process = CrawlerProcess(settings)
    
    # Import and run the spider
    from event_scraping.spiders.fitness_training.runningcalendar_spider import RunningCalendarSpider
    
    print("Starting crawler...")
    process.crawl(RunningCalendarSpider)
    process.start()

if __name__ == "__main__":
    print("Debugging Commands:")
    print("  'c' - continue execution")
    print("  'n' - next line")
    print("  's' - step into function")
    print("  'p variable_name' - print variable")
    print("  'pp variable_name' - pretty print variable")
    print("  'l' - show current code")
    print("  'h' - help")
    print("  'q' - quit debugging")
    print()
    
    try:
        run_spider_with_debug()
    except KeyboardInterrupt:
        print("\nSpider interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
