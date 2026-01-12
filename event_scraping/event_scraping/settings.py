# Add these at the top of settings.py
import logging
# Scrapy settings for event_scraping project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "event_scraping"

SPIDER_MODULES = ["event_scraping.spiders"]
NEWSPIDER_MODULE = "event_scraping.spiders"

ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "event_scraping (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Concurrency and throttling settings
#CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "event_scraping.middlewares.EventScrapingSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "event_scraping.middlewares.EventScrapingDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    "event_scraping.pipelines.EventScrapingPipeline": 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"


# Enable logging
LOG_ENABLED = True
LOG_LEVEL = 'DEBUG'

# Slow down the spider to be polite to the server
DOWNLOAD_DELAY = 2

# Add a user agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# Geocoding API keys (optional - services will be skipped if keys are not provided)
# Get your API keys from:
# - LocationIQ: https://locationiq.com/
# - OpenCage: https://opencagedata.com/
# 
# You can set these via environment variables or directly here:
LOCATIONIQ_API_KEY = 'pk.9749c919312e2bc60949144d0edb6806'
# OPENCAGE_API_KEY = 'your_opencage_api_key_here'
#
# The geocoding system will try services in order:
# 1. LocationIQ (if API key is configured) - faster and more reliable
# 2. Nominatim (free, no API key required, but may be rate-limited) - fallback
#
# If a service fails or is blocked, it will automatically try the next one.

# Database configuration for WordPress MySQL database
DB_HOST = 'sql7.nur4.host-h.net'
DB_NAME = 'cveropfnwf_wp7fbf'  # WordPress database name
DB_USER = 'cveropfnwf_998'  # Database username
DB_PASSWORD = '95RnJXcDDkmV16s7VUav'  # Database password
DB_PORT = 3306  # Database port

# Backup retention settings
BACKUP_RETENTION_DAYS = 7  # Number of days to keep backup files (older files will be deleted)