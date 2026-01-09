# Important File Locations

## Settings File
**Location:** `event_scraping/event_scraping/settings.py`

**Full Path:** `c:\Neeraj\Scraping\cver_magazine\event_scraping\event_scraping\settings.py`

This file contains:
- LocationIQ API key: `LOCATIONIQ_API_KEY = 'pk.9749c919312e2bc60949144d0edb6806'`
- Scrapy configuration settings
- Logging settings

## Common Utilities File
**Location:** `event_scraping/event_scraping/utils/common.py`

**Full Path:** `c:\Neeraj\Scraping\cver_magazine\event_scraping\event_scraping\utils\common.py`

This file contains utility functions:
- `clean_text()` - Clean and normalize text
- `extract_date()` - Extract and standardize dates
- `get_absolute_url()` - Convert relative URLs to absolute

## Base Spider File
**Location:** `event_scraping/event_scraping/spiders/base_spider.py`

**Full Path:** `c:\Neeraj\Scraping\cver_magazine\event_scraping\event_scraping\spiders\base_spider.py`

This file contains:
- Common functions for all spiders
- Error logging mechanism
- Geocoding functions (LocationIQ + Nominatim)

## Project Structure

```
cver_magazine/
├── event_scraping/
│   ├── event_scraping/
│   │   ├── settings.py          ← SETTINGS FILE HERE
│   │   ├── spiders/
│   │   │   ├── base_spider.py   ← BASE SPIDER HERE
│   │   │   └── ...
│   │   └── utils/
│   │       └── common.py         ← COMMON FILE HERE
│   └── ...
```

## Quick Access

To open these files in your IDE:
1. **Settings:** Navigate to `event_scraping/event_scraping/settings.py`
2. **Common:** Navigate to `event_scraping/event_scraping/utils/common.py`
3. **Base Spider:** Navigate to `event_scraping/event_scraping/spiders/base_spider.py`

## LocationIQ API Key

The LocationIQ API key is configured in:
- **File:** `event_scraping/event_scraping/settings.py`
- **Line:** Around line 108
- **Variable:** `LOCATIONIQ_API_KEY`

All spiders automatically use this key from settings - no need to configure it in individual spiders!

