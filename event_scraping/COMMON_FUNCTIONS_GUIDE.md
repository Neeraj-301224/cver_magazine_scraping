# Common Functions Guide for Spiders

## Overview

All spiders that inherit from `BaseSpider` automatically have access to common functions with built-in error logging. **You don't need to do anything special** - just call them like normal methods!

## Available Common Functions

All these functions are **automatically available** in every spider:

### 1. `self.convert_date_format(date_str)`
Converts various date formats to MM/DD/YYYY format.

**Usage:**
```python
raw_date = "30 October 2025"
formatted_date = self.convert_date_format(raw_date)  # Returns "10/30/2025"
```

### 2. `self.geocode_address(address)`
Gets coordinates from an address using LocationIQ first, then falls back to Nominatim.

**Priority:**
1. **LocationIQ** (if `LOCATIONIQ_API_KEY` is configured in settings) - faster and more reliable
2. **Nominatim** (OpenStreetMap) - free fallback option

**Usage:**
```python
address = "London, UK"
coords = self.geocode_address(address)  # Returns {'lat': 51.5074, 'lon': -0.1278} or None
```

**Configuration:**
To use LocationIQ, add to your Scrapy settings (`event_scraping/event_scraping/settings.py`):
```python
LOCATIONIQ_API_KEY = 'your_api_key_here'
```

If LocationIQ fails or no API key is configured, it automatically falls back to Nominatim.

**Note:** The geocoding logic is now in `utils/common.py` and can be used directly:
```python
from event_scraping.utils.common import geocode_address, geocode_locationiq, geocode_nominatim

# Direct usage (useful for non-spider code)
coords = geocode_address(
    address="London, UK",
    locationiq_api_key="your_key",
    cache={}  # Optional cache dict
)
```

### 3. `self.extract_coordinates(response)`
Extracts coordinates from a page using multiple heuristics.

**Usage:**
```python
coords = self.extract_coordinates(response)  # Returns {'lat': 51.5074, 'lon': -0.1278} or None
```

### 4. `self.extract_address(response)`
Extracts address from a page using multiple selectors.

**Usage:**
```python
address = self.extract_address(response)  # Returns address string or None
```

### 5. `self.remove_location_text(address)`
Removes "Location:" prefixes from addresses.

**Usage:**
```python
cleaned = self.remove_location_text("Location: London, UK")  # Returns "London, UK"
```

### 6. `self.get_event_category(title, description_parts)`
Categorizes events based on keywords (if spider has `CATEGORY_KEYWORDS`).

**Usage:**
```python
category, subcategory = self.get_event_category(title, desc_parts)
```

## Error Logging

All these functions now have **automatic error logging** built-in:
- Errors are automatically logged with context
- Functions return `None` or safe fallback values on error
- No need to wrap calls in try-except blocks

## Example: Before vs After

### Before (with duplicate code):
```python
def convert_date_format(self, date_str):
    """Convert date to MM/DD/YYYY format."""
    if not date_str:
        return None
    try:
        from datetime import datetime
        # ... 50+ lines of date parsing code ...
    except Exception as e:
        self.logger.error(f"Date conversion failed: {e}")
        return date_str
```

### After (using base class):
```python
# Just call it - no implementation needed!
date = self.convert_date_format(raw_date)
```

## Site-Specific Overrides

If your spider needs site-specific behavior, you can override the method:

```python
def extract_address(self, response):
    """Site-specific address extraction."""
    # Try site-specific selector first
    address = response.css('.site-specific-class::text').get()
    if address:
        return self.clean_text(address)
    
    # Fallback to base class method
    return super().extract_address(response)
```

## What You Need to Do

1. **Remove duplicate implementations** - Delete methods like `convert_date_format`, `geocode_address`, etc. from your spiders
2. **Keep calling them the same way** - `self.convert_date_format(date)` works exactly as before
3. **Remove `geocoding_cache` initialization** - It's now handled in `BaseSpider.__init__()`
4. **Override only if needed** - Only override methods if you need site-specific behavior

## Benefits

✅ **Less code** - Remove 50-100 lines of duplicate code per spider  
✅ **Better error handling** - Automatic error logging with context  
✅ **Consistency** - All spiders use the same implementations  
✅ **Maintainability** - Fix bugs once in base class, not in every spider  
✅ **No breaking changes** - Existing code continues to work

## Migration Checklist

For each spider:
- [ ] Remove duplicate `convert_date_format()` method
- [ ] Remove duplicate `geocode_address()` method  
- [ ] Remove duplicate `extract_coordinates()` method
- [ ] Remove duplicate `remove_location_text()` method
- [ ] Remove duplicate `extract_address()` method (unless site-specific)
- [ ] Remove `self.geocoding_cache = {}` from `__init__`
- [ ] Keep all existing function calls - they work as-is!

## Example: Cleaned Spider

See `mindfulnessassociation_spider.py` for a complete example of a cleaned-up spider.

