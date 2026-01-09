# Scraped Data Processing Guide

## Overview

All scraped JSON files are now saved to the `scraped_data/` folder, and `insert_event.py` automatically processes all JSON files from this folder while checking for duplicates.

## Folder Structure

```
event_scraping/
├── scraped_data/          ← All JSON files saved here
│   ├── bhf.json
│   ├── mindfulnessassociation.json
│   ├── letsdothis.json
│   └── ... (all spider JSON files)
├── insert_event.py       ← Processes all JSON files
└── ...
```

## How It Works

### 1. Running Spiders

When you run any spider (e.g., `python run_bhf_spider.py`), the JSON output is automatically saved to:
- **Location:** `event_scraping/scraped_data/{spider_name}.json`
- **Example:** `event_scraping/scraped_data/bhf.json`

### 2. Processing JSON Files

Run `insert_event.py` to process all JSON files:

```bash
cd event_scraping
python insert_event.py
```

This will:
1. ✅ Find all `*.json` files in `scraped_data/` folder
2. ✅ Check each event against the database for duplicates
3. ✅ Only insert new events (skips duplicates)
4. ✅ Show detailed progress and summary

### 3. Duplicate Detection

Events are checked for duplicates using:
- **Primary:** URL matching (most reliable)
- **Fallback:** Name + Date combination

If an event already exists, it's skipped and not inserted again.

## Features

### Automatic Folder Creation
- The `scraped_data/` folder is created automatically if it doesn't exist
- All spiders save their JSON files there

### Duplicate Prevention
- Before inserting, checks if event exists in database
- Skips geocoding for events that already exist (if enabled)
- Prevents duplicate database entries

### Batch Processing
- Processes all JSON files in one run
- Shows progress for each file
- Provides summary statistics

## Usage Examples

### Process All JSON Files
```bash
cd event_scraping
python insert_event.py
```

### Process Specific Folder
```python
from insert_event import main
main(json_folder="path/to/custom/folder")
```

## Output Example

```
Found 5 JSON file(s) in 'scraped_data'
================================================================================

📄 Processing file: bhf.json
--------------------------------------------------------------------------------
  Found 25 event(s) in bhf.json
  [1/25] ➕ Inserting: London Marathon 2025
      ✅ Successfully inserted (post ID: 1234)
  [2/25] ⏭️  Skipping duplicate: London 10K (exists as post ID: 567)
  ...

📊 File Summary for bhf.json:
     ✅ Successful: 20
     ⏭️  Duplicates: 5
     ❌ Failed: 0

================================================================================
📊 FINAL SUMMARY
================================================================================
Total JSON files processed: 5
Total events found: 125
✅ Successfully inserted: 95
⏭️  Duplicates skipped: 30
❌ Failed: 0
================================================================================
```

## Database Check Before Geocoding

To enable database checking before geocoding (saves API calls), set in your spider:

```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.check_db_before_geocoding = True  # Enable DB check
```

Then when calling geocode_address, pass event data:

```python
# In parse_event method
item = EventScrapingItem()
# ... populate item ...

# Check and geocode only if event is new
coords = self.geocode_address(address, event_data=dict(item))
```

## Benefits

✅ **Organized:** All JSON files in one folder  
✅ **Efficient:** Batch processing of all files  
✅ **Smart:** Automatic duplicate detection  
✅ **Fast:** Skips geocoding for existing events  
✅ **Safe:** Won't create duplicate database entries  

