# Selenium Setup for RunThrough Spider

The RunThrough spider uses Selenium to handle the "Load More" button functionality.

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Install ChromeDriver:
   - Download from: https://chromedriver.chromium.org/downloads
   - Or use: `pip install webdriver-manager` (alternative approach)
   - Ensure ChromeDriver is in your PATH or in the same directory as your script

## Alternative: Using webdriver-manager (Recommended)

If you want automatic ChromeDriver management, you can install:
```bash
pip install webdriver-manager
```

Then the spider will automatically download and manage ChromeDriver.

## Notes

- The spider will automatically detect if Selenium is available
- If Selenium is not installed, it will fall back to regular scraping (may miss dynamically loaded content)
- The spider clicks the "Load More" button until it becomes disabled, ensuring all records are loaded

