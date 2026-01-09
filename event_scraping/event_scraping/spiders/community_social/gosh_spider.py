import scrapy
import re
import time
from ..base_spider import BaseSpider
from ...items import EventScrapingItem
from scrapy.http import HtmlResponse


class GOSHSpider(BaseSpider):
    """Spider for https://www.gosh.org/get-involved/fundraising-events/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Uses Selenium to handle "Load More" button to load all events.
    """
    name = "gosh"
    category = "community_social"
    site_name = "gosh"
    allowed_domains = ["gosh.org"]
    start_urls = [
        "https://www.gosh.org/get-involved/fundraising-events/?category=climb&category=cycle&category=galas-and-special-events&category=row&category=run&category=skydive&category=trek&category=triathlon&category=your-challenge"
    ]
    
    # All categories and their keywords to include
    CATEGORY_KEYWORDS = {
        'Running': {
            'Road running': ['5k', '5km', '5 k', '5 km', '10k', '10km', '10 k', '10 km', 
                           'half marathon', 'half-marathon', 'halfmarathon', 'full marathon', 
                           'marathon', 'ultra', 'ultramarathon', 'ultra marathon', 'ultra-marathon'],
            'Endurance races': ['endurance', 'endurance race', 'long distance', 'ultra distance'],
            'Adventure running': ['adventure run', 'adventure running', 'adventure race'],
            'Trail running': ['trail run', 'trail running', 'trail race', 'trail', 'off road'],
            'Park runs': ['parkrun', 'park run', 'parkrun', 'parkrun'],
            'Charity runs': ['charity run', 'charity running', 'charity race', 'fundraising'],
            'Fun runs': ['fun run', 'fun running', 'fun race', 'fun run'],
            'Obstacle courses': ['obstacle course', 'obstacle race', 'obstacle run', 'mud run', 'mud race'],
            'Inflatable courses': ['inflatable', 'bouncy', 'inflatable course', 'inflatable race']
        },
        'Cycling': {
            'Sportives': ['sportive', 'sportif', 'cycling sportive', 'bike sportive'],
            'Time Trials': ['time trial', 'tt', 'cycling time trial', 'bike time trial'],
            'Road Races': ['road race', 'cycling race', 'bike race', 'road cycling'],
            'Cyclocross': ['cyclocross', 'cx', 'cross', 'cyclo-cross'],
            'Mountain Biking': ['mountain bike', 'mtb', 'mountain biking', 'off road cycling'],
            'Track Cycling': ['track cycling', 'velodrome', 'track race', 'track bike'],
            'Charity & Challenge Rides': ['charity ride', 'challenge ride', 'charity cycling', 'fundraising ride']
        },
        'Swimming': {
            'Open Water Swims': ['open water', 'open water swim', 'sea swim', 'lake swim', 'river swim'],
            'Pool Meets': ['pool meet', 'pool swimming', 'pool race', 'swimming meet'],
            'Swim Runs': ['swim run', 'swimrun', 'swim-run', 'aquathlon'],
            'Channel/Distance Swims': ['channel swim', 'distance swim', 'long distance swim', 'marathon swim']
        },
        'Functional Fitness': {
            'CrossFit Competitions': ['crossfit', 'cross fit', 'crossfit competition', 'crossfit games'],
            'Hyrox / DEKA FIT': ['hyrox', 'deka fit', 'deka', 'hyrox race', 'deka race'],
            'Obstacle Fitness Events': ['obstacle fitness', 'fitness obstacle', 'fitness challenge'],
            'Bootcamps & Fitness Challenges': ['bootcamp', 'fitness challenge', 'fitness bootcamp', 'challenge']
        },
        'Multi-Discipline': {
            'Triathlon': ['triathlon', 'tri', 'triathlete', 'triathlon race'],
            'Duathlon': ['duathlon', 'du', 'duathlete', 'duathlon race'],
            'Aquathlon': ['aquathlon', 'aqua', 'aquathlete', 'aquathlon race'],
            'Adventure Races': ['adventure race', 'multi sport', 'multi-sport', 'adventure challenge']
        }
    }
    
    # Flatten all keywords for easier matching
    ALL_KEYWORDS = []
    for category_group, subcategories in CATEGORY_KEYWORDS.items():
        for subcategory, keywords in subcategories.items():
            ALL_KEYWORDS.extend(keywords)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_events = set()  # Track seen events to avoid duplicates
        self.geocoding_cache = {}  # Cache geocoding results to avoid repeated API calls
        self.pages_visited = set()  # Track visited pages to avoid infinite loops
        self.total_items_scraped = 0  # Track total items scraped

    def parse(self, response):
        """Parse the main page and extract event links using Selenium to handle Load More button."""
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # Always try to use Selenium to handle "Load More" button for GOSH
        # GOSH uses dynamic loading, so we need Selenium to get all records
        self.logger.info("=" * 80)
        self.logger.info("ATTEMPTING TO LOAD ALL RECORDS USING SELENIUM")
        self.logger.info("=" * 80)
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException, NoSuchElementException
            
            self.logger.info("✓ Selenium imported successfully")
            
            # Check if Selenium is available and configured
            options = webdriver.ChromeOptions()
            options.add_argument('--headless=new')  # Use new headless mode
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Try to use webdriver-manager if available (auto-downloads ChromeDriver)
            try:
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager
                self.logger.info("Attempting to use webdriver-manager...")
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
                self.logger.info("✓ Using webdriver-manager for ChromeDriver")
            except ImportError as e:
                # Fallback to system ChromeDriver
                self.logger.info(f"webdriver-manager not available: {e}")
                self.logger.info("Attempting to use system ChromeDriver...")
                driver = webdriver.Chrome(options=options)
                self.logger.info("✓ Using system ChromeDriver")
            except Exception as e:
                self.logger.error(f"Failed to initialize ChromeDriver: {e}")
                raise
            
            try:
                self.logger.info(f"Loading URL with Selenium: {response.url}")
                driver.get(response.url)
                self.logger.info("✓ Page loaded")
                
                # Wait for page to load
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                self.logger.info("✓ Body element found")
                
                # Additional wait for dynamic content to initialize
                self.logger.info("Waiting for dynamic content to initialize...")
                time.sleep(5)  # Increased initial wait
                
                # Count initial event links - try multiple selectors for GOSH site
                event_selectors = [
                    'a[href*="/get-involved/fundraising-events/"]',
                    'a[href*="/fundraising-events/"]',
                    'a[href*="/event/"]',
                    '[class*="event"] a',
                    '[data-event-id] a',
                ]
                
                initial_event_count = 0
                for selector in event_selectors:
                    count = len(driver.find_elements(By.CSS_SELECTOR, selector))
                    if count > initial_event_count:
                        initial_event_count = count
                        self.logger.info(f"Found {initial_event_count} potential event links using selector: {selector}")
                
                self.logger.info(f"📊 INITIAL EVENT COUNT: {initial_event_count}")
                
                # Click "Load More" button until it's disabled or not visible
                max_clicks = 200  # Safety limit
                click_count = 0
                consecutive_no_change = 0
                last_event_count = initial_event_count
                
                self.logger.info("=" * 80)
                self.logger.info("STARTING 'LOAD MORE' BUTTON CLICKING PROCESS")
                self.logger.info("=" * 80)
                
                while click_count < max_clicks:
                    try:
                        self.logger.info(f"\n--- Attempt {click_count + 1} to find and click 'Load More' button ---")
                            
                        # Try multiple selectors for the "Load More" button on GOSH site
                        button_selectors = [
                            ('CSS', 'button[class*="load"]'),
                            ('CSS', 'button[class*="more"]'),
                            ('CSS', 'a[class*="load"]'),
                            ('CSS', 'a[class*="more"]'),
                            ('CSS', '[data-action="load-more"]'),
                            ('CSS', 'button[aria-label*="load more"]'),
                            ('CSS', 'a[aria-label*="load more"]'),
                        ]
                        
                        load_more_button = None
                        found_selector = None
                        
                        # Try CSS selectors first
                        for selector_type, selector in button_selectors:
                            try:
                                if selector_type == 'CSS':
                                    buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                                    for btn in buttons:
                                        btn_text = (btn.text or '').lower()
                                        btn_classes = (btn.get_attribute('class') or '').lower()
                                        # Check if button text contains "load more" or similar
                                        if ('load more' in btn_text or 'loadmore' in btn_text or 
                                            'load' in btn_classes or 'more' in btn_classes):
                                            # Check if button is visible and not disabled
                                            if btn.is_displayed():
                                                load_more_button = btn
                                                found_selector = selector
                                                self.logger.info(f"✓ Found button using CSS selector: {selector}")
                                                self.logger.info(f"  Button classes: {btn_classes}")
                                                self.logger.info(f"  Button text: {btn.text[:50] if btn.text else 'N/A'}")
                                                break
                                if load_more_button:
                                    break
                            except Exception as e:
                                self.logger.debug(f"Selector {selector} failed: {e}")
                                continue
                        
                        # Try XPath for text-based search
                        if not load_more_button:
                            try:
                                xpath_selectors = [
                                    '//button[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "load more")]',
                                    '//a[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "load more")]',
                                    '//button[contains(@class, "load") or contains(@class, "more")]',
                                    '//a[contains(@class, "load") or contains(@class, "more")]',
                                ]
                                for xpath in xpath_selectors:
                                    try:
                                        elements = driver.find_elements(By.XPATH, xpath)
                                        for elem in elements:
                                            if elem.is_displayed():
                                                load_more_button = elem
                                                found_selector = f"XPath: {xpath}"
                                                self.logger.info(f"✓ Found button using XPath: {xpath}")
                                                break
                                        if load_more_button:
                                            break
                                    except:
                                        continue
                            except Exception as e:
                                self.logger.debug(f"XPath search failed: {e}")
                        
                        # Try to find by text content (last resort)
                        if not load_more_button:
                            try:
                                self.logger.info("Searching all buttons and links by text content...")
                                buttons = driver.find_elements(By.TAG_NAME, "button")
                                links = driver.find_elements(By.TAG_NAME, "a")
                                all_elements = buttons + links
                                
                                for elem in all_elements:
                                    elem_text = (elem.text or '').lower()
                                    elem_classes = (elem.get_attribute('class') or '').lower()
                                    if (('load more' in elem_text or 'loadmore' in elem_text) and 
                                        elem.is_displayed()):
                                        load_more_button = elem
                                        found_selector = "Text-based search"
                                        self.logger.info(f"✓ Found button by text: '{elem.text}'")
                                        break
                            except Exception as e:
                                self.logger.debug(f"Text-based search failed: {e}")
                    
                        if not load_more_button:
                            self.logger.info("❌ 'Load More' button not found - all events may be loaded")
                            break
                        
                        # Get current event count before clicking
                        current_event_count = 0
                        for selector in event_selectors:
                            count = len(driver.find_elements(By.CSS_SELECTOR, selector))
                            if count > current_event_count:
                                current_event_count = count
                        self.logger.info(f"📊 Current event count before click: {current_event_count}")
                        
                        # Scroll to button to ensure it's in view
                        self.logger.info("Scrolling to button...")
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", load_more_button)
                        time.sleep(1)
                        
                        # Wait for button to be clickable
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable(load_more_button)
                            )
                            self.logger.info("✓ Button is clickable")
                        except Exception as e:
                            self.logger.warning(f"Button not clickable: {e}, trying anyway...")
                        
                        # Click the button using JavaScript (more reliable)
                        self.logger.info(f"🖱️  Clicking 'Load More' button (click #{click_count + 1})...")
                        try:
                            driver.execute_script("arguments[0].click();", load_more_button)
                            click_count += 1
                            self.logger.info(f"✓ Clicked 'Load More' button (click #{click_count})")
                        except Exception as e:
                            self.logger.error(f"❌ Failed to click button: {e}")
                            break
                        
                        # Wait for new content to load
                        self.logger.info("⏳ Waiting for new content to load...")
                        time.sleep(5)  # Wait for AJAX to complete
                        
                        # Wait for new content to appear (check if event count increased)
                        new_event_count = current_event_count
                        for wait_attempt in range(20):  # Wait up to 10 seconds for new content
                            time.sleep(0.5)
                            for selector in event_selectors:
                                count = len(driver.find_elements(By.CSS_SELECTOR, selector))
                                if count > new_event_count:
                                    new_event_count = count
                            if new_event_count > current_event_count:
                                self.logger.info(f"✅ New events loaded! Count increased from {current_event_count} to {new_event_count}")
                                last_event_count = new_event_count
                                consecutive_no_change = 0
                                break
                        
                        # If no new events after waiting, log it
                        if new_event_count == current_event_count:
                            self.logger.warning(f"⚠️  No new events loaded after click #{click_count}. Current count: {current_event_count}")
                            consecutive_no_change += 1
                            if consecutive_no_change >= 2:
                                self.logger.info("Event count not increasing. All records may be loaded.")
                                break
                        else:
                            consecutive_no_change = 0
                        
                        # Check if button is now disabled or not visible
                        try:
                            # Try to find the button again
                            if not load_more_button.is_displayed():
                                self.logger.info("'Load More' button no longer visible. All records likely loaded.")
                                break
                        except:
                            # Button might have been removed
                            self.logger.info("'Load More' button removed. All records likely loaded.")
                            break
                            
                    except TimeoutException:
                        # Button not found - might be all loaded
                        self.logger.info("Timeout waiting for 'Load More' button. All content may be loaded.")
                        break
                    except NoSuchElementException:
                        # No more "Load More" button found
                        self.logger.info("'Load More' button not found. All records may be loaded.")
                        break
                    except Exception as e:
                        self.logger.warning(f"Error clicking 'Load More' button: {e}")
                        # Continue anyway - might have loaded some content
                        time.sleep(2)
                        if click_count > 0:
                            # If we've clicked at least once, try to continue
                            continue
                        else:
                            break
                
                # Final count of events
                final_event_count = 0
                for selector in event_selectors:
                    count = len(driver.find_elements(By.CSS_SELECTOR, selector))
                    if count > final_event_count:
                        final_event_count = count
                self.logger.info(f"Final event count: {final_event_count} (started with {initial_event_count}, clicked {click_count} times)")
                
                # Get the fully loaded page source
                page_source = driver.page_source
                
                # Create a new Scrapy response with the fully loaded content
                response = HtmlResponse(
                    url=response.url,
                    body=page_source.encode('utf-8'),
                    encoding='utf-8'
                )
                
                self.logger.info(f"Page fully loaded with {click_count} 'Load More' clicks. Total events: {final_event_count}")
                
            finally:
                driver.quit()
                
        except ImportError:
            self.logger.warning("Selenium not available. Install with: pip install selenium")
            self.logger.warning("Also ensure ChromeDriver is installed and in PATH")
            self.logger.warning("Falling back to regular scraping (may miss dynamically loaded content)")
        except Exception as e:
            self.logger.error(f"Error using Selenium: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            self.logger.warning("Falling back to regular scraping")
        
        # Find event links on the page
        self.logger.info("Extracting event links from page...")
        
        # Try multiple selectors for event links on GOSH site
        event_link_selectors = [
            'a[href*="/get-involved/fundraising-events/"]::attr(href)',
            'a[href*="/fundraising-events/"]::attr(href)',
            'a[href*="/event/"]::attr(href)',
            '[class*="event"] a::attr(href)',
            '[data-event-id] a::attr(href)',
            'article a::attr(href)',
            '.event-card a::attr(href)',
            '[class*="Event"] a::attr(href)',
        ]
        
        event_links_found = 0
        seen_urls = set()
        
        for selector in event_link_selectors:
            links = response.css(selector).getall()
            for link in links:
                if link:
                    absolute_url = response.urljoin(link)
                    # Filter for actual event pages (not listing pages)
                    # Exclude the main listing page itself
                    if (('/get-involved/fundraising-events/' in absolute_url or 
                         '/fundraising-events/' in absolute_url or 
                         '/event/' in absolute_url) and 
                        absolute_url != response.url and
                        '?category=' not in absolute_url and
                        absolute_url not in seen_urls and 
                        absolute_url not in self.seen_events):
                        seen_urls.add(absolute_url)
                        self.seen_events.add(absolute_url)
                        event_links_found += 1
                        self.logger.info(f"Found event link #{event_links_found}: {absolute_url}")
                        try:
                            yield response.follow(link, self.parse_event, errback=self.handle_error)
                        except Exception as e:
                            self.logger.error(f"Error following event link {link}: {e}")
        
        self.logger.info(f"Total event links found: {event_links_found}")

    def parse_event(self, response):
        """Parse individual event pages to extract event details."""
        self.logger.info(f"Parsing event page: {response.url}")
        self.logger.debug(f"Event page status: {response.status}")
        
        item = EventScrapingItem()
        item['category'] = self.category
        item['site'] = self.site_name
        item['url'] = response.url

        # Try some common selectors for title
        title = (
            response.css('h1::text').get() or
            response.css('.event-title::text').get() or
            response.css('.title::text').get() or
            response.css('h1 *::text').get() or
            response.css('[class*="event-title"]::text').get() or
            response.css('[class*="title"]::text').get() or
            response.css('h2::text').get()
        )
        
        if title:
            title = title.strip()
        
        # Enhanced description extraction
        desc_parts = []
        
        # Try multiple selectors for description
        desc_selectors = [
            '.description *::text',
            '.event-description *::text',
            '.content *::text',
            'article *::text',
            '.event-details *::text',
            'p::text',
            '[class*="description"] *::text',
            '[class*="content"] *::text',
        ]
        
        for selector in desc_selectors:
            parts = response.css(selector).getall()
            if parts:
                desc_parts = [part.strip() for part in parts if part.strip()]
                if desc_parts:
                    self.logger.debug(f"Found description using selector: {selector}")
                    break
        
        # Enhanced date extraction
        date = None
        raw_date = None
        
        # Try multiple date selectors
        date_selectors = [
            ('[class="dtstart dtend"]::text', response.css('[class="dtstart dtend"]::text').get()),
            ('.date::text', response.css('.date::text').get()),
            ('time::attr(datetime)', response.css('time::attr(datetime)').get()),
            ('.event-date::text', response.css('.event-date::text').get()),
            ('[class*="date"]::text', response.css('[class*="date"]::text').get()),
            ('time::text', response.css('time::text').get()),
        ]
        
        for selector_name, selector_result in date_selectors:
            if selector_result:
                date = selector_result.strip()
                raw_date = date
                self.logger.debug(f"Found date using selector '{selector_name}': {date}")
                break
        
        # If still no date, try to extract from description
        if not date and desc_parts:
            desc_text = ' '.join(desc_parts)
            
            # Look for date patterns in description
            date_patterns = [
                r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                r'(\d{1,2}/\d{1,2}/\d{4})',
                r'(\d{1,2}-\d{1,2}-\d{4})',
                r'(\d{4}-\d{1,2}-\d{1,2})',
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, desc_text, re.IGNORECASE)
                if match:
                    date = match.group(1)
                    raw_date = date
                    self.logger.debug(f"Found date in description: {date}")
                    break
        
        # Store raw date before conversion
        if not raw_date:
            raw_date = date
        
        # Convert date to MM/DD/YYYY format
        if date:
            date = self.convert_date_format(date)
        
        # Short description extraction
        short_description = None
        if desc_parts:
            joined = '\n'.join(desc_parts).strip()
            short_description = joined.split('\n')[0]
            if len(short_description) > 200:
                short_description = short_description[:200].rsplit(' ', 1)[0] + '...'

        # Address extraction
        address = self.extract_address(response)
        
        # Clean address: remove "Location" text
        if address:
            address = self.remove_location_text(address)
        
        # Coordinates extraction
        coords = self.extract_coordinates(response)
        
        # Build event_data for database check before geocoding
        event_data = {
            'name': title,
            'date': date,
            'url': response.url
        }
        
        # Try to geocode address if available
        # Pass event_data to enable database check before geocoding
        if address:
            geocoded_coords = self.geocode_address(address, event_data=event_data)
            if geocoded_coords:
                if not coords:
                    coords = geocoded_coords
                    self.logger.debug(f"Coordinates from geocoding: {coords}")

        # Determine event category and subcategory
        event_category, event_subcategory = self.get_event_category(title, desc_parts) if title else (None, None)
        
        # Clean and set item fields
        cleaned_title = self.clean_text(title) if title else None
        parsed_date = date
        cleaned_description = self.clean_text(short_description) if short_description else None

        item['name'] = cleaned_title
        item['date'] = parsed_date
        item['raw_date'] = raw_date
        item['short_description'] = cleaned_description
        item['coordinates'] = coords
        item['address'] = address
        item['category'] = event_category
        item['subcategory'] = event_subcategory
        item['raw'] = {
            'title': title,
            'date': date,
            'desc_preview': short_description,
            'full_description': ' '.join(desc_parts) if desc_parts else None,
            'address': address,
            'coordinates': coords,
        }
        
        # Check for duplicate items based on name and date
        item_key = f"{item['name']}_{item['date']}"
        if item_key in self.seen_events:
            self.logger.debug(f"Skipping duplicate item: {item['name']}")
            return
        
        # Add to seen items
        self.seen_events.add(item_key)
        
        # Increment total items scraped
        self.total_items_scraped += 1
        
        # Log final item data
        self.logger.info(f"Event extracted - Name: {item['name'][:50] if item['name'] else 'N/A'}...")
        self.logger.debug(f"Full item data: {dict(item)}")
        
        yield item

    def is_target_race_type(self, title, description_parts):
        """Check if the event matches any of our target categories."""
        if not title:
            return False
        
        # Combine title and description for analysis
        full_text = title.lower()
        if description_parts:
            full_text += " " + " ".join(description_parts).lower()
        
        # Check for any matching keywords
        for keyword in self.ALL_KEYWORDS:
            if keyword.lower() in full_text:
                return True
        
        return False
    
    def get_event_category(self, title, description_parts):
        """Determine the specific category and subcategory for an event.
        
        For community_social spiders, always returns "Charity Events" for both
        category and subcategory to ensure correct database lookup.
        """
        # Always return "Charity Events" for both category and subcategory
        return "Charity Events", "Charity Events"

    def remove_location_text(self, address):
        """Remove 'Location' text and similar prefixes from address."""
        if not address:
            return address
        
        # Remove common location prefixes (case insensitive)
        patterns_to_remove = [
            r'^location\s*:?\s*-?\s*',
            r'^location\s+',
            r'\blocation\s*:?\s*-?\s*',
        ]
        
        cleaned_address = address
        for pattern in patterns_to_remove:
            cleaned_address = re.sub(pattern, '', cleaned_address, flags=re.IGNORECASE)
        
        # Clean up extra whitespace
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address).strip()
        
        return cleaned_address if cleaned_address else address

    def extract_address(self, response):
        """Extract full address from the page using multiple heuristics."""
        # Try multiple selectors for address
        address_selectors = [
            '.address::text',
            '.location::text', 
            '.venue::text',
            '.event-location::text',
            '.event-venue::text',
            '[class*="address"]::text',
            '[class*="location"]::text',
            '[class*="venue"]::text',
            '.event-info .location::text',
            '.event-details .address::text',
            'address::text',
            '.contact-info::text',
            '.event-contact::text',
        ]
        
        for selector in address_selectors:
            address = response.css(selector).get()
            if address and len(address.strip()) > 5:
                return self.clean_text(address)
        
        # Try to extract from description or content
        content_text = ' '.join(response.css('*::text').getall())
        
        # Look for UK postcode pattern
        postcode_pattern = r'[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}'
        postcode_match = re.search(postcode_pattern, content_text, re.IGNORECASE)
        
        if postcode_match:
            postcode = postcode_match.group()
            start = max(0, postcode_match.start() - 100)
            end = min(len(content_text), postcode_match.end() + 100)
            address_candidate = content_text[start:end].strip()
            
            if len(address_candidate) > 10:
                return self.clean_text(address_candidate)
        
        return None

    def convert_date_format(self, date_str):
        """Convert various date formats to MM/DD/YYYY format."""
        if not date_str:
            return None
        
        try:
            from datetime import datetime
            
            # Clean the date string
            date_str = date_str.strip()
            
            # Month name mappings
            month_names = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12',
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
            }
            
            # Try various patterns
            patterns = [
                (r'(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+(\d{4})', month_names),
                (r'(\d{1,2})\s+(\w+)\s+(\d{4})', month_names),
                (r'(\d{1,2})/(\d{1,2})/(\d{4})', None),  # DD/MM/YYYY
                (r'(\d{1,2})-(\d{1,2})-(\d{4})', None),  # DD-MM-YYYY
                (r'(\d{4})-(\d{1,2})-(\d{1,2})', None),  # YYYY-MM-DD
            ]
            
            for pattern, month_map in patterns:
                match = re.search(pattern, date_str, re.IGNORECASE)
                if match:
                    if month_map:
                        day, month_name, year = match.groups()
                        month_num = month_map.get(month_name.lower())
                        if month_num:
                            return f"{month_num}/{day.zfill(2)}/{year}"
                    else:
                        parts = match.groups()
                        if len(parts) == 3:
                            if pattern.startswith(r'(\d{4})'):  # YYYY-MM-DD
                                year, month, day = parts
                                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
                            else:  # DD/MM/YYYY or DD-MM-YYYY
                                day, month, year = parts
                                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Try datetime parsing
            formats = [
                '%d %B %Y', '%d %b %Y', '%d/%m/%Y', '%d-%m-%Y',
                '%Y-%m-%d', '%B %d, %Y', '%b %d, %Y',
            ]
            
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%m/%d/%Y')
                except ValueError:
                    continue
            
            return date_str
            
        except Exception as e:
            self.logger.error(f"Date conversion failed for '{date_str}': {e}")
            return date_str

    # geocode_address is inherited from BaseSpider, which uses the common function
    # that tries LocationIQ first (if API key is configured), then falls back to Nominatim.
    # It also checks the database before geocoding if event_data is provided and
    # check_db_before_geocoding is enabled.

    def extract_coordinates(self, response):
        """Attempt to find coordinates (lat, lon) in the page."""
        # Try meta tags
        lat = response.css('meta[property="place:location:latitude"]::attr(content)').get()
        lon = response.css('meta[property="place:location:longitude"]::attr(content)').get()
        
        if lat and lon:
            try:
                lat_f = float(lat.strip())
                lon_f = float(lon.strip())
                if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                    return {'lat': lat_f, 'lon': lon_f}
            except ValueError:
                pass
        
        # Try data attributes
        lat = response.css('[data-lat]::attr(data-lat)').get()
        lon = response.css('[data-lng]::attr(data-lng), [data-lon]::attr(data-lon)').get()
        
        if lat and lon:
            try:
                lat_f = float(lat.strip())
                lon_f = float(lon.strip())
                if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                    return {'lat': lat_f, 'lon': lon_f}
            except ValueError:
                pass
        
        # Try Google Maps links
        for href in response.css('a::attr(href)').getall():
            if href and 'google.com/maps' in href and '@' in href:
                try:
                    after_at = href.split('@', 1)[1]
                    coords_part = after_at.split(',', 2)
                    lat_val = float(coords_part[0])
                    lon_val = float(coords_part[1])
                    if 49 <= lat_val <= 61 and -8 <= lon_val <= 2:  # UK bounds
                        return {'lat': lat_val, 'lon': lon_val}
                except (ValueError, IndexError):
                    continue
        
        return None

