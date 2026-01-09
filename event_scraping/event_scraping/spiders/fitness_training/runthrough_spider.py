from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class RunThroughSpider(BaseSpider):
    """Spider for https://www.runthrough.co.uk/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Filters for specific race types: 5K, 10K, Half Marathon, Ultras
    """
    name = "runthrough"
    category = "fitness_training"
    site_name = "runthrough"
    allowed_domains = ["runthrough.co.uk"]
    start_urls = [
        "https://www.runthrough.co.uk/",
        "https://www.runthrough.co.uk/events/",
        "https://www.runthrough.co.uk/all-events/",
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
        """Parse the main page and extract event links and pagination."""
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # Track this page as visited
        self.pages_visited.add(response.url)
        
        # First, try to find API endpoint for "Load More" functionality
        script_tags = response.css('script::text').getall()
        api_endpoints_found = []
        
        import re
        import json
        
        for script in script_tags:
            if not script:
                continue
                
            # Look for API endpoints that might load events
            api_patterns = [
                r'["\'](https?://[^"\']*api[^"\']*events[^"\']*)["\']',
                r'["\'](/[^"\']*api[^"\']*events[^"\']*)["\']',
                r'fetch\(["\']([^"\']*events[^"\']*)["\']',
                r'\.get\(["\']([^"\']*events[^"\']*)["\']',
                r'\.post\(["\']([^"\']*events[^"\']*)["\']',
                r'axios\.(get|post)\(["\']([^"\']*events[^"\']*)["\']',
                r'loadMore\(["\']([^"\']*)["\']',
                r'load.*more.*["\']([^"\']*)["\']',
            ]
            
            for pattern in api_patterns:
                matches = re.findall(pattern, script, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[-1] if match else None
                    if match and match not in api_endpoints_found:
                        api_endpoints_found.append(match)
                        self.logger.info(f"Found potential API endpoint: {match}")
        
        # Always try to use Selenium to handle "Load More" button for RunThrough
        # RunThrough uses dynamic loading, so we need Selenium to get all records
        self.logger.info("=" * 80)
        self.logger.info("ATTEMPTING TO LOAD ALL RECORDS USING SELENIUM")
        self.logger.info("=" * 80)
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException, NoSuchElementException
            from scrapy.http import HtmlResponse
            import time
            
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
                
                # Count initial event links - try multiple selectors
                event_selectors = [
                    'a[href*="/event/"]',
                    'a[href^="/"]',
                    '[class*="event"] a',
                ]
                
                initial_event_count = 0
                for selector in event_selectors:
                    count = len(driver.find_elements(By.CSS_SELECTOR, selector))
                    if count > initial_event_count:
                        initial_event_count = count
                        self.logger.info(f"Found {initial_event_count} potential event links using selector: {selector}")
                
                self.logger.info(f"📊 INITIAL EVENT COUNT: {initial_event_count}")
                
                # Check for "Load More" button immediately
                self.logger.info("Checking for 'Load More' button...")
                all_buttons = driver.find_elements(By.TAG_NAME, "button")
                self.logger.info(f"Found {len(all_buttons)} buttons on page")
                
                for i, btn in enumerate(all_buttons[:10]):  # Check first 10 buttons
                    btn_classes = btn.get_attribute('class') or ''
                    btn_text = btn.text or ''
                    self.logger.info(f"Button {i+1}: classes='{btn_classes}', text='{btn_text[:50]}'")
                
                # Click "Load More" button until it's disabled
                max_clicks = 200  # Increased safety limit
                click_count = 0
                consecutive_no_change = 0
                last_event_count = initial_event_count
                
                self.logger.info("=" * 80)
                self.logger.info("STARTING 'LOAD MORE' BUTTON CLICKING PROCESS")
                self.logger.info("=" * 80)
                
                while click_count < max_clicks:
                    try:
                        self.logger.info(f"\n--- Attempt {click_count + 1} to find and click 'Load More' button ---")
                            
                        # Try multiple selectors for the "Load More" button
                        button_selectors = [
                            ('CSS', 'button.button.button-primary:not(.disabled)'),
                            ('CSS', 'button.button-primary:not(.disabled)'),
                            ('CSS', 'button[class*="button"][class*="button-primary"]:not([class*="disabled"])'),
                            ('CSS', 'button.button.button-primary'),
                        ]
                        
                        load_more_button = None
                        found_selector = None
                        
                        # Try CSS selectors first
                        for selector_type, selector in button_selectors:
                            try:
                                if selector_type == 'CSS':
                                    buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                                    for btn in buttons:
                                        btn_classes = btn.get_attribute('class') or ''
                                        if 'disabled' not in btn_classes.lower():
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
                                    '//button[contains(@class, "button") and contains(@class, "button-primary") and not(contains(@class, "disabled"))]',
                                    '//button[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "load more")]',
                                    '//button[contains(@class, "button-primary")]',
                                ]
                                for xpath in xpath_selectors:
                                    try:
                                        buttons = driver.find_elements(By.XPATH, xpath)
                                        for btn in buttons:
                                            btn_classes = btn.get_attribute('class') or ''
                                            if 'disabled' not in btn_classes.lower():
                                                load_more_button = btn
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
                                self.logger.info("Searching all buttons by text content...")
                                buttons = driver.find_elements(By.TAG_NAME, "button")
                                for btn in buttons:
                                    btn_text = (btn.text or '').lower()
                                    btn_classes = (btn.get_attribute('class') or '').lower()
                                    if ('load more' in btn_text or 'loadmore' in btn_text) and 'disabled' not in btn_classes:
                                        load_more_button = btn
                                        found_selector = "Text-based search"
                                        self.logger.info(f"✓ Found button by text: '{btn.text}'")
                                        break
                            except Exception as e:
                                self.logger.debug(f"Text-based search failed: {e}")
                    
                        if not load_more_button:
                            self.logger.warning("❌ 'Load More' button not found with any selector")
                            # Check if button is disabled
                            try:
                                disabled_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.button.button-primary.disabled')
                                if disabled_buttons:
                                    self.logger.info("✓ Found disabled 'Load More' button - all records may be loaded")
                                    break
                            except:
                                pass
                            break
                        
                        # Check if button is displayed
                        if not load_more_button.is_displayed():
                            self.logger.warning("❌ 'Load More' button found but not displayed")
                            # Check if it's disabled
                            try:
                                disabled_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.button.button-primary.disabled')
                                if disabled_buttons:
                                    self.logger.info("✓ Button is disabled - all records may be loaded")
                                    break
                            except:
                                pass
                            break
                        
                        # Get current event count before clicking
                        current_event_count = len(driver.find_elements(By.CSS_SELECTOR, 'a[href*="/event/"]'))
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
                        
                        # Wait for new content to load - wait longer for AJAX
                        self.logger.info("⏳ Waiting for new content to load...")
                        time.sleep(5)  # Increased wait time
                        
                        # Wait for new content to appear (check if event count increased)
                        new_event_count = current_event_count
                        for wait_attempt in range(20):  # Wait up to 10 seconds for new content
                            time.sleep(0.5)
                            new_event_count = len(driver.find_elements(By.CSS_SELECTOR, 'a[href*="/event/"]'))
                            if new_event_count > current_event_count:
                                self.logger.info(f"✅ New events loaded! Count increased from {current_event_count} to {new_event_count}")
                                last_event_count = new_event_count
                                consecutive_no_change = 0
                                break
                        
                        # If no new events after waiting, log it
                        if new_event_count == current_event_count:
                            self.logger.warning(f"⚠️  No new events loaded after click #{click_count}. Current count: {current_event_count}")
                            consecutive_no_change += 1
                        else:
                            consecutive_no_change = 0
                        
                        # Check if button is now disabled
                        try:
                            disabled_button = driver.find_element(
                                By.CSS_SELECTOR,
                                'button.button.button-primary.disabled'
                            )
                            if disabled_button and disabled_button.is_displayed():
                                self.logger.info("'Load More' button is now disabled. All records loaded.")
                                time.sleep(2)  # Final wait for any remaining content
                                break
                        except NoSuchElementException:
                            # Check if button still exists and is not disabled
                            try:
                                active_button = driver.find_element(By.CSS_SELECTOR, 'button.button.button-primary:not(.disabled)')
                                if not active_button or not active_button.is_displayed():
                                    self.logger.info("'Load More' button no longer visible. All records likely loaded.")
                                    break
                            except:
                                # Button might have been removed
                                self.logger.info("'Load More' button removed. All records likely loaded.")
                                break
                        
                        # Check if event count didn't change (might mean all loaded)
                        if new_event_count == current_event_count:
                            consecutive_no_change += 1
                            if consecutive_no_change >= 2:
                                self.logger.info("Event count not increasing. All records may be loaded.")
                                # Double-check if button is disabled
                                try:
                                    disabled = driver.find_element(By.CSS_SELECTOR, 'button.button.button-primary.disabled')
                                    if disabled:
                                        break
                                except:
                                    pass
                        else:
                            consecutive_no_change = 0
                            
                    except TimeoutException:
                        # Button not found - might be all loaded
                        self.logger.info("Timeout waiting for 'Load More' button. Checking if all content is loaded...")
                        try:
                            disabled_button = driver.find_element(By.CSS_SELECTOR, 'button.button.button-primary.disabled')
                            if disabled_button:
                                self.logger.info("Button is disabled. All records loaded.")
                                break
                        except:
                            pass
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
                final_event_count = len(driver.find_elements(By.CSS_SELECTOR, 'a[href*="/event/"]'))
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
        
        # Find event links on the homepage or listings
        all_links = response.css('a::attr(href)').getall()
        event_links_found = 0
        event_links = []
        
        # Common patterns for RunThrough event URLs
        event_patterns = ['/events/', '/event/', '/all-events/']
        
        for href in all_links:
            if href:
                # Convert to absolute URL
                absolute_url = response.urljoin(href)
                
                # Check if this is an event link
                # RunThrough URLs typically don't have /event/ in them, but are direct event pages
                # Look for URLs that are event pages (not navigation, not filters, etc.)
                is_event_link = False
                
                # Check if URL contains event-like patterns but exclude common non-event pages
                if any(pattern in href for pattern in event_patterns):
                    is_event_link = True
                elif href.startswith('/') and not href.startswith('/events/') and not href.startswith('/all-events/'):
                    # Check if it's a direct event page (typically slug-based URLs)
                    # Exclude common non-event pages
                    excluded_patterns = ['/about', '/contact', '/faq', '/login', '/signup', '/cart', 
                                        '/wishlist', '/results', '/photos', '/videos', '/blog', 
                                        '/news', '/terms', '/privacy', '/cookie', '/search',
                                        '/distances/', '/regions/', '/cities/', '/venues/', '/series/',
                                        '/gift', '/membership', '/calendar', '/race-info', '/corporate',
                                        '/foundation', '/kit', '/coach', '/retreats', '/charity',
                                        '/partners', '/volunteer', '/careers', '/sustainability',
                                        '/community', '/pacing', '/prizes', '/club', '/injury',
                                        '/tips', '/fundraising', '/partners']
                    
                    # If it's a slug-based URL and not excluded, it might be an event
                    if not any(excluded in href for excluded in excluded_patterns):
                        # Check if URL looks like an event slug (contains common race terms)
                        event_indicators = ['run', 'race', 'marathon', '10k', '5k', 'half', 'ultra', 
                                          'trail', 'triathlon', 'duathlon', 'swim']
                        if any(indicator in href.lower() for indicator in event_indicators):
                            is_event_link = True
                
                if is_event_link and absolute_url not in self.seen_events:
                    # Add to seen events
                    self.seen_events.add(absolute_url)
                    
                    event_links_found += 1
                    event_links.append(absolute_url)
                    self.logger.info(f"Found event link #{event_links_found}: {absolute_url}")
                    
                    try:
                        yield response.follow(href, self.parse_event, errback=self.handle_error)
                    except Exception as e:
                        self.logger.error(f"Error following event link {href}: {e}")
        
        self.logger.info(f"Total event links found: {event_links_found}")
        
        # Try to find event cards/data directly on the page
        event_card_selectors = [
            'a[href*="/events/"]',
            'a[href*="/event/"]',
            '.event-card a::attr(href)',
            '[class*="event"] a::attr(href)',
            '[class*="Event"] a::attr(href)',
            '.event-item a::attr(href)',
            'article[class*="event"] a::attr(href)',
            'div[class*="event-card"] a::attr(href)',
            '[data-event-id] a::attr(href)',
            '[data-testid*="event"] a::attr(href)',
            '[data-cy*="event"] a::attr(href)',
            '.slide a::attr(href)',
            '[class*="slide"] a::attr(href)',
        ]
        
        for selector in event_card_selectors:
            card_links = response.css(selector).getall()
            if card_links:
                self.logger.info(f"Found {len(card_links)} links with selector '{selector}'")
                for link in card_links:
                    if link:
                        absolute_url = response.urljoin(link)
                        if absolute_url not in self.seen_events:
                            self.seen_events.add(absolute_url)
                            try:
                                yield response.follow(link, self.parse_event, errback=self.handle_error)
                            except Exception as e:
                                self.logger.error(f"Error following card link {link}: {e}")
        
        # Pagination
        pagination_links_raw = response.css('[class*="paging"] a::attr(href)').getall()
        
        # If no links found with primary selector, try fallback selectors
        if not pagination_links_raw:
            pagination_links_raw = (
                response.css('a.next::attr(href), a.pagination-next::attr(href)').getall() or
                response.css('a[rel="next"]::attr(href)').getall() or
                response.css('.pagination a::attr(href)').getall() or
                response.css('.page-numbers a::attr(href)').getall() or
                response.css('[class*="next"] a::attr(href)').getall() or
                response.css('[class*="pagination"] a::attr(href)').getall() or
                response.css('a[aria-label*="next"]::attr(href), a[aria-label*="Next"]::attr(href)').getall()
            )
        
        # Convert to absolute URLs and remove duplicates
        pagination_links = []
        seen_urls = set()
        current_url = response.url
        
        for link in pagination_links_raw:
            if link:
                absolute_url = response.urljoin(link)
                if (absolute_url != current_url and 
                    absolute_url not in seen_urls and 
                    absolute_url not in self.pages_visited):
                    pagination_links.append(absolute_url)
                    seen_urls.add(absolute_url)
        
        self.logger.info(f"Pagination links found: {len(pagination_links)}")
        
        for next_page in pagination_links:
            if next_page:
                self.logger.info(f"Following pagination: {next_page}")
                try:
                    yield response.follow(next_page, self.parse, errback=self.handle_error)
                except Exception as e:
                    self.logger.error(f"Error following pagination link {next_page}: {e}")
        
        # Check if we should continue scraping (optional limit)
        if self.total_items_scraped >= 1000:  # Safety limit to prevent infinite scraping
            self.logger.warning("Reached safety limit of 1000 items. Stopping spider.")
            return

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
            response.css('[class*="title"]::text').get()
        )
        
        # PRIORITY: Extract from receipt__description receipt__description_flex class
        receipt_container = response.css('.receipt__description.receipt__description_flex')
        receipt_text = None
        if receipt_container and len(receipt_container) > 0:
            receipt_text = ' '.join(receipt_container.css('*::text').getall())
            self.logger.debug(f"Found receipt__description content: {receipt_text[:200] if receipt_text else 'None'}...")
        
        # Enhanced description extraction - prioritize receipt__description
        desc_parts = []
        if receipt_container and len(receipt_container) > 0:
            desc_parts = receipt_container.css('*::text').getall()
            if not desc_parts:
                desc_parts = receipt_container.css('::text').getall()
        
        # Fallback to other selectors if receipt__description not found
        if not desc_parts:
            desc_parts = (
                response.css('.description *::text, .event-description *::text, .content *::text').getall() or
                response.css('article *::text, .event-details *::text').getall() or
                response.css('p::text, .text::text').getall() or
                response.css('[class*="description"] *::text').getall()
            )
        
        # Enhanced date extraction - prioritize receipt__description
        date = None
        used_selector = None
        
        # First, try to extract date from receipt__description
        if receipt_container and len(receipt_container) > 0:
            # Look for date patterns in receipt__description
            import re
            if receipt_text:
                # RunThrough specific format: "Sat, 15th Nov, 2025" or similar
                date_patterns = [
                    r'([A-Za-z]{3},\s*\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3},?\s+\d{4})',
                    r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                    r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                    r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                    r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                    r'(\d{1,2}/\d{1,2}/\d{4})',
                    r'(\d{1,2}-\d{1,2}-\d{4})',
                    r'(\d{4}-\d{1,2}-\d{1,2})',
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, receipt_text, re.IGNORECASE)
                    if match:
                        date = match.group(1)
                        used_selector = 'receipt__description (regex)'
                        self.logger.debug(f"Found date in receipt__description: {date}")
                        break
                
                # Also try direct CSS selectors within receipt__description
                if not date:
                    date_selectors_in_receipt = [
                        ('.date::text', receipt_container.css('.date::text').get()),
                        ('time::attr(datetime)', receipt_container.css('time::attr(datetime)').get()),
                        ('[class*="date"]::text', receipt_container.css('[class*="date"]::text').get()),
                    ]
                    for selector_name, selector_result in date_selectors_in_receipt:
                        if selector_result:
                            date = selector_result
                            used_selector = f'receipt__description ({selector_name})'
                            self.logger.debug(f"Found date using selector '{selector_name}': {date}")
                            break
        
        # Fallback to other date selectors if not found in receipt__description
        if not date:
            date_selectors = [
                ('[class="dtstart dtend"]::text', response.css('[class="dtstart dtend"]::text').get()),
                ('.date::text', response.css('.date::text').get()),
                ('time::attr(datetime)', response.css('time::attr(datetime)').get()),
                ('.event-date::text', response.css('.event-date::text').get()),
                ('[class*="date"]::text', response.css('[class*="date"]::text').get()),
                ('[class*="time"]::text', response.css('[class*="time"]::text').get()),
                ('.event-info::text', response.css('.event-info::text').get()),
                ('.event-details::text', response.css('.event-details::text').get()),
                ('.event-meta::text', response.css('.event-meta::text').get()),
                ('.event-header::text', response.css('.event-header::text').get()),
                ('h2::text, h3::text', response.css('h2::text, h3::text').get()),
                ('.event-summary::text', response.css('.event-summary::text').get()),
                ('[class*="event-date"]::text', response.css('[class*="event-date"]::text').get()),
            ]
            
            for selector_name, selector_result in date_selectors:
                if selector_result:
                    date = selector_result
                    used_selector = selector_name
                    self.logger.debug(f"Found date using selector '{selector_name}': {date}")
                    break
        
        # If still no date, try to extract from description
        if not date and desc_parts:
            import re
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
                # RunThrough specific format: "Sat, 15th Nov, 2025"
                r'([A-Za-z]{3},\s*\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3},\s*\d{4})',
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, desc_text, re.IGNORECASE)
                if match:
                    date = match.group(1)
                    self.logger.debug(f"Found date in description: {date}")
                    break
        
        # Store raw date before conversion
        raw_date = date
        
        # Convert date to MM/DD/YYYY format
        if date:
            date = self.convert_date_format(date)
        
        # Check if this event matches our target race types
        if not self.is_target_race_type(title, desc_parts):
            self.logger.info(f"Event does not match target race types - skipping: {title}")
            self.logger.debug(f"Title: {title}, Description preview: {desc_parts[:3] if desc_parts else 'None'}")
            return

        # Short description: extract from event__details container text-center mt-3
        short_description = None
        # Try multiple selector combinations for event__details
        event_details_container = None
        
        # Try exact class combination first
        event_details_container = response.css('.event__details.container.text-center.mt-3')
        if not event_details_container or len(event_details_container) == 0:
            # Try with space-separated classes (some CSS frameworks use spaces)
            event_details_container = response.css('[class*="event__details"][class*="container"][class*="text-center"][class*="mt-3"]')
        if not event_details_container or len(event_details_container) == 0:
            # Try with different combinations
            event_details_container = response.css('.event__details.container.text-center')
        if not event_details_container or len(event_details_container) == 0:
            event_details_container = response.css('.event__details.container')
        if not event_details_container or len(event_details_container) == 0:
            event_details_container = response.css('.event__details')
        if not event_details_container or len(event_details_container) == 0:
            event_details_container = response.css('[class*="event__details"]')
        
        if event_details_container and len(event_details_container) > 0:
            # Get first element from SelectorList
            container = event_details_container[0]
            
            # Extract text from event__details container
            event_details_text = ' '.join(container.css('*::text').getall())
            if not event_details_text:
                event_details_text = ' '.join(container.css('::text').getall())
            
            if event_details_text:
                import re
                # Remove extra whitespace
                cleaned_text = ' '.join(event_details_text.split())
                # Take first 200 chars or first sentence
                sentences = re.split(r'[.!?]\s+', cleaned_text)
                if sentences:
                    short_description = sentences[0]
                    if len(short_description) > 200:
                        short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
                else:
                    short_description = cleaned_text[:200] + '...' if len(cleaned_text) > 200 else cleaned_text
                self.logger.debug(f"Found short_description from event__details: {short_description[:100]}...")
        
        # Fallback to receipt_text or desc_parts if event__details not found
        if not short_description:
            if receipt_text:
                # Use receipt_text for description, clean it up
                import re
                # Remove extra whitespace
                cleaned_receipt = ' '.join(receipt_text.split())
                # Take first 200 chars or first sentence
                sentences = re.split(r'[.!?]\s+', cleaned_receipt)
                if sentences:
                    short_description = sentences[0]
                    if len(short_description) > 200:
                        short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
                else:
                    short_description = cleaned_receipt[:200] + '...' if len(cleaned_receipt) > 200 else cleaned_receipt
            elif desc_parts:
                joined = '\n'.join(desc_parts).strip()
                short_description = joined.split('\n')[0]
                if len(short_description) > 200:
                    short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
            
            # If still no date, try to extract from short description
            if not date and short_description:
                import re
                
                # Look for date patterns in short description
                date_patterns = [
                    r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                    r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                    r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                    r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                    r'(\d{1,2}/\d{1,2}/\d{4})',
                    r'(\d{1,2}-\d{1,2}-\d{4})',
                    r'(\d{4}-\d{1,2}-\d{1,2})',
                    r'([A-Za-z]{3},\s*\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3},\s*\d{4})',
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, short_description, re.IGNORECASE)
                    if match:
                        date = match.group(1)
                        raw_date = date
                        date = self.convert_date_format(date)
                        break

        # Address extraction - prioritize receipt__description
        address = None
        if receipt_container and len(receipt_container) > 0:
            # Try to extract address from receipt__description
            address = self.extract_address_from_receipt(receipt_container, receipt_text)
        
        # Fallback to general address extraction
        if not address:
            address = self.extract_address(response)
        
        # Clean address: remove "Location" text
        if address:
            address = self.remove_location_text(address)
        
        # Coordinates extraction using multiple heuristics
        coords = self.extract_coordinates(response)
        
        # Build event_data for database check before geocoding
        event_data = {
            'name': title,
            'date': date,
            'url': response.url
        }
        
        # Always try to geocode address if available (will use geocoded result if no coordinates found)
        # Pass event_data to enable database check before geocoding
        if address:
            geocoded_coords = self.geocode_address(address, event_data=event_data)
            if geocoded_coords:
                if not coords:
                    # Use geocoded coordinates if we don't have any
                    coords = geocoded_coords
                    self.logger.debug(f"Coordinates from geocoding: {coords}")
                else:
                    # Log that we have both (but use the extracted ones)
                    self.logger.debug(f"Coordinates found from page, geocoded coordinates also available: {geocoded_coords}")

        # Determine event category and subcategory
        event_category, event_subcategory = self.get_event_category(title, desc_parts)
        
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
        """Determine the specific category and subcategory for an event."""
        if not title:
            return None, None
        
        # Combine title and description for analysis
        full_text = title.lower()
        if description_parts:
            full_text += " " + " ".join(description_parts).lower()
        
        # Check each category group and subcategory
        for category_group, subcategories in self.CATEGORY_KEYWORDS.items():
            for subcategory, keywords in subcategories.items():
                for keyword in keywords:
                    if keyword.lower() in full_text:
                        self.logger.debug(f"Event categorized as: {category_group} -> {subcategory}")
                        return category_group, subcategory
        
        return "Other", "General"

    def extract_address_from_receipt(self, receipt_container, receipt_text):
        """Extract address/location from receipt__description container.
        
        The receipt container has items in order:
        1. First: Date
        2. Second: Date/Time
        3. Third: Location (address)
        """
        if not receipt_container or len(receipt_container) == 0 or not receipt_text:
            return None
        
        import re
        
        # Method 1: Try to get child elements/text nodes and extract the 3rd one
        # Get the first element from SelectorList, then get its children
        if len(receipt_container) > 0:
            container_element = receipt_container[0]
            # Use xpath to get direct children (more reliable than CSS '> *')
            child_elements = container_element.xpath('./*')
            if len(child_elements) >= 3:
                # Get the 3rd child element (index 2)
                third_element_selector = child_elements[2]
                location_text = ' '.join(third_element_selector.css('*::text').getall())
                if not location_text:
                    location_text = ' '.join(third_element_selector.css('::text').getall())
                if location_text and len(location_text.strip()) > 5:
                    self.logger.debug(f"Found location from 3rd child element: {location_text[:100]}")
                    return self.clean_text(location_text.strip())
        
        # Method 2: Try to split receipt text by common separators and get 3rd part
        # Split by newlines, or common separators
        receipt_lines = receipt_text.split('\n')
        receipt_lines = [line.strip() for line in receipt_lines if line.strip()]
        if len(receipt_lines) >= 3:
            location = receipt_lines[2]
            if len(location) > 5:
                self.logger.debug(f"Found location from 3rd line: {location[:100]}")
                return self.clean_text(location)
        
        # Method 3: Extract all text nodes and get the 3rd non-empty one
        all_text_nodes = receipt_container.css('*::text').getall()
        all_text_nodes = [t.strip() for t in all_text_nodes if t.strip() and len(t.strip()) > 2]
        if len(all_text_nodes) >= 3:
            location = all_text_nodes[2]
            if len(location) > 5:
                self.logger.debug(f"Found location from 3rd text node: {location[:100]}")
                return self.clean_text(location)
        
        # Method 4: Try to find location-specific elements within receipt__description
        # Look for common location indicators
        location_selectors = [
            '[class*="location"]::text',
            '[class*="address"]::text',
            '[class*="venue"]::text',
            '[class*="place"]::text',
            '.location::text',
            '.address::text',
            '.venue::text',
            '.place::text',
        ]
        
        for selector in location_selectors:
            location_text = receipt_container.css(selector).getall()
            if location_text:
                # Join multiple location parts
                location = ' '.join([t.strip() for t in location_text if t.strip()])
                if len(location) > 5:
                    self.logger.debug(f"Found location from selector '{selector}': {location}")
                    return self.clean_text(location)
        
        # Extract all text from receipt container and look for location patterns
        all_receipt_text = ' '.join(receipt_container.css('*::text').getall())
        if not all_receipt_text:
            all_receipt_text = receipt_text
        
        # Look for UK postcode pattern (strong indicator of location)
        postcode_pattern = r'[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}'
        postcode_match = re.search(postcode_pattern, all_receipt_text, re.IGNORECASE)
        
        if postcode_match:
            postcode = postcode_match.group()
            # Extract context around postcode (location information)
            start = max(0, postcode_match.start() - 150)
            end = min(len(all_receipt_text), postcode_match.end() + 50)
            address_candidate = all_receipt_text[start:end].strip()
            
            # Clean up the address candidate
            # Remove common prefixes/suffixes that aren't part of address
            address_candidate = re.sub(r'^(.*?)(?:location|venue|address|at|in|near)\s*:?\s*', '', address_candidate, flags=re.IGNORECASE)
            address_candidate = re.sub(r'\s+', ' ', address_candidate).strip()
            
            if len(address_candidate) > 10:
                self.logger.debug(f"Found location from postcode context: {address_candidate}")
                return self.clean_text(address_candidate)
        
        # Look for location patterns: "Location:", "Venue:", "Address:", "At:", etc.
        location_patterns = [
            r'(?:location|venue|address|place|at|in)\s*:?\s*([^,\n]{10,100})',
            r'(?:located|situated|held)\s+(?:at|in|on)\s+([^,\n]{10,100})',
        ]
        
        for pattern in location_patterns:
            matches = re.finditer(pattern, all_receipt_text, re.IGNORECASE)
            for match in matches:
                location = match.group(1).strip()
                # Clean up common suffixes
                location = re.sub(r'\s*(?:,|\.|$).*$', '', location).strip()
                if len(location) > 10 and len(location) < 200:
                    self.logger.debug(f"Found location from pattern '{pattern}': {location}")
                    return self.clean_text(location)
        
        # Look for common address keywords and extract surrounding text
        address_keywords = ['street', 'road', 'avenue', 'lane', 'close', 'drive', 'way', 'place', 'park', 'venue', 'stadium', 'arena', 'hall', 'centre', 'center']
        for keyword in address_keywords:
            if keyword in all_receipt_text.lower():
                keyword_pos = all_receipt_text.lower().find(keyword)
                if keyword_pos > 0:
                    # Extract text before and after keyword
                    start = max(0, keyword_pos - 80)
                    end = min(len(all_receipt_text), keyword_pos + len(keyword) + 80)
                    address_candidate = all_receipt_text[start:end].strip()
                    
                    # Try to extract a complete address phrase
                    # Look for sentence boundaries or common separators
                    address_candidate = re.sub(r'^[^a-zA-Z0-9]*', '', address_candidate)
                    address_candidate = re.sub(r'[^a-zA-Z0-9\s,.-]*$', '', address_candidate)
                    address_candidate = re.sub(r'\s+', ' ', address_candidate).strip()
                    
                    if len(address_candidate) > 10 and len(address_candidate) < 200:
                        self.logger.debug(f"Found location from keyword '{keyword}': {address_candidate}")
                        return self.clean_text(address_candidate)
        
        # If we have receipt_text but no specific location found, return the full receipt text
        # (it might contain location info)
        if receipt_text and len(receipt_text) > 10 and len(receipt_text) < 500:
            cleaned = ' '.join(receipt_text.split())
            self.logger.debug(f"Using full receipt text as location: {cleaned[:100]}...")
            return self.clean_text(cleaned)
        
        return None

    def remove_location_text(self, address):
        """Remove 'Location' text and similar prefixes from address."""
        if not address:
            return address
        
        import re
        
        # Remove common location prefixes (case insensitive)
        # Patterns to remove: "Location:", "Location", "Location -", etc.
        patterns_to_remove = [
            r'^location\s*:?\s*-?\s*',  # "Location:", "Location -", "Location:"
            r'^location\s+',            # "Location " at start
            r'\blocation\s*:?\s*-?\s*', # "Location:" anywhere
        ]
        
        cleaned_address = address
        for pattern in patterns_to_remove:
            cleaned_address = re.sub(pattern, '', cleaned_address, flags=re.IGNORECASE)
        
        # Clean up extra whitespace
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address).strip()
        
        # If we removed something, return cleaned version, otherwise return original
        if cleaned_address != address:
            self.logger.debug(f"Removed 'Location' text from address: '{address}' -> '{cleaned_address}'")
        
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
            '[class*="event-location"]::text',
            '[class*="event-venue"]::text',
        ]
        
        for selector in address_selectors:
            address = response.css(selector).get()
            if address and len(address.strip()) > 5:
                return self.clean_text(address)
        
        # Try to extract from description or content
        content_text = ' '.join(response.css('*::text').getall())
        
        # Look for common address patterns
        import re
        
        # UK postcode pattern
        postcode_pattern = r'[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}'
        postcode_match = re.search(postcode_pattern, content_text, re.IGNORECASE)
        
        if postcode_match:
            postcode = postcode_match.group()
            start = max(0, postcode_match.start() - 100)
            end = min(len(content_text), postcode_match.end() + 100)
            address_candidate = content_text[start:end].strip()
            
            if len(address_candidate) > 10:
                return self.clean_text(address_candidate)
        
        # Look for common address keywords
        address_keywords = ['street', 'road', 'avenue', 'lane', 'close', 'drive', 'way', 'place', 'park']
        for keyword in address_keywords:
            if keyword in content_text.lower():
                keyword_pos = content_text.lower().find(keyword)
                if keyword_pos > 0:
                    start = max(0, keyword_pos - 50)
                    end = min(len(content_text), keyword_pos + 100)
                    address_candidate = content_text[start:end].strip()
                    
                    if len(address_candidate) > 10:
                        return self.clean_text(address_candidate)
        
        return None

    def convert_date_format(self, date_str):
        """Convert various date formats to MM/DD/YYYY format."""
        if not date_str:
            return None
        
        try:
            import re
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
            
            # Pattern 1: RunThrough format "Sat, 15th Nov, 2025" or "15th Nov, 2025"
            pattern1 = r'(?:[A-Za-z]{3},\s*)?(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+(\d{4})'
            match1 = re.search(pattern1, date_str, re.IGNORECASE)
            if match1:
                day, month_name, year = match1.groups()
                month_num = month_names.get(month_name.lower())
                if month_num:
                    return f"{month_num}/{day.zfill(2)}/{year}"
            
            # Pattern 2: DD Month YYYY (e.g., "30 October 2025")
            pattern2 = r'(\d{1,2})\s+(\w+)\s+(\d{4})'
            match2 = re.search(pattern2, date_str, re.IGNORECASE)
            if match2:
                day, month_name, year = match2.groups()
                month_num = month_names.get(month_name.lower())
                if month_num:
                    return f"{month_num}/{day.zfill(2)}/{year}"
            
            # Pattern 3: DD/MM/YYYY (e.g., "30/10/2025")
            pattern3 = r'(\d{1,2})/(\d{1,2})/(\d{4})'
            match3 = re.search(pattern3, date_str)
            if match3:
                day, month, year = match3.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Pattern 4: DD-MM-YYYY (e.g., "30-10-2025")
            pattern4 = r'(\d{1,2})-(\d{1,2})-(\d{4})'
            match4 = re.search(pattern4, date_str)
            if match4:
                day, month, year = match4.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Pattern 5: YYYY-MM-DD (e.g., "2025-10-30")
            pattern5 = r'(\d{4})-(\d{1,2})-(\d{1,2})'
            match5 = re.search(pattern5, date_str)
            if match5:
                year, month, day = match5.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Pattern 6: MM/DD/YYYY (already in correct format)
            pattern6 = r'(\d{1,2})/(\d{1,2})/(\d{4})'
            match6 = re.search(pattern6, date_str)
            if match6:
                month, day, year = match6.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Try to parse with datetime and convert
            try:
                formats = [
                    '%d %B %Y', '%d %b %Y', '%d/%m/%Y', '%d-%m-%Y',
                    '%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y', '%b %d, %Y',
                    '%d %B %Y', '%d %b %Y', '%a, %d %b %Y', '%A, %d %B %Y',
                    '%dth %b %Y', '%dst %b %Y', '%dnd %b %Y', '%drd %b %Y',
                ]
                
                for fmt in formats:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        return parsed_date.strftime('%m/%d/%Y')
                    except ValueError:
                        continue
            except Exception:
                pass
            
            # If all else fails, return the original string
            return date_str
            
        except Exception as e:
            self.logger.error(f"Date conversion failed for '{date_str}': {e}")
            return date_str

    # geocode_address is inherited from BaseSpider, which uses the common function
    # that tries LocationIQ first (if API key is configured), then falls back to Nominatim.
    # It also checks the database before geocoding if event_data is provided and
    # check_db_before_geocoding is enabled.

    def extract_coordinates(self, response):
        """Attempt to find coordinates (lat, lon) in the page using several heuristics.

        Heuristics checked in order:
        - meta tags (og:latitude / og:longitude or geo.position)
        - elements with data-lat / data-lng attributes
        - links to Google Maps / Bing Maps containing @lat,lon
        - microformats / schema.org geo properties
        - embedded maps and iframes
        Returns dict {'lat': float, 'lon': float} or None
        """
        # 1) meta tags
        lat = response.css('meta[property="place:location:latitude"]::attr(content)').get()
        lon = response.css('meta[property="place:location:longitude"]::attr(content)').get()
        if not lat or not lon:
            lat = response.css('meta[name="geo.position"]::attr(content)').get()
            if lat and ',' in lat:
                parts = lat.split(',')
                lat, lon = parts[0].strip(), parts[1].strip()
            else:
                lat = response.css('meta[property="og:latitude"]::attr(content)').get()
                lon = response.css('meta[property="og:longitude"]::attr(content)').get()

        # 2) data attributes on map containers
        if not lat or not lon:
            lat = response.css('[data-lat]::attr(data-lat)').get()
            lon = response.css('[data-lng]::attr(data-lng), [data-lon]::attr(data-lon)').get()

        # 3) links to Google Maps with @lat,lon
        if not lat or not lon:
            for href in response.css('a::attr(href)').getall():
                if href and 'google.com/maps' in href and '@' in href:
                    try:
                        after_at = href.split('@', 1)[1]
                        coords_part = after_at.split(',', 2)
                        # Google Maps format is usually @lat,lon
                        potential_lat = coords_part[0]
                        potential_lon = coords_part[1]
                        
                        # Validate these look like coordinates before using them
                        try:
                            lat_val = float(potential_lat)
                            lon_val = float(potential_lon)
                            # Check if they're in reasonable UK range
                            if 49 <= lat_val <= 61 and -8 <= lon_val <= 2:
                                lat = potential_lat
                                lon = potential_lon
                                break
                        except ValueError:
                            continue
                    except Exception:
                        continue

        # 4) schema.org / microformat properties
        if not lat or not lon:
            lat = response.css('[itemprop="latitude"]::attr(content), [itemprop="latitude"]::text').get()
            lon = response.css('[itemprop="longitude"]::attr(content), [itemprop="longitude"]::text').get()

        # 5) embedded maps and iframes
        if not lat or not lon:
            for iframe in response.css('iframe::attr(src)').getall():
                if iframe and ('google.com/maps' in iframe or 'maps.google' in iframe):
                    try:
                        import re
                        coord_pattern = r'@(-?\d+\.?\d*),(-?\d+\.?\d*)'
                        match = re.search(coord_pattern, iframe)
                        if match:
                            lat, lon = match.groups()
                            break
                    except Exception:
                        continue

        # 6) Try to extract from JavaScript or embedded data
        if not lat or not lon:
            import re
            content_text = ' '.join(response.css('*::text').getall())
            
            coord_patterns = [
                r'lat[itude]*[:\s]*(-?\d+\.?\d*)',
                r'lon[gitude]*[:\s]*(-?\d+\.?\d*)',
                r'latitude[:\s]*(-?\d+\.?\d*)',
                r'longitude[:\s]*(-?\d+\.?\d*)',
                r'@(-?\d+\.?\d*),(-?\d+\.?\d*)',
                r'(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
            ]
            
            for pattern in coord_patterns:
                matches = re.findall(pattern, content_text, re.IGNORECASE)
                if matches:
                    try:
                        if len(matches[0]) == 2:
                            lat, lon = matches[0]
                        else:
                            coord = float(matches[0])
                            if -90 <= coord <= 90:
                                lat = str(coord)
                                lon_matches = re.findall(r'lon[gitude]*[:\s]*(-?\d+\.?\d*)', content_text, re.IGNORECASE)
                                if lon_matches:
                                    lon = lon_matches[0]
                            else:
                                lon = str(coord)
                                lat_matches = re.findall(r'lat[itude]*[:\s]*(-?\d+\.?\d*)', content_text, re.IGNORECASE)
                                if lat_matches:
                                    lat = lat_matches[0]
                        break
                    except Exception:
                        continue

        # final normalization and validation
        try:
            if lat and lon:
                lat_f = float(lat.strip())
                lon_f = float(lon.strip())
                
                # Validate coordinate ranges
                if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                    # Additional validation: UK coordinates should be approximately:
                    # Latitude: 49-61°N (including Northern Ireland and Scotland)
                    # Longitude: -8 to 2°E (or 8°W to 2°E)
                    # If coordinates are way outside UK bounds, they're likely wrong
                    if not (49 <= lat_f <= 61 and -8 <= lon_f <= 2):
                        self.logger.warning(f"Coordinates {lat_f}, {lon_f} are outside UK bounds. Likely incorrect, skipping.")
                        return None
                    
                    return {'lat': lat_f, 'lon': lon_f}
        except Exception:
            pass

        return None

