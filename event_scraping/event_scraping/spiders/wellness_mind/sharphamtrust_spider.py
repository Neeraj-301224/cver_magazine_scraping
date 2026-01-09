import scrapy
import re
import time
from ..base_spider import BaseSpider
from ...items import EventScrapingItem
from scrapy.http import HtmlResponse


class SharphamTrustSpider(BaseSpider):
    """Spider for https://www.sharphamtrust.org/whatson

    Targets event listings to extract:
    - title, date, location, url, description
    
    Uses Selenium to handle dynamic content loading.
    """
    name = "sharphamtrust"
    category = "wellness_mind"
    site_name = "sharphamtrust"
    allowed_domains = ["sharphamtrust.org"]
    start_urls = [
        "https://www.sharphamtrust.org/whatson"
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_events = set()
        self.geocoding_cache = {}
        self.total_items_scraped = 0
    
    def ensure_uk_in_address(self, address):
        """Ensure 'UK' is present in the address if it's not already there."""
        if not address:
            return address
        
        # Check if "UK" is already present (case-insensitive)
        if re.search(r'\bUK\b', address, re.IGNORECASE):
            return address
        
        # Skip adding UK for online events
        if address.lower() in ['online', 'online retreats']:
            return address
        
        # Add "UK" to the end of the address
        return f"{address}, UK"

    def parse(self, response):
        """Parse the listing page and extract 'More Info' links to follow to detail pages."""
        self.logger.info(f"Parsing listing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # Use Selenium to load the page and find all "More Info" links
        self.logger.info("=" * 80)
        self.logger.info("LOADING PAGE WITH SELENIUM TO FIND 'MORE INFO' LINKS")
        self.logger.info("=" * 80)
        
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException, NoSuchElementException
            
            self.logger.info("✓ Selenium imported successfully")
            
            # Configure Chrome options
            options = webdriver.ChromeOptions()
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Initialize driver
            try:
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
                self.logger.info("✓ Using webdriver-manager for ChromeDriver")
            except ImportError:
                driver = webdriver.Chrome(options=options)
                self.logger.info("✓ Using system ChromeDriver")
            
            try:
                self.logger.info(f"Loading URL with Selenium: {response.url}")
                driver.get(response.url)
                self.logger.info("✓ Page loaded")
                
                # Wait for page to load
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                self.logger.info("✓ Body element found")
                
                # Wait for dynamic content
                self.logger.info("Waiting for dynamic content to initialize...")
                time.sleep(5)
                
                # Scroll to load all content
                self.logger.info("Scrolling to load all content...")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
                # Find all "More Info" links
                more_info_links = []
                
                # Try multiple selectors for "More Info" links
                link_selectors = [
                    'a:contains("More Info")',
                    'a[href*="/whatson/"]',
                    'a[href*="/event/"]',
                    'a[href*="/retreat/"]',
                ]
                
                # Use XPath to find links containing "More Info" text
                try:
                    links = driver.find_elements(By.XPATH, '//a[contains(text(), "More Info")]')
                    self.logger.info(f"Found {len(links)} 'More Info' links using XPath")
                    for link in links:
                        href = link.get_attribute('href')
                        if href and href not in more_info_links:
                            more_info_links.append(href)
                            self.logger.info(f"Found More Info link: {href}")
                except Exception as e:
                    self.logger.warning(f"Error finding More Info links with XPath: {e}")
                
                # Also try to find links in event cards
                try:
                    all_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/whatson/"], a[href*="/event/"], a[href*="/retreat/"]')
                    for link in all_links:
                        href = link.get_attribute('href')
                        if href and href not in more_info_links and href != response.url:
                            more_info_links.append(href)
                except Exception as e:
                    self.logger.warning(f"Error finding event links: {e}")
                
                self.logger.info(f"Total unique event detail page links found: {len(more_info_links)}")
                
                # Follow each link to extract event details
                for link_url in more_info_links:
                    if link_url not in self.seen_events:
                        self.seen_events.add(link_url)
                        self.logger.info(f"Following link to detail page: {link_url}")
                        yield scrapy.Request(
                            url=link_url,
                            callback=self.parse_event_detail,
                            errback=self.handle_error,
                            meta={'original_url': link_url}
                        )
                
            finally:
                driver.quit()
                self.logger.info("✓ Selenium driver closed")
                
        except ImportError:
            self.logger.warning("Selenium not available, trying regular Scrapy parsing...")
            # Fallback: try to find links with regular Scrapy
            more_info_links = response.css('a:contains("More Info")::attr(href)').getall()
            for link in more_info_links:
                if link:
                    absolute_url = response.urljoin(link)
                    if absolute_url not in self.seen_events:
                        self.seen_events.add(absolute_url)
                        yield response.follow(link, self.parse_event_detail, errback=self.handle_error)
        except Exception as e:
            self.logger.error(f"Error with Selenium: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            # Fallback: try to find links with regular Scrapy
            more_info_links = response.css('a:contains("More Info")::attr(href)').getall()
            for link in more_info_links:
                if link:
                    absolute_url = response.urljoin(link)
                    if absolute_url not in self.seen_events:
                        self.seen_events.add(absolute_url)
                        yield response.follow(link, self.parse_event_detail, errback=self.handle_error)
    
    def parse_event_detail(self, response):
        """Parse individual event detail pages to extract title, date, location, and description."""
        self.logger.info(f"Parsing event detail page: {response.url}")
        
        item = EventScrapingItem()
        item['category'] = self.category
        item['site'] = self.site_name
        item['url'] = response.url
        
        # Extract title from class: row event-description content justify-content-center content
        title = None
        title_selectors = [
            '.row.event-description.content.justify-content-center.content h1::text',
            '.row.event-description.content.justify-content-center.content h2::text',
            '.row.event-description.content.justify-content-center.content h3::text',
            '.row.event-description h1::text',
            '.row.event-description h2::text',
            '.event-description h1::text',
            '.event-description h2::text',
            'h1::text',
            'h2::text',
        ]
        
        for selector in title_selectors:
            title = response.css(selector).get()
            if title:
                title = title.strip()
                break
        
        if not title:
            # Try XPath as fallback
            title_elem = response.xpath('//div[contains(@class, "row") and contains(@class, "event-description") and contains(@class, "content")]//h1 | //div[contains(@class, "row") and contains(@class, "event-description") and contains(@class, "content")]//h2')
            if title_elem:
                title = title_elem.css('::text').get()
                if title:
                    title = title.strip()
        
        # Extract date from class: fa-solid fa-calendar-days me-2 me-lg-0
        # The date is in a <small> tag beside the <i> tag with the calendar icon
        date = None
        raw_date = None
        
        # Use XPath to find the calendar icon (i tag with fa-calendar-days class)
        calendar_icon = response.xpath('//i[contains(@class, "fa-calendar-days")]')
        if calendar_icon:
            # Find the <small> tag that is a sibling of the calendar icon
            # Try following sibling first
            small_tag = calendar_icon.xpath('./following-sibling::small[1]')
            if not small_tag:
                # Try preceding sibling
                small_tag = calendar_icon.xpath('./preceding-sibling::small[1]')
            if not small_tag:
                # Try any sibling small tag
                small_tag = calendar_icon.xpath('./../small[1]')
            
            if small_tag:
                date_text = ' '.join(small_tag.css('::text').getall()).strip()
                if date_text:
                    raw_date = date_text
                    date = self.convert_date_format(raw_date)
                    if date:
                        self.logger.info(f"Found date from small tag: {raw_date} -> {date}")
            
            # Fallback: if no small tag found, try parent element
            if not date:
                parent = calendar_icon.xpath('./parent::*')
                if parent:
                    date_text = ' '.join(parent.css('::text').getall()).strip()
                    # Also check following sibling
                    following = parent.xpath('./following-sibling::*[1]')
                    if following:
                        following_text = ' '.join(following.css('::text').getall()).strip()
                        if following_text:
                            date_text += ' ' + following_text
                    
                    # Extract date pattern from the text
                    date_patterns = [
                        r'(\d{4})\s+(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
                        r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
                        r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
                        r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',
                    ]
                    
                    for pattern in date_patterns:
                        match = re.search(pattern, date_text, re.IGNORECASE)
                        if match:
                            raw_date = match.group(0).strip()
                            date = self.convert_date_format(raw_date)
                            if date:
                                self.logger.info(f"Found date from parent: {raw_date} -> {date}")
                                break
        
        # Extract location/address from class: fa-solid fa-location-dot me-2 me-lg-0
        # The location is in a <small> tag beside the <i> tag with the location icon
        address = None
        
        # Use XPath to find the location icon (i tag with fa-location-dot class)
        location_icon = response.xpath('//i[contains(@class, "fa-location-dot")]')
        if location_icon:
            # Find the <small> tag that is a sibling of the location icon
            # Try following sibling first
            small_tag = location_icon.xpath('./following-sibling::small[1]')
            if not small_tag:
                # Try preceding sibling
                small_tag = location_icon.xpath('./preceding-sibling::small[1]')
            if not small_tag:
                # Try any sibling small tag
                small_tag = location_icon.xpath('./../small[1]')
            
            if small_tag:
                location_text = ' '.join(small_tag.css('::text').getall()).strip()
                if location_text:
                    address = location_text
                    self.logger.info(f"Found address from small tag: {address}")
            
            # Fallback: if no small tag found, try parent element
            if not address:
                parent = location_icon.xpath('./parent::*')
                if parent:
                    # Get text from parent, but exclude the icon itself
                    location_text = ' '.join(parent.css('::text').getall()).strip()
                    # Clean up the location text
                    location_text = re.sub(r'^location\s*:?\s*-?\s*', '', location_text, flags=re.IGNORECASE)
                    location_text = re.sub(r'\s+', ' ', location_text).strip()
                    
                    # Remove any date patterns that might have been picked up
                    location_text = re.sub(r'\d{4}\s+\d{1,2}\s+\w+', '', location_text, flags=re.IGNORECASE)
                    location_text = re.sub(r'\d{1,2}\s+\w+\s+\d{4}', '', location_text, flags=re.IGNORECASE)
                    location_text = re.sub(r'\s+', ' ', location_text).strip()
                    
                    if location_text and len(location_text) > 3:
                        address = location_text
                        self.logger.info(f"Found address from parent: {address}")
        
        # If no address found, try to identify venue from title or description
        if not address:
            all_text = (title or '') + ' ' + ' '.join(response.css('body::text').getall())
            venue_keywords = ['Online', 'The Barn', 'Sharpham House', 'The Coach House', 'Woodland', 'The Hermitage']
            for keyword in venue_keywords:
                if keyword.lower() in all_text.lower():
                    address = keyword
                    break
        
        # Default address if not found
        if not address:
            address = "Sharpham House, Ashprington, Totnes, Devon, UK TQ9 7UT"
        
        # Extract description from class: row event-description content justify-content-center content
        description = None
        desc_selectors = [
            '.row.event-description.content.justify-content-center.content p::text',
            '.row.event-description.content p::text',
            '.event-description p::text',
            '.row.event-description.content.justify-content-center.content *::text',
        ]
        
        desc_parts = []
        for selector in desc_selectors:
            parts = response.css(selector).getall()
            if parts:
                desc_parts = [part.strip() for part in parts if part.strip() and len(part.strip()) > 10]
                if desc_parts:
                    break
        
        if desc_parts:
            description = ' '.join(desc_parts)
        
        # Create short description
        short_description = None
        if description:
            short_description = description[:200] + '...' if len(description) > 200 else description
        
        # Build event_data for database check before geocoding
        event_data = {
            'name': title,
            'date': date,
            'url': response.url
        }
        
        # Geocode address
        # Pass event_data to enable database check before geocoding
        coords = None
        if address and address not in ['Online', 'Online Retreats']:
            coords = self.geocode_address(address, event_data=event_data)
        elif address == "Sharpham House, Ashprington, Totnes, Devon, UK TQ9 7UT":
            coords = self.geocode_address(address, event_data=event_data)
        
        # Only create item if we have at least a title
        if title:
            item['name'] = self.clean_text(title)
            item['date'] = date
            item['raw_date'] = raw_date
            item['short_description'] = self.clean_text(short_description) if short_description else None
            item['coordinates'] = coords
            # Ensure UK is present in address
            address = self.ensure_uk_in_address(address)
            item['address'] = address
            item['category'] = "Wellness & Mind"
            item['subcategory'] = "Mindfulness"
            item['raw'] = {
                'title': title,
                'date': raw_date,
                'description': description,
                'address': address,  # Use the updated address with UK
                'coordinates': coords,
            }
            
            # Check for duplicates
            item_key = f"{item['name']}_{item['date']}"
            if item_key not in self.seen_events:
                self.seen_events.add(item_key)
                self.total_items_scraped += 1
                self.logger.info(f"✓ Extracted event #{self.total_items_scraped}: {item['name'][:50]}... (Date: {date}, Location: {address})")
                yield item
            else:
                self.logger.debug(f"Skipping duplicate: {item['name'][:50]}...")
        else:
            self.logger.warning(f"Could not extract title from event detail page: {response.url}")
    
    def extract_events_with_selenium(self, driver, base_url):
        """Extract events using Selenium WebDriver."""
        from selenium.webdriver.common.by import By
        from selenium.common.exceptions import NoSuchElementException
        
        self.logger.info("Extracting events using Selenium...")
        
        # Get page source and look for all text containing date patterns
        page_text = driver.find_element(By.TAG_NAME, 'body').text
        self.logger.info(f"Page text length: {len(page_text)} characters")
        
        # Find all headings that might be event titles
        headings = driver.find_elements(By.CSS_SELECTOR, 'h2, h3, h4, h5, h6')
        self.logger.info(f"Found {len(headings)} headings on the page")
        
        # Process each heading as a potential event
        for idx, heading in enumerate(headings):
            try:
                title = heading.text.strip()
                if not title or len(title) < 10:
                    continue
                
                # Skip navigation headings
                skip_keywords = ['Filter', 'Browse', 'Calendar', 'Events', 'Courses', 'Retreats', 'Month', 'Reset', 'Update', 'Whats on', 'Sign up', 'Donate', 'Menu', 'Home', 'The Sharpham Trust']
                if any(keyword.lower() in title.lower() for keyword in skip_keywords):
                    continue
                
                self.logger.debug(f"Processing heading {idx + 1}: '{title[:60]}...'")
                
                # Get the parent container
                try:
                    parent = heading.find_element(By.XPATH, './parent::*')
                except:
                    parent = None
                
                # Get all text from parent and following siblings
                all_text = title
                if parent:
                    all_text += ' ' + parent.text
                
                # Also check following siblings
                try:
                    following = heading.find_elements(By.XPATH, './following-sibling::*[position()<=5]')
                    for elem in following:
                        all_text += ' ' + elem.text
                except:
                    pass
                
                # Look for date pattern: "2025 13 Dec" or similar
                date_match = None
                date_patterns = [
                    r'(\d{4})\s+(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
                    r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, all_text, re.IGNORECASE)
                    if match:
                        date_match = match.group(0).strip()
                        break
                
                if not date_match:
                    self.logger.debug(f"No date found for heading: {title[:50]}...")
                    continue
                
                # Extract venue
                address = None
                venue_keywords = ['Online', 'The Barn', 'Sharpham House', 'The Coach House', 'Woodland', 'The Hermitage']
                for keyword in venue_keywords:
                    if keyword.lower() in all_text.lower():
                        address = keyword
                        break
                
                if not address:
                    address = "Sharpham House, Ashprington, Totnes, Devon, UK TQ9 7UT"
                
                # Extract description
                description = None
                if parent:
                    try:
                        desc_elems = parent.find_elements(By.CSS_SELECTOR, 'p')
                        if desc_elems:
                            desc_texts = [elem.text.strip() for elem in desc_elems if elem.text.strip()]
                            if desc_texts:
                                description = ' '.join(desc_texts)
                    except:
                        pass
                
                # Extract URL
                url = base_url
                try:
                    if parent:
                        links = parent.find_elements(By.CSS_SELECTOR, 'a')
                        for link in links:
                            href = link.get_attribute('href')
                            if href and ('whatson' in href.lower() or 'event' in href.lower() or 'retreat' in href.lower()):
                                url = href
                                break
                except:
                    pass
                
                # Create and yield item
                item = EventScrapingItem()
                item['category'] = self.category
                item['site'] = self.site_name
                item['url'] = url
                item['name'] = self.clean_text(title)
                
                item['date'] = self.convert_date_format(date_match)
                item['raw_date'] = date_match
                
                short_description = description[:200] + '...' if description and len(description) > 200 else description
                item['short_description'] = self.clean_text(short_description) if short_description else None
                
                # Build event_data for database check before geocoding
                geocode_event_data = {
                    'name': item['name'],
                    'date': item['date'],
                    'url': item['url']
                }
                
                coords = None
                # Pass event_data to enable database check before geocoding
                if address and address not in ['Online', 'Online Retreats']:
                    coords = self.geocode_address(address, event_data=geocode_event_data)
                elif address == "Sharpham House, Ashprington, Totnes, Devon, UK TQ9 7UT":
                    coords = self.geocode_address(address, event_data=geocode_event_data)
                
                item['coordinates'] = coords
                # Ensure UK is present in address
                address = self.ensure_uk_in_address(address)
                item['address'] = address
                item['category'] = "Wellness & Mind"
                item['subcategory'] = "Mindfulness"
                item['raw'] = {
                    'title': title,
                    'date': date_match,
                    'description': description,
                    'address': address,
                    'coordinates': coords,
                }
                
                # Check for duplicates
                item_key = f"{item['name']}_{item['date']}"
                if item_key not in self.seen_events:
                    self.seen_events.add(item_key)
                    self.total_items_scraped += 1
                    self.logger.info(f"✓ Extracted event #{self.total_items_scraped}: {item['name'][:50]}... (Date: {date_match})")
                    yield item
                else:
                    self.logger.debug(f"Skipping duplicate: {item['name'][:50]}...")
                    
            except Exception as e:
                self.logger.debug(f"Error extracting from heading {idx + 1}: {e}")
                import traceback
                self.logger.debug(traceback.format_exc())
                continue
        
        self.logger.info(f"Finished extracting events. Total unique events: {self.total_items_scraped}")
    
    def extract_events_from_page(self, response):
        """Extract events directly from the listing page (fallback method)."""
        self.logger.info("Extracting events directly from listing page (fallback)...")
        
        events_found = 0
        
        # Find all headings
        headings = response.css('h2, h3, h4, h5, h6')
        self.logger.info(f"Found {len(headings)} headings on the page")
        
        for idx, heading in enumerate(headings):
            try:
                title = heading.css('::text').get()
                if not title:
                    title = ''.join(heading.css('::text').getall())
                
                if not title or len(title.strip()) < 10:
                    continue
                
                title = title.strip()
                
                # Skip navigation headings
                skip_keywords = ['Filter', 'Browse', 'Calendar', 'Events', 'Courses', 'Retreats', 'Month', 'Reset', 'Update', 'Whats on', 'Sign up', 'Donate', 'Menu']
                if any(keyword.lower() in title.lower() for keyword in skip_keywords):
                    continue
                
                # Get parent and following text
                parent = heading.xpath('./parent::*[1]')
                all_text = title
                if parent:
                    all_text += ' ' + ' '.join(parent.css('::text').getall())
                
                # Look for date
                date_match = None
                date_patterns = [
                    r'(\d{4})\s+(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
                    r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})',
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, all_text, re.IGNORECASE)
                    if match:
                        date_match = match.group(0).strip()
                        break
                
                if not date_match:
                    continue
                
                # Extract venue
                address = None
                venue_keywords = ['Online', 'The Barn', 'Sharpham House', 'The Coach House', 'Woodland', 'The Hermitage']
                for keyword in venue_keywords:
                    if keyword.lower() in all_text.lower():
                        address = keyword
                        break
                
                if not address:
                    address = "Sharpham House, Ashprington, Totnes, Devon, UK TQ9 7UT"
                
                # Extract description
                description = None
                if parent:
                    desc_parts = parent.css('p::text').getall()
                    if desc_parts:
                        description = ' '.join([d.strip() for d in desc_parts if d.strip()])
                
                # Extract URL
                url = response.url
                if parent:
                    link = parent.css('a::attr(href)').get()
                    if link:
                        url = response.urljoin(link)
                
                # Create item
                if title and date_match:
                    item = EventScrapingItem()
                    item['category'] = self.category
                    item['site'] = self.site_name
                    item['url'] = url
                    item['name'] = self.clean_text(title)
                    item['date'] = self.convert_date_format(date_match)
                    item['raw_date'] = date_match
                    
                    short_description = description[:200] + '...' if description and len(description) > 200 else description
                    item['short_description'] = self.clean_text(short_description) if short_description else None
                    
                    # Build event_data for database check before geocoding
                    geocode_event_data = {
                        'name': item['name'],
                        'date': item['date'],
                        'url': item['url']
                    }
                    
                    coords = None
                    # Pass event_data to enable database check before geocoding
                    if address and address not in ['Online']:
                        coords = self.geocode_address(address, event_data=geocode_event_data)
                    
                    item['coordinates'] = coords
                    # Ensure UK is present in address
                    address = self.ensure_uk_in_address(address)
                    item['address'] = address
                    item['category'] = "Wellness & Mind"
                    item['subcategory'] = "Mindfulness"
                    item['raw'] = {
                        'title': title,
                        'date': date_match,
                        'description': description,
                        'address': address,
                        'coordinates': coords,
                    }
                    
                    item_key = f"{item['name']}_{item['date']}"
                    if item_key not in self.seen_events:
                        self.seen_events.add(item_key)
                        self.total_items_scraped += 1
                        events_found += 1
                        self.logger.info(f"Extracted event #{events_found}: {item['name'][:50]}...")
                        yield item
                        
            except Exception as e:
                self.logger.debug(f"Error extracting event from heading {idx + 1}: {e}")
                continue
        
        return events_found

    def convert_date_format(self, date_str):
        """Convert date to MM/DD/YYYY format."""
        if not date_str:
            return None
        try:
            from datetime import datetime
            date_str = date_str.strip()
            
            # Handle various date formats
            formats = [
                '%Y %d %b',      # "2025 13 Dec"
                '%Y %d %B',      # "2025 13 December"
                '%d %b %Y',      # "13 Dec 2025"
                '%d %B %Y',      # "13 December 2025"
                '%b %d, %Y',     # "Dec 13, 2025"
                '%B %d, %Y',     # "December 13, 2025"
                '%d/%m/%Y',      # "13/12/2025"
                '%d-%m-%Y',      # "13-12-2025"
                '%Y-%m-%d',      # "2025-12-13"
                '%Y/%m/%d',      # "2025/12/13"
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%m/%d/%Y')
                except ValueError:
                    continue
            
            # Try to parse with dateutil if available
            try:
                from dateutil import parser
                dt = parser.parse(date_str)
                return dt.strftime('%m/%d/%Y')
            except:
                pass
            
            return date_str
        except Exception as e:
            self.logger.error(f"Date conversion failed for '{date_str}': {e}")
            return None

    # geocode_address is inherited from BaseSpider, which uses the common function
    # that tries LocationIQ first (if API key is configured), then falls back to Nominatim.
    # It also checks the database before geocoding if event_data is provided and
    # check_db_before_geocoding is enabled.
