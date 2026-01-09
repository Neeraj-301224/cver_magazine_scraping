import scrapy
import re
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class UKRunningEventsSpider(BaseSpider):
    """Spider for https://www.ukrunningevents.co.uk/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Filters for specific race types: 5K, 10K, Half Marathon, Ultras
    """
    name = "ukrunningevents"
    category = "fitness_training"
    site_name = "ukrunningevents"
    allowed_domains = ["ukrunningevents.co.uk"]
    start_urls = [
        "https://www.ukrunningevents.co.uk/",
        "https://www.ukrunningevents.co.uk/events/inflatable-5k",
        "https://www.ukrunningevents.co.uk/events/trail-runs",
        "https://www.ukrunningevents.co.uk/events/hiking-trails",
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
        
        # Find event links on the homepage or listings
        # Use XPath to get href attribute more reliably
        all_links = response.xpath('//a/@href').getall()
        # Also try CSS selector as fallback
        if not all_links:
            all_links = response.css('a::attr(href)').getall()
        
        self.logger.info(f"Found {len(all_links)} total links on page")
        event_links_found = 0
        event_links = []
        skipped_count = 0
        
        # Common patterns for UKRunningEvents event URLs
        # Events are typically at /events/[event-name] or similar patterns
        event_patterns = ['/event/', '/events/', '/race/', '/races/', '/running-event/']
        
        for href in all_links:
            if not href or not isinstance(href, str):
                continue
            
            # Clean the href - strip whitespace
            href = href.strip()
            
            # Skip empty hrefs
            if not href:
                continue
            
            # Skip malformed URLs (containing HTML attributes, encoded characters, etc.)
            if any(bad_pattern in href for bad_pattern in ['%3C', '%3E', 'wire:', 'wire:snapshot', 'wire:effects', '<', '>', 'javascript:', 'mailto:', 'href=', 'class=', 'wire:id']):
                skipped_count += 1
                continue
            
            # Skip URLs that are too long (likely malformed) or too short
            if len(href) > 500 or len(href) < 2:
                continue
            
            # Valid hrefs should start with /, http://, https://, or be relative paths
            # Skip if it doesn't match these patterns
            if not (href.startswith('/') or href.startswith('http://') or href.startswith('https://') or href.startswith('./') or href.startswith('../')):
                continue
            
            # Skip if it contains spaces (invalid in URLs)
            if ' ' in href:
                continue
            
            # Convert to absolute URL
            try:
                absolute_url = response.urljoin(href)
            except Exception as e:
                self.logger.debug(f"Skipping invalid URL: {href} - {e}")
                continue
            
            # Validate URL format - must be http or https
            if not absolute_url.startswith('http://') and not absolute_url.startswith('https://'):
                continue
            
            # Additional check: skip if URL contains encoded HTML tags
            if '%3C' in absolute_url or '%3E' in absolute_url:
                continue
            
            # Check if this is an event link
            is_event_link = False
            
            # Exclude only the main /events page itself (not category pages like /events/trail-runs)
            if href == '/events' or href == '/events/' or href == '/events':
                continue
            
            # Check if URL contains event-like patterns but exclude common non-event pages
            if any(pattern in href for pattern in event_patterns):
                # Exclude common non-event pages (but allow category pages like /events/trail-runs)
                excluded_patterns = ['/about', '/contact', '/faq', '/login', '/signup', '/cart', 
                                    '/wishlist', '/results', '/photos', '/videos', '/blog', 
                                    '/news', '/terms', '/privacy', '/cookie', '/search',
                                    '/distances/', '/regions/', '/cities/', '/venues/', '/series/',
                                    '/gift', '/membership', '/calendar', '/race-info', '/corporate',
                                    '/foundation', '/kit', '/coach', '/retreats', '/charity',
                                    '/partners', '/volunteer', '/careers', '/sustainability',
                                    '/community', '/pacing', '/prizes', '/club', '/injury',
                                    '/tips', '/fundraising']
                
                # If it's an event URL and not excluded, it might be an event
                if not any(excluded in href for excluded in excluded_patterns):
                    # For /events/ URLs, check if there's a slug after it
                    if '/events/' in href:
                        parts = href.split('/events/')
                        if len(parts) > 1 and parts[1] and parts[1].strip():
                            slug = parts[1].split('/')[0]  # Get the first part of the slug
                            # Exclude category listing pages but allow individual events
                            category_pages = ['inflatable-5k', 'trail-runs', 'hiking-trails']
                            if slug not in category_pages:
                                # Has a slug after /events/ that's not a category page, likely an individual event
                                is_event_link = True
                            # Also check if it's a deeper path (individual event within a category)
                            if len(parts[1].split('/')) > 1:
                                # Has multiple path segments, likely an individual event
                                is_event_link = True
                    # Check if URL looks like an event slug
                    event_indicators = ['run', 'race', 'marathon', '10k', '5k', 'half', 'ultra', 
                                      'trail', 'triathlon', 'duathlon', 'swim', 'derby', 'st-albans',
                                      'guildford', 'bristol', 'lincoln', 'leeds', 'cardiff', 'southampton',
                                      'huntingdon', 'brands-hatch', 'cheshire', 'coventry', 'bournemouth',
                                      'exeter', 'newcastle', 'glasgow', 'edinburgh', 'bakewell', 'york',
                                      'kempton', 'cheltenham', 'norwich', 'inflatable', 'hiking']
                    if any(indicator in href.lower() for indicator in event_indicators) or '/event/' in href:
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
        
        self.logger.info(f"Total event links found: {event_links_found} (skipped {skipped_count} invalid links)")
        
        # Try to find event cards/data directly on the page
        event_card_selectors = [
            'a[href*="/event/"]',
            'a[href*="/events/"]',
            'a[href*="/race/"]',
            'a[href*="/running-event/"]',
            '.event-card a::attr(href)',
            '[class*="event"] a::attr(href)',
            '[class*="Event"] a::attr(href)',
            '.event-item a::attr(href)',
            'article[class*="event"] a::attr(href)',
            'div[class*="event-card"] a::attr(href)',
            '[data-event-id] a::attr(href)',
            '[data-testid*="event"] a::attr(href)',
            '[data-cy*="event"] a::attr(href)',
        ]
        
        for selector in event_card_selectors:
            card_links = response.css(selector).getall()
            if card_links:
                self.logger.info(f"Found {len(card_links)} links with selector '{selector}'")
                for link in card_links:
                    if not link or not isinstance(link, str):
                        continue
                    
                    # Clean the link - strip whitespace
                    link = link.strip()
                    
                    # Skip empty links
                    if not link:
                        continue
                    
                    # Skip malformed URLs
                    if any(bad_pattern in link for bad_pattern in ['%3C', '%3E', 'wire:', 'wire:snapshot', 'wire:effects', '<', '>', 'javascript:', 'mailto:', 'href=', 'class=', 'wire:id']):
                        continue
                    
                    # Skip URLs that are too long (likely malformed) or too short
                    if len(link) > 500 or len(link) < 2:
                        continue
                    
                    # Valid hrefs should start with /, http://, https://, or be relative paths
                    if not (link.startswith('/') or link.startswith('http://') or link.startswith('https://') or link.startswith('./') or link.startswith('../')):
                        continue
                    
                    # Skip if it contains spaces (invalid in URLs)
                    if ' ' in link:
                        continue
                    
                    try:
                        absolute_url = response.urljoin(link)
                    except Exception as e:
                        self.logger.debug(f"Skipping invalid URL: {link} - {e}")
                        continue
                    
                    # Validate URL format
                    if not absolute_url.startswith('http://') and not absolute_url.startswith('https://'):
                        continue
                    
                    # Additional check: skip if URL contains encoded HTML tags
                    if '%3C' in absolute_url or '%3E' in absolute_url:
                        continue
                    
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
        
        # Skip if response is not successful
        if response.status != 200:
            self.logger.warning(f"Skipping non-200 response: {response.status} for {response.url}")
            return
        
        try:
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
            
            # Enhanced description extraction
            # Description is in tab with x-show="tab === 'about'"
            # Use XPath to find element with x-show attribute containing "about"
            desc_xpath = '//*[contains(@x-show, "about")]//text()'
            
            desc_parts = (
                # Primary selector: get all text from element with x-show containing "about"
                response.xpath(desc_xpath).getall() or
                # Also try CSS selector for x-show attribute
                response.css('[x-show*="about"] *::text').getall() or
                # Fallback selectors
                response.css('.description *::text, .event-description *::text, .content *::text').getall() or
                response.css('article *::text, .event-details *::text').getall() or
                response.css('p::text, .text::text').getall() or
                response.css('[class*="description"] *::text').getall()
            )
            
            # Enhanced date extraction
            # Date is in: uppercase font-cubano text-ukre-red text-3xl lg:text-5xl text-center
            # Date is broken across multiple tags inside this div, so get all text
            date = None
            
            # Try UK Running Events specific date selector first
            # Use XPath to get all text from the div (since date is broken across multiple tags)
            date_xpath = '//*[contains(@class, "uppercase") and contains(@class, "font-cubano") and contains(@class, "text-ukre-red") and contains(@class, "text-3xl") and contains(@class, "text-center")]//text()'
            
            date_selectors = [
                # Primary selector: get all text from the div (date broken across multiple tags)
                ('xpath_date_div', ' '.join(response.xpath(date_xpath).getall()).strip()),
                # Also try CSS selector to get all text
                ('css_date_div', ' '.join(response.css('[class*="uppercase"][class*="font-cubano"][class*="text-ukre-red"][class*="text-3xl"][class*="text-center"] *::text').getall()).strip()),
                # Fallback to common selectors
                ('[class="dtstart dtend"]', response.css('[class="dtstart dtend"]::text').get()),
                ('.date', response.css('.date::text').get()),
                ('time', response.css('time::attr(datetime)').get()),
                ('.event-date', response.css('.event-date::text').get()),
                ('[class*="date"]', response.css('[class*="date"]::text').get()),
            ]
            
            for selector_name, selector_result in date_selectors:
                if selector_result and selector_result.strip():
                    date = selector_result.strip()
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
                        self.logger.debug(f"Found date in description: {date}")
                        break
            
            # Store raw date before conversion
            raw_date = date
            
            # Clean date: remove "Distances \n" and other unwanted text
            if date:
                # Remove "Distances" and newlines from date
                date = re.sub(r'Distances\s*\n*', '', date, flags=re.IGNORECASE)
                date = self.clean_whitespace(date)
            
            # Clean raw_date as well
            if raw_date:
                raw_date = re.sub(r'Distances\s*\n*', '', raw_date, flags=re.IGNORECASE)
                raw_date = self.clean_whitespace(raw_date)
            
            # Convert date to MM/DD/YYYY format
            if date:
                date = self.convert_date_format(date)
            
            # Check if this event matches our target race types
            if not self.is_target_race_type(title, desc_parts):
                self.logger.info(f"Event does not match target race types - skipping: {title}")
                return

            # Short description extraction
            short_description = None
            if desc_parts:
                joined = '\n'.join(desc_parts).strip()
                short_description = joined.split('\n')[0]
                if len(short_description) > 200:
                    short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
                # Clean: remove excessive whitespace from short_description
                if short_description:
                    short_description = self.clean_whitespace(short_description)

            # Address extraction
            # Location is in: text-xl lg:text-3xl font-bold mt-2
            # The actual location is given in <br> tags, so we need to get text from the element itself
            address = None
            
            # Try UK Running Events specific location selector first
            # Location is in: text-xl lg:text-3xl font-bold mt-2
            # Get all text from the element (including text separated by <br> tags)
            location_xpath = '//*[contains(@class, "text-xl") and contains(@class, "font-bold") and contains(@class, "mt-2")]//text()'
            
            location_selectors = [
                # Primary selector: get all text from the element (location in <br> tags)
                ('xpath_location', response.xpath(location_xpath).getall()),
                # Also try CSS selector to get all text
                ('css_location', response.css('[class*="text-xl"][class*="font-bold"][class*="mt-2"] *::text').getall()),
                # Try getting text directly from the element
                ('css_location_direct', [response.css('[class*="text-xl"][class*="font-bold"][class*="mt-2"]::text').get()] if response.css('[class*="text-xl"][class*="font-bold"][class*="mt-2"]::text').get() else []),
            ]
            
            for selector_name, selector_result in location_selectors:
                if selector_result and isinstance(selector_result, list) and selector_result:
                    # Join all text parts and clean up (handle <br> tags by joining with space)
                    address = ' '.join([text.strip() for text in selector_result if text and text.strip()]).strip()
                    if address:
                        self.logger.debug(f"Found location using selector '{selector_name}': {address}")
                        break
            
            # Fallback to general address extraction if specific selector didn't work
            if not address:
                address = self.extract_address(response)
            
            # Clean address: remove "Location" text and excessive whitespace
            if address:
                address = self.remove_location_text(address)
                address = self.clean_whitespace(address)

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
            # Clean full_description: remove excessive whitespace
            full_description = ' '.join(desc_parts) if desc_parts else None
            if full_description:
                full_description = self.clean_whitespace(full_description)
            
            item['raw'] = {
                'title': title,
                'date': date,
                'desc_preview': short_description,
                'full_description': full_description,
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
            
        except Exception as e:
            self.logger.error(f"Error parsing event page {response.url}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return

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

    def clean_whitespace(self, text):
        """Remove excessive whitespace (multiple spaces, tabs, newlines) from text."""
        if not text:
            return text
        
        # Replace all newlines, tabs, and carriage returns with spaces
        text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        # Strip leading/trailing whitespace
        return text.strip()
    
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

