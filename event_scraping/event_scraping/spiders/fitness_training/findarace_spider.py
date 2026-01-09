import scrapy
import re
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class FindARaceSpider(BaseSpider):
    """Spider for https://findarace.com/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Filters for specific race types: 5K, 10K, Half Marathon, Ultras
    """
    name = "findarace"
    category = "fitness_training"
    site_name = "findarace"
    allowed_domains = ["findarace.com"]
    start_urls = [
        "https://findarace.com/events#results",
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
        """Parse the events listing page and extract event links and pagination."""
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # Track this page as visited
        self.pages_visited.add(response.url)
        
        # Extract event links from the listing page
        # Try multiple selectors to find event links
        event_links_found = 0
        
        # Common selectors for event links on listing pages
        event_link_selectors = [
            'a[href*="/race/"]::attr(href)',
            'a[href*="/events/"]::attr(href)',
            'a[href*="/event/"]::attr(href)',
            '.event-card a::attr(href)',
            '[class*="event-card"] a::attr(href)',
            '[class*="EventCard"] a::attr(href)',
            'article a::attr(href)',
            '[data-event-id] a::attr(href)',
            'a[href*="view-event"]::attr(href)',
            'a[href*="view-event"]::attr(href)',
        ]
        
        seen_event_urls = set()
        
        for selector in event_link_selectors:
            links = response.css(selector).getall()
            if links:
                self.logger.debug(f"Found {len(links)} links with selector '{selector}'")
                for href in links:
                    if href:
                        # Convert to absolute URL
                        absolute_url = response.urljoin(href)
                        
                        # Check if this is an event detail page (not a listing/pagination page)
                        if (absolute_url not in seen_event_urls and 
                            absolute_url not in self.seen_events and
                            '/events/p' not in absolute_url and  # Exclude pagination URLs
                            '/events#' not in absolute_url and  # Exclude hash links
                            not absolute_url.endswith('/events') and
                            not absolute_url.endswith('/events/')):
                            
                            # Check if it looks like an event detail page
                            excluded_patterns = ['/about', '/contact', '/faq', '/login', '/signup', 
                                                '/cart', '/wishlist', '/results', '/photos', '/videos', 
                                                '/blog', '/news', '/terms', '/privacy', '/cookie', 
                                                '/search', '/distances/', '/regions/', '/cities/', 
                                                '/venues/', '/series/', '/gift', '/membership', 
                                                '/calendar', '/race-info', '/corporate', '/foundation', 
                                                '/kit', '/coach', '/retreats', '/charity', '/partners', 
                                                '/volunteer', '/careers', '/sustainability', '/community', 
                                                '/pacing', '/prizes', '/club', '/injury', '/tips', 
                                                '/fundraising']
                            
                            if not any(excluded in absolute_url for excluded in excluded_patterns):
                                seen_event_urls.add(absolute_url)
                                self.seen_events.add(absolute_url)
                                event_links_found += 1
                                self.logger.info(f"Found event link #{event_links_found}: {absolute_url}")
                                
                                try:
                                    yield response.follow(href, self.parse_event, errback=self.handle_error)
                                except Exception as e:
                                    self.logger.error(f"Error following event link {href}: {e}")
        
        self.logger.info(f"Total event links found on this page: {event_links_found}")
        
        # Handle pagination - look for /events/p{number} pattern
        pagination_links = []
        seen_pagination_urls = set()
        
        # Method 1: Look for pagination links in the HTML
        pagination_selectors = [
            'a[href*="/events/p"]::attr(href)',
            'a[href*="/events/p"]::attr(href)',
            '.pagination a::attr(href)',
            '[class*="pagination"] a::attr(href)',
            '[class*="paging"] a::attr(href)',
            'a.next::attr(href)',
            'a[rel="next"]::attr(href)',
            'a[aria-label*="next"]::attr(href)',
            'a[aria-label*="Next"]::attr(href)',
        ]
        
        for selector in pagination_selectors:
            links = response.css(selector).getall()
            if links:
                for href in links:
                    if href:
                        absolute_url = response.urljoin(href)
                        # Include all pagination pages
                        if '/events/p' in absolute_url:
                            page_match = re.search(r'/events/p(\d+)', absolute_url)
                            if page_match:
                                page_num = int(page_match.group(1))
                                if absolute_url not in seen_pagination_urls:
                                    seen_pagination_urls.add(absolute_url)
                                    pagination_links.append(absolute_url)
                            elif absolute_url not in seen_pagination_urls:
                                # If no page number found but it's a pagination link, include it
                                seen_pagination_urls.add(absolute_url)
                                pagination_links.append(absolute_url)
        
        # Method 2: Extract current page number and generate next pages
        current_url = response.url
        page_match = re.search(r'/events/p(\d+)', current_url)
        if page_match:
            current_page = int(page_match.group(1))
            # Generate next page
            next_page = current_page + 1
            next_page_url = f"https://findarace.com/events/p{next_page}"
            if next_page_url not in seen_pagination_urls and next_page_url not in self.pages_visited:
                pagination_links.append(next_page_url)
                self.logger.info(f"Generated next page URL: {next_page_url}")
        elif '/events' in current_url and '/events/p' not in current_url:
            # We're on the first page (/events or /events#results), generate page 2
            next_page_url = "https://findarace.com/events/p2"
            if next_page_url not in seen_pagination_urls and next_page_url not in self.pages_visited:
                pagination_links.append(next_page_url)
                self.logger.info(f"Generated page 2 URL: {next_page_url}")
        
        # Follow pagination links
        for next_page_url in pagination_links:
            if next_page_url not in self.pages_visited:
                self.logger.info(f"Following pagination: {next_page_url}")
                try:
                    yield response.follow(next_page_url, self.parse, errback=self.handle_error)
                except Exception as e:
                    self.logger.error(f"Error following pagination link {next_page_url}: {e}")

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
        
        # Enhanced description extraction - using specific FindARace CSS classes
        # Primary selector: inline-block mb-4 text-sm font-semibold tracking-wide text-gray-700 uppercase
        # Find element with these classes, then get text from next sibling (next tag or next span)
        desc_xpath = '//*[contains(@class, "inline-block") and contains(@class, "mb-4") and contains(@class, "text-sm") and contains(@class, "font-semibold") and contains(@class, "tracking-wide") and contains(@class, "text-gray-700") and contains(@class, "uppercase")]/following-sibling::*[1]//text()'
        desc_xpath_span = '//*[contains(@class, "inline-block") and contains(@class, "mb-4") and contains(@class, "text-sm") and contains(@class, "font-semibold") and contains(@class, "tracking-wide") and contains(@class, "text-gray-700") and contains(@class, "uppercase")]/following-sibling::span[1]//text()'
        
        # Try the specific class selector first - get next sibling
        desc_parts = (
            response.xpath(desc_xpath_span).getall() or
            response.xpath(desc_xpath).getall() or
            # Also try CSS selector - find element then next sibling
            response.css('[class*="inline-block"][class*="mb-4"][class*="text-sm"][class*="font-semibold"][class*="text-gray-700"] + span *::text').getall() or
            response.css('[class*="inline-block"][class*="mb-4"][class*="text-sm"][class*="font-semibold"][class*="text-gray-700"] + * *::text').getall() or
            # Fallback selectors
            response.css('.description *::text, .event-description *::text, .content *::text').getall() or
            response.css('article *::text, .event-details *::text').getall() or
            response.css('p::text, .text::text').getall() or
            response.css('[class*="description"] *::text').getall()
        )
        
        # Enhanced date extraction - using specific FindARace CSS classes
        date = None
        
        # Primary selector: inline-block text-gray-700 uppercase font-semibold text-sm tracking-wide w-24 shrink-0 mt-[0.175rem]
        # Find element with these classes, then get text from next sibling (next tag or next span)
        date_xpath = '//*[contains(@class, "inline-block") and contains(@class, "text-gray-700") and contains(@class, "uppercase") and contains(@class, "font-semibold") and contains(@class, "text-sm") and contains(@class, "tracking-wide") and contains(@class, "w-24")]/following-sibling::*[1]/text()'
        date_xpath_span = '//*[contains(@class, "inline-block") and contains(@class, "text-gray-700") and contains(@class, "uppercase") and contains(@class, "font-semibold") and contains(@class, "text-sm") and contains(@class, "tracking-wide") and contains(@class, "w-24")]/following-sibling::span[1]/text()'
        
        # Try the specific class selector first - get next sibling
        date_selectors = [
            ('xpath_date_next_span', response.xpath(date_xpath_span).get()),
            ('xpath_date_next', response.xpath(date_xpath).get()),
            # Also try CSS selector - find element then next sibling
            ('css_date_next', response.css('[class*="inline-block"][class*="w-24"][class*="text-gray-700"] + *::text').get()),
            ('css_date_next_span', response.css('[class*="inline-block"][class*="w-24"][class*="text-gray-700"] + span::text').get()),
            # Fallback selectors
            ('dtstart', response.css('[class="dtstart dtend"]::text').get()),
            ('.date', response.css('.date::text').get()),
            ('time', response.css('time::attr(datetime)').get()),
            ('.event-date', response.css('.event-date::text').get()),
            ('[class*="date"]', response.css('[class*="date"]::text').get()),
        ]
        
        for selector_name, selector_result in date_selectors:
            if selector_result:
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
        self.logger.info(f"Event extracted - Name: {item['name'][:50] if item['name'] else 'N/A'}... (Total: {self.total_items_scraped})")
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

