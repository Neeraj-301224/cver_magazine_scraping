import scrapy
import re
import time
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class EventbriteSpider(BaseSpider):
    """Spider for Eventbrite charity events

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Handles multiple category URLs and pagination.
    """
    name = "eventbrite"
    category = "community_social"
    site_name = "eventbrite"
    allowed_domains = ["eventbrite.co.uk", "eventbrite.com"]
    start_urls = [
        "https://www.eventbrite.co.uk/b/united-kingdom/charity-and-causes/environment/",
        "https://www.eventbrite.co.uk/b/united-kingdom/charity-and-causes/healthcare/",
        "https://www.eventbrite.co.uk/b/united-kingdom/charity-and-causes/animal-welfare/",
        "https://www.eventbrite.co.uk/b/united-kingdom/charity-and-causes/human-rights/",
        "https://www.eventbrite.co.uk/b/united-kingdom/charity-and-causes/education/",
        "https://www.eventbrite.co.uk/b/united-kingdom/charity-and-causes/poverty/",
        "https://www.eventbrite.co.uk/b/united-kingdom/charity-and-causes/international-aid/",
        "https://www.eventbrite.co.uk/b/united-kingdom/charity-and-causes/disaster-relief/",
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
        self.max_page = 10000  # Increased limit for pagination

    def parse(self, response):
        """Parse the page and extract event links, then handle pagination."""
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # Track this page as visited
        self.pages_visited.add(response.url)
        
        # Find event links on the page
        self.logger.info("Extracting event links from page...")
        
        # Try multiple selectors for event links on Eventbrite site
        event_link_selectors = [
            'a[href*="/e/"]::attr(href)',
            'a[href*="eventbrite.co.uk/e/"]::attr(href)',
            'a[href*="eventbrite.com/e/"]::attr(href)',
            '[class*="event"] a::attr(href)',
            '[data-event-id] a::attr(href)',
            'article a::attr(href)',
            '.event-card a::attr(href)',
            '[class*="Event"] a::attr(href)',
            'a[href*="/events/"]::attr(href)',
        ]
        
        event_links_found = 0
        seen_urls = set()
        
        for selector in event_link_selectors:
            links = response.css(selector).getall()
            for link in links:
                if link:
                    absolute_url = response.urljoin(link)
                    # Filter for actual event pages (not listing pages)
                    # Eventbrite event URLs typically contain /e/ followed by event name
                    if ('/e/' in absolute_url or '/events/' in absolute_url) and \
                       absolute_url != response.url and \
                       '?page=' not in absolute_url and \
                       absolute_url not in seen_urls and \
                       absolute_url not in self.seen_events:
                        seen_urls.add(absolute_url)
                        self.seen_events.add(absolute_url)
                        event_links_found += 1
                        self.logger.info(f"Found event link #{event_links_found}: {absolute_url}")
                        try:
                            yield response.follow(link, self.parse_event, errback=self.handle_error)
                        except Exception as e:
                            self.logger.error(f"Error following event link {link}: {e}")
        
        self.logger.info(f"Total event links found on this page: {event_links_found}")
        
        # Stop pagination if no events found on this page (likely reached the end)
        if event_links_found == 0:
            self.logger.info("No events found on this page. Stopping pagination.")
            return
        
        # Handle pagination
        # Method 1: Look for pagination links in the HTML
        pagination_links = []
        seen_pagination_urls = set()
        
        pagination_selectors = [
            'a[href*="?page="]::attr(href)',
            '.pagination a::attr(href)',
            '[class*="pagination"] a::attr(href)',
            '[class*="paging"] a::attr(href)',
            'a.next::attr(href)',
            'a[rel="next"]::attr(href)',
            'a[aria-label*="next"]::attr(href)',
            'a[aria-label*="Next"]::attr(href)',
            'a[data-spec="pagination-next-link"]::attr(href)',
        ]
        
        for selector in pagination_selectors:
            links = response.css(selector).getall()
            if links:
                for href in links:
                    if href:
                        absolute_url = response.urljoin(href)
                        # Check if it's a pagination link
                        if ('?page=' in absolute_url or 
                            'next' in absolute_url.lower() or
                            'pagination' in absolute_url.lower()) and \
                           absolute_url not in seen_pagination_urls and \
                           absolute_url not in self.pages_visited:
                            seen_pagination_urls.add(absolute_url)
                            pagination_links.append(absolute_url)
        
        # Method 2: Extract current page number and generate next page
        current_url = response.url
        page_match = re.search(r'[?&]page=(\d+)', current_url)
        current_page = int(page_match.group(1)) if page_match else 1
        
        # Generate next page URL only if we found events on current page
        if event_links_found > 0 and current_page < self.max_page:
            if '?page=' in current_url:
                next_page_url = re.sub(r'[?&]page=\d+', f'?page={current_page + 1}', current_url)
            else:
                # Add page parameter
                separator = '&' if '?' in current_url else '?'
                next_page_url = f"{current_url}{separator}page={current_page + 1}"
            
            if next_page_url not in seen_pagination_urls and next_page_url not in self.pages_visited:
                pagination_links.append(next_page_url)
                self.logger.info(f"Generated next page URL: {next_page_url}")
        
        # Follow pagination links
        for next_page in pagination_links:
            if next_page:
                self.logger.info(f"Following pagination: {next_page}")
                try:
                    yield response.follow(next_page, self.parse, errback=self.handle_error)
                except Exception as e:
                    self.logger.error(f"Error following pagination link {next_page}: {e}")
        

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
            response.css('h2::text').get() or
            response.css('[data-spec="event-title"]::text').get()
        )
        
        if title:
            title = title.strip()
        
        # Enhanced description extraction
        desc_parts = []
        
        # Try Eventbrite-specific selector first: #event-description
        eventbrite_desc = response.css('#event-description')
        if eventbrite_desc:
            desc_parts = eventbrite_desc.css('*::text').getall()
            if not desc_parts:
                desc_parts = eventbrite_desc.css('::text').getall()
            desc_parts = [part.strip() for part in desc_parts if part.strip()]
            if desc_parts:
                self.logger.debug(f"Found description using #event-description")
        
        # Fallback: Try multiple selectors for description
        if not desc_parts:
            desc_selectors = [
                '.description *::text',
                '.event-description *::text',
                '.content *::text',
                'article *::text',
                '.event-details *::text',
                'p::text',
                '[class*="description"] *::text',
                '[class*="content"] *::text',
                '[data-spec="event-description"] *::text',
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
        
        # Try Eventbrite-specific selector: .start-date-and-location__date (same pattern as location extraction)
        eventbrite_date = response.css('.start-date-and-location__date, [class*="start-date-and-location__date"]')
        if eventbrite_date:
            # Get all text from the element (including child elements)
            date_text = ' '.join(eventbrite_date.css('*::text').getall())
            if not date_text or len(date_text.strip()) < 5:
                # Try direct text if no child text found
                date_text = ' '.join(eventbrite_date.css('::text').getall())
            
            if date_text and len(date_text.strip()) > 5:
                date = date_text.strip()
                raw_date = date
                self.logger.debug(f"Found date using .start-date-and-location__date: {date}")
        
        # Fallback: Try multiple date selectors
        if not date:
            date_selectors = [
                ('[class="dtstart dtend"]::text', response.css('[class="dtstart dtend"]::text').get()),
                ('.date::text', response.css('.date::text').get()),
                ('time::attr(datetime)', response.css('time::attr(datetime)').get()),
                ('.event-date::text', response.css('.event-date::text').get()),
                ('[class*="date"]:not([class*="location"])::text', response.css('[class*="date"]:not([class*="location"])::text').get()),
                ('time::text', response.css('time::text').get()),
                ('[data-spec="event-date"]::text', response.css('[data-spec="event-date"]::text').get()),
            ]
            
            for selector_name, selector_result in date_selectors:
                if selector_result:
                    potential_date = selector_result.strip()
                    # Verify it's not a location
                    location_indicators = ['street', 'road', 'avenue', 'lane', 'campsite', 'lido', 'venue', 'hall', 'center', 'centre']
                    date_patterns = [r'\d{1,2}', r'january|february|march|april|may|june|july|august|september|october|november|december']
                    has_date_pattern = any(re.search(pattern, potential_date, re.IGNORECASE) for pattern in date_patterns)
                    has_location_indicator = any(indicator in potential_date.lower() for indicator in location_indicators)
                    
                    if not (has_location_indicator and not has_date_pattern):
                        date = potential_date
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
        # Try Eventbrite-specific selector first: .start-date-and-location__location.start-date-and-location__location--link
        eventbrite_location = response.css('.start-date-and-location__location.start-date-and-location__location--link, [class*="start-date-and-location__location"][class*="start-date-and-location__location--link"]')
        if eventbrite_location:
            # Get all text from the element (including child elements)
            address_text = ' '.join(eventbrite_location.css('*::text').getall())
            if not address_text or len(address_text.strip()) < 5:
                # Try direct text if no child text found
                address_text = ' '.join(eventbrite_location.css('::text').getall())
            
            if address_text and len(address_text.strip()) > 5:
                self.logger.debug(f"Found address using Eventbrite location selector: {address_text[:50]}")
                return self.clean_text(address_text)
        
        # Fallback: Try multiple selectors for address
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
            '[data-spec="event-location"]::text',
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

