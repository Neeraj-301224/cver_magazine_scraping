from ..base_spider import BaseSpider
from ...items import EventScrapingItem
import re
import scrapy


class LetsDoThisSpider(BaseSpider):
    """Spider for https://www.letsdothis.com/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Processes all events from the all-events page.
    """
    name = "letsdothis"
    category = "fitness_training"
    site_name = "letsdothis"
    allowed_domains = ["letsdothis.com"]
    start_urls = [
        "https://www.letsdothis.com/gb/all-events?boundingBox=60.9%2C2.1%2C49.8%2C-8.9&startDate=1760572800000"
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
        # Try common API endpoints that might be used
        self.api_endpoints = [
            "https://www.letsdothis.com/api/events",
            "https://www.letsdothis.com/gb/api/events",
            "https://api.letsdothis.com/events",
            "https://www.letsdothis.com/gb/all-events?boundingBox=60.9%2C2.1%2C49.8%2C-8.9&startDate=1762128000000"
        ]
        
    def start_requests(self):
        """Override to add API endpoint requests."""
        # First, try the regular start URL
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.handle_error)
        
        # Also try common API endpoints
        for api_url in self.api_endpoints:
            self.logger.info(f"Trying API endpoint: {api_url}")
            yield scrapy.Request(api_url, callback=self.parse_api_response, errback=self.handle_error)

    def parse(self, response):
        """Parse the main page and extract event links and pagination."""
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        self.logger.info(f"Response body size: {len(response.body)} bytes")
        
        # Track this page as visited
        self.pages_visited.add(response.url)
        
        # Debug: Log page title and structure
        page_title = response.css('title::text').get()
        self.logger.info(f"Page title: {page_title}")
        
        # Try to extract events from embedded JSON/script tags (common for JS-rendered sites)
        import json
        script_tags = response.css('script::text').getall()
        for script in script_tags:
            if script and ('event' in script.lower() or 'id' in script.lower()):
                # Try to find JSON data
                try:
                    # Look for JSON-LD or embedded JSON objects
                    if '{' in script and '}' in script:
                        # Try to extract JSON objects
                        import re
                        json_patterns = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', script)
                        for json_str in json_patterns:
                            try:
                                data = json.loads(json_str)
                                # Check if it looks like event data
                                if isinstance(data, dict) and ('id' in data or 'name' in data or 'url' in data):
                                    self.logger.info(f"Found potential event data in script: {json_str[:200]}")
                            except json.JSONDecodeError:
                                continue
                except Exception as e:
                    self.logger.debug(f"Error parsing script tag: {e}")
        
        # Check for JSON-LD structured data
        json_ld = response.css('script[type="application/ld+json"]::text').getall()
        for ld_data in json_ld:
            try:
                data = json.loads(ld_data)
                if isinstance(data, dict) and data.get('@type') == 'Event' or (isinstance(data, list) and any(item.get('@type') == 'Event' for item in data if isinstance(item, dict))):
                    self.logger.info(f"Found JSON-LD event data")
                    # Extract event URL from JSON-LD
                    if isinstance(data, dict) and 'url' in data:
                        event_url = data.get('url')
                        if event_url and event_url not in self.seen_events:
                            self.seen_events.add(event_url)
                            yield response.follow(event_url, self.parse_event, errback=self.handle_error)
            except json.JSONDecodeError:
                continue
        
        # Check if page loads event data via JavaScript/API
        # Try to extract event links from various possible selectors
        all_links = response.css('a::attr(href)').getall()
        self.logger.info(f"Total links found on page: {len(all_links)}")
        
        # Find event links - common patterns for letsdothis.com
        event_links = []
        event_links_found = 0
        
        # Multiple patterns to check for event URLs
        event_patterns = ['/events/', '/e/', '/event/', '/gb/events/', '/gb/e/']
        
        for href in all_links:
            if href:
                # Convert to absolute URL
                absolute_url = response.urljoin(href)
                
                # Check if this is an event link
                is_event_link = any(pattern in href for pattern in event_patterns)
                
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
        
        self.logger.info(f"Total event links found from <a> tags: {event_links_found}")
        
        # Try to find event cards/data directly on the page (if events are loaded via JavaScript)
        # Common selectors for event listings on letsdothis.com
        event_card_selectors = [
            '.event-card',
            '[class*="event"]',
            '[data-event-id]',
            '.event-item',
            '[class*="EventCard"]',
            '[class*="Event"]',
            'article[class*="event"]',
            'div[class*="event-card"]',
            'a[href*="/events/"]',
            'a[href*="/e/"]',
            '[data-testid*="event"]',
            '[data-cy*="event"]'
        ]
        
        event_cards = None
        for selector in event_card_selectors:
            cards = response.css(selector)
            if cards:
                self.logger.info(f"Found {len(cards)} elements with selector: {selector}")
                event_cards = cards
                break
        
        if event_cards:
            self.logger.info(f"Found {len(event_cards)} event cards on page")
            for card in event_cards:
                # Extract event URL from card
                event_url = (
                    card.css('a::attr(href)').get() or
                    card.css('[href*="/events/"]::attr(href)').get() or
                    card.css('[href*="/e/"]::attr(href)').get() or
                    card.css('::attr(href)').get()
                )
                
                # If card itself is a link
                if not event_url:
                    card_href = card.css('::attr(href)').get()
                    if card_href and any(pattern in card_href for pattern in event_patterns):
                        event_url = card_href
                
                if event_url:
                    absolute_url = response.urljoin(event_url)
                    if absolute_url not in self.seen_events:
                        self.seen_events.add(absolute_url)
                        self.logger.info(f"Found event from card: {absolute_url}")
                        try:
                            yield response.follow(event_url, self.parse_event, errback=self.handle_error)
                        except Exception as e:
                            self.logger.error(f"Error following event card link {event_url}: {e}")
        else:
            self.logger.warning("No event cards found with any selector")
        
        # Try to find events in data attributes or other attributes
        data_event_elements = response.css('[data-event-id], [data-event-url], [data-event], [data-id]')
        if data_event_elements:
            self.logger.info(f"Found {len(data_event_elements)} elements with data-event attributes")
            for elem in data_event_elements:
                event_url = (
                    elem.css('::attr(data-event-url)').get() or
                    elem.css('::attr(href)').get() or
                    elem.css('a::attr(href)').get()
                )
                if event_url:
                    absolute_url = response.urljoin(event_url)
                    if absolute_url not in self.seen_events:
                        self.seen_events.add(absolute_url)
                        self.logger.info(f"Found event from data attribute: {absolute_url}")
                        try:
                            yield response.follow(event_url, self.parse_event, errback=self.handle_error)
                        except Exception as e:
                            self.logger.error(f"Error following data-event link {event_url}: {e}")
        
        # Log all hrefs containing 'event' for debugging
        event_related_links = [href for href in all_links if href and 'event' in href.lower()]
        if event_related_links:
            self.logger.info(f"Found {len(event_related_links)} links containing 'event': {event_related_links[:10]}")
        
        if event_links_found == 0 and not event_cards and len(event_related_links) == 0:
            self.logger.warning("No events found! Page might be JavaScript-rendered. Trying to find API endpoints...")
            # Look for API endpoints in script tags
            for script in script_tags:
                # Look for fetch, axios, or API calls
                import re
                api_patterns = [
                    r'["\'](https?://[^"\']*api[^"\']*events[^"\']*)["\']',
                    r'["\'](/[^"\']*api[^"\']*events[^"\']*)["\']',
                    r'fetch\(["\']([^"\']*events[^"\']*)["\']',
                    r'axios\.(get|post)\(["\']([^"\']*events[^"\']*)["\']',
                ]
                for pattern in api_patterns:
                    matches = re.findall(pattern, script, re.IGNORECASE)
                    if matches:
                        for match in matches:
                            api_url = match if isinstance(match, str) else match[0] if isinstance(match, tuple) else None
                            if api_url:
                                if not api_url.startswith('http'):
                                    api_url = response.urljoin(api_url)
                                self.logger.info(f"Found potential API endpoint in script: {api_url}")
                                # Try to call the API
                                try:
                                    yield scrapy.Request(api_url, callback=self.parse_api_response, errback=self.handle_error)
                                except Exception as e:
                                    self.logger.error(f"Error requesting API endpoint {api_url}: {e}")
            # Save response body for debugging
            self.logger.debug(f"Page HTML sample (first 500 chars): {response.text[:500]}")
        
        # Check for pagination - letsdothis.com might use different pagination patterns
        pagination_links = []
        pagination_selectors = [
            'a[aria-label="Next"]::attr(href)',
            'a[rel="next"]::attr(href)',
            '.pagination a.next::attr(href)',
            '.pagination-next::attr(href)',
            '[class*="next"] a::attr(href)',
            '[class*="pagination"] a::attr(href)',
            'button[aria-label="Next page"]::attr(data-href)',
            '[data-next-page]::attr(data-next-page)'
        ]
        
        for selector in pagination_selectors:
            next_link = response.css(selector).get()
            if next_link:
                absolute_url = response.urljoin(next_link)
                if absolute_url not in self.pages_visited:
                    pagination_links.append(absolute_url)
                    break
        
        # Also check for "Load More" or "Show More" buttons
        load_more = response.css('[class*="load-more"], [class*="LoadMore"], button:contains("Load more"), button:contains("Show more")').get()
        if load_more:
            # Some sites use data attributes for next page
            next_url = load_more.css('::attr(data-url), ::attr(data-href)').get()
            if next_url:
                absolute_url = response.urljoin(next_url)
                if absolute_url not in self.pages_visited:
                    pagination_links.append(absolute_url)
        
        # Follow pagination links
        for next_page in pagination_links:
            if next_page:
                self.logger.info(f"Following pagination: {next_page}")
                try:
                    yield response.follow(next_page, self.parse, errback=self.handle_error)
                except Exception as e:
                    self.logger.error(f"Error following pagination link {next_page}: {e}")
        

    def parse_api_response(self, response):
        """Parse API JSON response for events."""
        self.logger.info(f"Parsing API response: {response.url}")
        try:
            import json
            data = response.json()
            
            # Handle different API response structures
            events = []
            if isinstance(data, list):
                events = data
            elif isinstance(data, dict):
                # Common API response structures
                events = (
                    data.get('events', []) or
                    data.get('data', []) or
                    data.get('items', []) or
                    data.get('results', []) or
                    [data]  # Single event
                )
            
            self.logger.info(f"Found {len(events)} events in API response")
            
            for event in events:
                if not isinstance(event, dict):
                    continue
                
                # Extract event URL from API response
                event_url = (
                    event.get('url') or
                    event.get('eventUrl') or
                    event.get('link') or
                    event.get('permalink')
                )
                
                # If URL is relative, make it absolute
                if event_url and not event_url.startswith('http'):
                    event_url = f"https://www.letsdothis.com{event_url}" if event_url.startswith('/') else f"https://www.letsdothis.com/{event_url}"
                
                # If we have event data directly, create item from it
                if event_url:
                    if event_url not in self.seen_events:
                        self.seen_events.add(event_url)
                        yield response.follow(event_url, self.parse_event, errback=self.handle_error)
                elif event.get('id') or event.get('slug'):
                    # Try to construct URL from ID or slug
                    event_id = event.get('id') or event.get('slug')
                    potential_url = f"https://www.letsdothis.com/gb/e/{event_id}"
                    if potential_url not in self.seen_events:
                        self.seen_events.add(potential_url)
                        yield response.follow(potential_url, self.parse_event, errback=self.handle_error)
                else:
                    # Create item directly from API data
                    item = self.create_item_from_api_data(event)
                    if item:
                        item_key = f"{item['name']}_{item['date']}" if item.get('name') and item.get('date') else None
                        if item_key and item_key not in self.seen_events:
                            self.seen_events.add(item_key)
                            self.total_items_scraped += 1
                            yield item
        
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse API response as JSON: {e}")
        except Exception as e:
            self.logger.error(f"Error parsing API response: {e}")

    def create_item_from_api_data(self, event_data):
        """Create an EventScrapingItem from API event data."""
        if not isinstance(event_data, dict):
            return None
        
        item = EventScrapingItem()
        item['category'] = self.category
        item['site'] = self.site_name
        
        # Extract data from API response
        item['name'] = self.clean_text(
            event_data.get('name') or
            event_data.get('title') or
            event_data.get('eventName')
        )
        
        item['url'] = (
            event_data.get('url') or
            event_data.get('eventUrl') or
            event_data.get('link') or
            event_data.get('permalink') or
            (f"https://www.letsdothis.com/gb/e/{event_data.get('id') or event_data.get('slug')}" if (event_data.get('id') or event_data.get('slug')) else None)
        )
        
        # Extract date
        raw_date = (
            event_data.get('date') or
            event_data.get('eventDate') or
            event_data.get('startDate') or
            event_data.get('dateTime')
        )
        item['raw_date'] = raw_date
        item['date'] = self.convert_date_format(raw_date) if raw_date else None
        
        # Extract description
        description = (
            event_data.get('description') or
            event_data.get('shortDescription') or
            event_data.get('summary') or
            event_data.get('about')
        )
        if description:
            short_desc = description[:200] + '...' if len(description) > 200 else description
            item['short_description'] = self.clean_text(short_desc)
        else:
            item['short_description'] = None
        
        # Extract address
        location = event_data.get('location') or event_data.get('venue') or {}
        if isinstance(location, dict):
            address_parts = [
                location.get('address'),
                location.get('city'),
                location.get('state'),
                location.get('postcode'),
                location.get('country')
            ]
            address = ', '.join(filter(None, address_parts))
            item['address'] = self.clean_text(address) if address else None
        elif isinstance(location, str):
            item['address'] = self.clean_text(location)
        else:
            item['address'] = None
        
        # Extract coordinates
        coords = None
        if isinstance(location, dict):
            if location.get('latitude') and location.get('longitude'):
                coords = {
                    'lat': float(location['latitude']),
                    'lon': float(location['longitude'])
                }
            elif location.get('lat') and location.get('lon'):
                coords = {
                    'lat': float(location['lat']),
                    'lon': float(location['lon'])
                }
        item['coordinates'] = coords
        
        # Determine category
        title = item['name'] or ''
        desc = item['short_description'] or ''
        item['category'], item['subcategory'] = self.get_event_category(title, [desc] if desc else [])
        
        item['raw'] = event_data
        
        return item if item['name'] else None

    def parse_event(self, response):
        """Parse individual event pages to extract event details."""
        self.logger.info(f"Parsing event page: {response.url}")
        
        item = EventScrapingItem()
        item['category'] = self.category
        item['site'] = self.site_name
        item['url'] = response.url

        # Extract title - try multiple selectors for letsdothis.com
        title = (
            response.css('h1::text').get() or
            response.css('[class*="event-title"]::text, [class*="EventTitle"]::text').get() or
            response.css('[class*="title"]::text').get() or
            response.css('.event-name::text, .event-name h1::text').get() or
            response.css('title::text').get()
        )
        
        if title:
            title = title.strip()
        
        # Enhanced description extraction
        desc_parts = (
            response.css('[class*="description"] *::text, [class*="Description"] *::text').getall() or
            response.css('.event-description *::text, .event-details *::text').getall() or
            response.css('[class*="content"] *::text, [class*="Content"] *::text').getall() or
            response.css('article *::text, .event-info *::text').getall() or
            response.css('p::text, [class*="text"]::text').getall()
        )
        
        # Enhanced date extraction with multiple selectors
        date_selectors = [
            ('time::attr(datetime)', response.css('time::attr(datetime)').get()),
            ('time::text', response.css('time::text').get()),
            ('[class*="date"]::text, [class*="Date"]::text', response.css('[class*="date"]::text, [class*="Date"]::text').get()),
            ('[class*="event-date"]::text', response.css('[class*="event-date"]::text').get()),
            ('[data-date]::attr(data-date)', response.css('[data-date]::attr(data-date)').get()),
            ('[class*="when"]::text', response.css('[class*="when"]::text').get()),
            ('[class*="event-info"] [class*="date"]::text', response.css('[class*="event-info"] [class*="date"]::text').get()),
        ]
        
        date = None
        used_selector = None
        for selector_name, selector_result in date_selectors:
            if selector_result:
                date = selector_result.strip() if isinstance(selector_result, str) else selector_result
                used_selector = selector_name
                self.logger.debug(f"Found date using selector '{selector_name}': {date}")
                break
        
        # If still no date, try to extract from description
        if not date and desc_parts:
            desc_text = ' '.join(desc_parts)
            
            # Look for date patterns in description
            date_patterns = [
                r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                r'(\d{1,2}/\d{1,2}/\d{4})',
                r'(\d{1,2}-\d{1,2}-\d{4})',
                r'(\d{4}-\d{1,2}-\d{1,2})'
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
        
        # Short description: first 2 sentences or first 200 chars
        short_description = None
        if desc_parts:
            joined = ' '.join(desc_parts).strip()
            # Try to split by sentences
            sentences = re.split(r'[.!?]\s+', joined)
            if sentences:
                short_description = '. '.join(sentences[:2])
                if len(short_description) > 200:
                    short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
            else:
                short_description = joined[:200] + '...' if len(joined) > 200 else joined
            
            # If still no date, try to extract from short description
            if not date and short_description:
                date_patterns = [
                    r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                    r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                    r'(\d{1,2}/\d{1,2}/\d{4})',
                    r'(\d{1,2}-\d{1,2}-\d{4})',
                    r'(\d{4}-\d{1,2}-\d{1,2})'
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, short_description, re.IGNORECASE)
                    if match:
                        date = match.group(1)
                        raw_date = date
                        date = self.convert_date_format(date)
                        break

        # Address extraction
        address = self.extract_address(response)
        
        # Coordinates extraction using multiple heuristics
        coords = self.extract_coordinates(response)
        
        # Build event_data for database check before geocoding
        event_data = {
            'name': title,
            'date': date,
            'url': response.url
        }
        
        # If no coordinates found, try to get them from address using geocoding
        # Pass event_data to enable database check before geocoding
        if not coords and address:
            coords = self.geocode_address(address, event_data=event_data)
            if coords:
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
        
        yield item

    def is_target_race_type(self, title, description_parts):
        """Check if the event matches any of our target categories."""
        if not title:
            return True  # Include all events if no title filtering is needed
        
        # Combine title and description for analysis
        full_text = title.lower()
        if description_parts:
            full_text += " " + " ".join(description_parts).lower()
        
        # Check for any matching keywords
        for keyword in self.ALL_KEYWORDS:
            if keyword.lower() in full_text:
                return True
        
        # Include all events for letsdothis.com (no filtering)
        return True
    
    def get_event_category(self, title, description_parts):
        """Determine the specific category and subcategory for an event."""
        if not title:
            return "Other", "General"
        
        # Combine title and description for analysis
        full_text = title.lower()
        if description_parts:
            full_text += " " + " ".join(description_parts).lower()
        
        # Check each category group and subcategory
        for category_group, subcategories in self.CATEGORY_KEYWORDS.items():
            for subcategory, keywords in subcategories.items():
                for keyword in keywords:
                    if keyword.lower() in full_text:
                        return category_group, subcategory
        
        return "Other", "General"

    def extract_address(self, response):
        """Extract full address from the page using multiple heuristics."""
        # Try multiple selectors for address
        address_selectors = [
            '[class*="address"]::text, [class*="Address"]::text',
            '[class*="location"]::text, [class*="Location"]::text',
            '[class*="venue"]::text, [class*="Venue"]::text',
            '[class*="event-location"]::text',
            '[class*="event-venue"]::text',
            '[class*="event-address"]::text',
            'address::text',
            '[itemprop="address"]::text',
            '[data-location]::attr(data-location)',
            '[data-venue]::attr(data-venue)',
            '[data-address]::attr(data-address)'
        ]
        
        for selector in address_selectors:
            address = response.css(selector).get()
            if address and len(address.strip()) > 5:
                return self.clean_text(address)
        
        # Try to extract from description or content
        content_text = ' '.join(response.css('*::text').getall())
        
        # Look for common address patterns
        # UK postcode pattern
        postcode_pattern = r'[A-Z]{1,2}\d[A-Z\d]? \d[A-Z]{2}'
        postcode_match = re.search(postcode_pattern, content_text, re.IGNORECASE)
        
        if postcode_match:
            # Try to get text around the postcode
            postcode = postcode_match.group()
            start = max(0, postcode_match.start() - 100)
            end = min(len(content_text), postcode_match.end() + 100)
            address_candidate = content_text[start:end].strip()
            
            if len(address_candidate) > 10:
                return self.clean_text(address_candidate)
        
        # Look for common address keywords
        address_keywords = ['street', 'road', 'avenue', 'lane', 'close', 'drive', 'way', 'place']
        for keyword in address_keywords:
            if keyword in content_text.lower():
                # Try to extract text around the keyword
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
            date_str = str(date_str).strip()
            
            # Month name mappings
            month_names = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12',
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
            }
            
            # Handle ISO datetime format (e.g., "2025-11-29T00:00:00Z")
            iso_pattern = r'(\d{4})-(\d{1,2})-(\d{1,2})'
            iso_match = re.search(iso_pattern, date_str)
            if iso_match:
                year, month, day = iso_match.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Pattern 1: DD Month YYYY (e.g., "30 October 2025")
            pattern1 = r'(\d{1,2})\s+(\w+)\s+(\d{4})'
            match1 = re.search(pattern1, date_str, re.IGNORECASE)
            if match1:
                day, month_name, year = match1.groups()
                month_num = month_names.get(month_name.lower())
                if month_num:
                    return f"{month_num}/{day.zfill(2)}/{year}"
            
            # Pattern 2: DD/MM/YYYY (e.g., "30/10/2025")
            pattern2 = r'(\d{1,2})/(\d{1,2})/(\d{4})'
            match2 = re.search(pattern2, date_str)
            if match2:
                day, month, year = match2.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Pattern 3: DD-MM-YYYY (e.g., "30-10-2025")
            pattern3 = r'(\d{1,2})-(\d{1,2})-(\d{4})'
            match3 = re.search(pattern3, date_str)
            if match3:
                day, month, year = match3.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Pattern 4: YYYY-MM-DD (e.g., "2025-10-30")
            pattern4 = r'(\d{4})-(\d{1,2})-(\d{1,2})'
            match4 = re.search(pattern4, date_str)
            if match4:
                year, month, day = match4.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Pattern 5: MM/DD/YYYY (already in correct format)
            pattern5 = r'(\d{1,2})/(\d{1,2})/(\d{4})'
            match5 = re.search(pattern5, date_str)
            if match5:
                month, day, year = match5.groups()
                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
            # Try to parse with datetime and convert
            try:
                # Try common formats
                formats = [
                    '%d %B %Y', '%d %b %Y', '%d/%m/%Y', '%d-%m-%Y',
                    '%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y', '%b %d, %Y',
                    '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S%z'
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
                        lat = coords_part[0]
                        lon = coords_part[1]
                        break
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
                        # Extract coordinates from iframe src
                        coord_pattern = r'@(-?\d+\.?\d*),(-?\d+\.?\d*)'
                        match = re.search(coord_pattern, iframe)
                        if match:
                            lat, lon = match.groups()
                            break
                    except Exception:
                        continue

        # 6) Try to extract from JavaScript or embedded data
        if not lat or not lon:
            content_text = ' '.join(response.css('*::text').getall())
            
            # Look for coordinate patterns in text
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
                            # Single coordinate found, look for the other
                            coord = float(matches[0])
                            if -90 <= coord <= 90:  # Likely latitude
                                lat = str(coord)
                                # Look for longitude
                                lon_matches = re.findall(r'lon[gitude]*[:\s]*(-?\d+\.?\d*)', content_text, re.IGNORECASE)
                                if lon_matches:
                                    lon = lon_matches[0]
                            else:  # Likely longitude
                                lon = str(coord)
                                # Look for latitude
                                lat_matches = re.findall(r'lat[itude]*[:\s]*(-?\d+\.?\d*)', content_text, re.IGNORECASE)
                                if lat_matches:
                                    lat = lat_matches[0]
                        break
                    except Exception:
                        continue

        # final normalization
        try:
            if lat and lon:
                lat_f = float(lat.strip())
                lon_f = float(lon.strip())
                # Validate coordinate ranges
                if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                    return {'lat': lat_f, 'lon': lon_f}
        except Exception:
            pass

        return None

