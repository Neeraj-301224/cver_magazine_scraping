import scrapy
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class RunGuidesSpider(BaseSpider):
    """Spider for https://www.runguides.com/uk/runs

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Filters for specific race types: 5K, 10K, Half Marathon, Ultras
    """
    name = "runguides"
    category = "fitness_training"
    site_name = "runguides"
    allowed_domains = ["runguides.com"]
    start_urls = [
        "https://www.runguides.com/uk/runs",
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
        # UK-only filter: only process URLs containing /uk/
        self.uk_only = True
    
    def handle_error(self, failure):
        """Custom error handling for failed requests. Handles 404s gracefully for API endpoints."""
        try:
            url = failure.request.url
        except Exception:
            url = "<unknown>"
        
        # Check if this is an HTTP error with a response
        if hasattr(failure.value, 'response'):
            response = failure.value.response
            status = response.status
            
            # For API endpoints, 404s are expected - log as debug/info instead of error
            if '/api/' in url and status == 404:
                self.logger.debug(f"API endpoint not found (expected): {url} (404)")
                return failure
            
            # For other 404s, log as warning
            if status == 404:
                self.logger.warning(f"Page not found: {url} (404)")
                return failure
            
            # For other HTTP errors, log as error
            self.logger.error(f"Request failed: {url}")
            self.logger.error(f"Error type: {failure.type}")
            self.logger.error(f"Response status: {status}")
        else:
            # For non-HTTP errors, log as error
            self.logger.error(f"Request failed: {url}")
            self.logger.error(f"Error type: {failure.type}")
            self.logger.error(f"Error value: {failure.value}")
        
        return failure

    def is_uk_url(self, url):
        """Check if URL is UK-specific. Returns True only for UK URLs."""
        if not url:
            return False
        url_lower = url.lower()
        # Must contain /uk/ and must NOT contain other regions
        excluded_regions = ['/north-america', '/northamerica', '/us/', '/usa/', '/canada/', '/ca/']
        if '/uk/' in url_lower:
            # Check it's not excluded
            if any(region in url_lower for region in excluded_regions):
                return False
            return True
        return False
    
    def parse(self, response):
        """Parse the main page and extract event links and pagination."""
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # Filter: Only process UK URLs
        if self.uk_only and not self.is_uk_url(response.url):
            self.logger.warning(f"Skipping non-UK URL: {response.url}")
            return
        
        # Track this page as visited
        self.pages_visited.add(response.url)
        
        # First, try to find API endpoints or embedded data in JavaScript
        script_tags = response.css('script::text').getall()
        api_endpoints_found = []
        
        # Look for API endpoints in script tags
        import re
        import json
        
        for script in script_tags:
            if not script:
                continue
                
            # Look for API endpoints
            api_patterns = [
                r'["\'](https?://[^"\']*api[^"\']*listings[^"\']*)["\']',
                r'["\'](https?://[^"\']*api[^"\']*events[^"\']*)["\']',
                r'["\'](https?://[^"\']*api[^"\']*runs[^"\']*)["\']',
                r'["\'](/[^"\']*api[^"\']*listings[^"\']*)["\']',
                r'["\'](/[^"\']*api[^"\']*events[^"\']*)["\']',
                r'["\'](/[^"\']*api[^"\']*runs[^"\']*)["\']',
                r'fetch\(["\']([^"\']*listings[^"\']*)["\']',
                r'fetch\(["\']([^"\']*events[^"\']*)["\']',
                r'fetch\(["\']([^"\']*runs[^"\']*)["\']',
                r'\.get\(["\']([^"\']*listings[^"\']*)["\']',
                r'\.get\(["\']([^"\']*events[^"\']*)["\']',
                r'\.get\(["\']([^"\']*runs[^"\']*)["\']',
            ]
            
            for pattern in api_patterns:
                matches = re.findall(pattern, script, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[-1] if match else None
                    if match and match not in api_endpoints_found:
                        api_endpoints_found.append(match)
                        self.logger.info(f"Found potential API endpoint: {match}")
        
        # Try common RunGuides API patterns (UK only)
        base_url = "https://www.runguides.com"
        common_api_patterns = [
            f"{base_url}/api/uk/listings",
            f"{base_url}/api/uk/events",
            f"{base_url}/api/uk/runs",
            f"{base_url}/uk/api/listings",
            f"{base_url}/uk/api/events",
            f"{base_url}/uk/api/runs",
            f"{base_url}/api/listings?region=uk",
            f"{base_url}/api/listings?country=uk",
            f"{base_url}/api/listings?location=uk",
        ]
        
        # Try to extract listings from embedded JavaScript variables
        listings_data = None
        for script in script_tags:
            if not script:
                continue
            # Look for listings array or data structure
            # Pattern: var listings = [...] or listings: [...] or "listings": [...]
            # Also handle multi-line JSON and complex structures
            listing_patterns = [
                r'listings\s*[:=]\s*(\[[^\]]+\])',
                r'"listings"\s*:\s*(\[[^\]]+\])',
                r"'listings'\s*:\s*(\[[^\]]+\])",
                r'var\s+listings\s*=\s*(\[[^\]]+\])',
                r'listings\s*[:=]\s*(\[.*?\])',  # More flexible with .*?
                r'"listings"\s*:\s*(\[.*?\])',
                # Try to find any array with event-like data
                r'(\[[^\[\]]*"name"[^\[\]]*\])',
                r'(\[[^\[\]]*"title"[^\[\]]*\])',
                # Try to find ng-init or data attributes with listings
                r'ng-init\s*=\s*["\']([^"\']*listings[^"\']*)["\']',
                r'data-listings\s*=\s*["\']([^"\']+)["\']',
            ]
            
            for pattern in listing_patterns:
                match = re.search(pattern, script, re.IGNORECASE | re.DOTALL)
                if match:
                    try:
                        listings_json = match.group(1)
                        # Try to clean up the JSON if needed
                        listings_json = listings_json.strip()
                        listings_data = json.loads(listings_json)
                        if isinstance(listings_data, list) and len(listings_data) > 0:
                            self.logger.info(f"Found listings data in JavaScript: {len(listings_data)} items")
                            break
                    except json.JSONDecodeError:
                        # Try to extract a more complete JSON structure
                        # Look for the full object containing listings
                        try:
                            # Try to find a larger JSON structure
                            obj_pattern = r'\{[^{}]*"listings"\s*:\s*(\[.*?\])[^{}]*\}'
                            obj_match = re.search(obj_pattern, script, re.IGNORECASE | re.DOTALL)
                            if obj_match:
                                listings_json = obj_match.group(1)
                                listings_data = json.loads(listings_json)
                                if isinstance(listings_data, list) and len(listings_data) > 0:
                                    self.logger.info(f"Found listings data in JavaScript object: {len(listings_data)} items")
                                    break
                        except:
                            continue
            if listings_data:
                break
        
        # Also try to extract from data attributes in HTML
        if not listings_data:
            # Look for data attributes that might contain listings
            data_listings = response.css('[data-listings]::attr(data-listings)').get()
            if data_listings:
                try:
                    listings_data = json.loads(data_listings)
                    if isinstance(listings_data, list) and len(listings_data) > 0:
                        self.logger.info(f"Found listings data in data attribute: {len(listings_data)} items")
                except:
                    pass
        
        # If we found listings data in JavaScript, process it
        if listings_data and isinstance(listings_data, list):
            self.logger.info(f"Processing {len(listings_data)} listings from JavaScript/data")
            for listing in listings_data:
                if isinstance(listing, dict):
                    # Extract event URL from listing
                    event_url = listing.get('url') or listing.get('link') or listing.get('slug')
                    if event_url:
                        if not event_url.startswith('http'):
                            # Construct UK URL if it's a relative path
                            if event_url.startswith('/'):
                                event_url = f"https://www.runguides.com{event_url}"
                            else:
                                event_url = response.urljoin(event_url)
                        
                        # UK-only filter: Only process UK URLs
                        if self.uk_only and not self.is_uk_url(event_url):
                            self.logger.debug(f"Skipping non-UK listing from JavaScript: {event_url}")
                            continue
                        
                        # Also try to extract data directly if available
                        if listing.get('name') or listing.get('title'):
                            # We have enough data to create an item directly
                            item = self.create_item_from_listing(listing, event_url)
                            if item:
                                self.logger.info(f"Created item from listing: {item.get('name', 'N/A')}")
                                yield item
                        else:
                            # Follow the link to get full details
                            if event_url not in self.seen_events:
                                self.seen_events.add(event_url)
                                self.logger.info(f"Following event URL: {event_url}")
                                yield response.follow(event_url, self.parse_event, errback=self.handle_error)
        
        # Try API endpoints if we found any (only UK ones)
        for api_url in api_endpoints_found:
            if not api_url.startswith('http'):
                api_url = response.urljoin(api_url)
            # Only process UK API endpoints
            if self.uk_only and not self.is_uk_url(api_url):
                self.logger.debug(f"Skipping non-UK API endpoint: {api_url}")
                continue
            self.logger.info(f"Trying API endpoint: {api_url}")
            yield scrapy.Request(api_url, callback=self.parse_api_response, errback=self.handle_error, 
                                headers={'Accept': 'application/json'})
        
        # Also try common API patterns (only UK ones)
        for api_pattern in common_api_patterns:
            if api_pattern not in api_endpoints_found:
                # Only process UK API endpoints
                if self.uk_only and not self.is_uk_url(api_pattern):
                    continue
                self.logger.info(f"Trying common API pattern: {api_pattern}")
                yield scrapy.Request(api_pattern, callback=self.parse_api_response, errback=self.handle_error,
                                    headers={'Accept': 'application/json', 'Referer': response.url})
        
        # Try to find API endpoint from network requests pattern
        # Look for common API patterns in script tags
        api_url_patterns = [
            r'["\']([^"\']*\/api\/[^"\']*listings[^"\']*)["\']',
            r'["\']([^"\']*\/api\/[^"\']*events[^"\']*)["\']',
            r'["\']([^"\']*\/api\/[^"\']*runs[^"\']*)["\']',
            r'url\s*[:=]\s*["\']([^"\']*\/api\/[^"\']*)["\']',
            r'endpoint\s*[:=]\s*["\']([^"\']*\/api\/[^"\']*)["\']',
        ]
        
        for script in script_tags:
            if not script:
                continue
            for pattern in api_url_patterns:
                matches = re.findall(pattern, script, re.IGNORECASE)
                for match in matches:
                    if match and match not in api_endpoints_found:
                        # Make it absolute if relative
                        if not match.startswith('http'):
                            if match.startswith('/'):
                                match = f"{base_url}{match}"
                            else:
                                match = response.urljoin(match)
                        if match not in api_endpoints_found:
                            api_endpoints_found.append(match)
                            self.logger.info(f"Found API endpoint from script: {match}")
                            yield scrapy.Request(match, callback=self.parse_api_response, errback=self.handle_error,
                                                headers={'Accept': 'application/json', 'Referer': response.url})
        
        # Fallback: Find event links on the homepage or listings
        all_links = response.css('a::attr(href)').getall()
        event_links_found = 0
        event_links = []
        
        # Common patterns for RunGuides event URLs
        event_patterns = ['/uk/runs/', '/runs/', '/event/', '/events/', '/race/', '/races/']
        
        for href in all_links:
            if href:
                # Convert to absolute URL
                absolute_url = response.urljoin(href)
                
                # UK-only filter: Skip non-UK URLs
                if self.uk_only and not self.is_uk_url(absolute_url):
                    continue
                
                # Check if this is an event link
                is_event_link = False
                
                # Check if URL contains event-like patterns but exclude common non-event pages
                if any(pattern in href for pattern in event_patterns):
                    # Exclude common non-event pages and non-UK regions
                    excluded_patterns = ['/about', '/contact', '/faq', '/login', '/signup', '/cart', 
                                        '/wishlist', '/results', '/photos', '/videos', '/blog', 
                                        '/news', '/terms', '/privacy', '/cookie', '/search',
                                        '/distances/', '/regions/', '/cities/', '/venues/', '/series/',
                                        '/gift', '/membership', '/calendar', '/race-info', '/corporate',
                                        '/foundation', '/kit', '/coach', '/retreats', '/charity',
                                        '/partners', '/volunteer', '/careers', '/sustainability',
                                        '/community', '/pacing', '/prizes', '/club', '/injury',
                                        '/tips', '/fundraising', '/partners', '/uk/runs',  # Exclude the main listing page
                                        '/north-america', '/northamerica']  # Exclude North America
                    
                    # If it's an event URL and not excluded, it might be an event
                    if not any(excluded in href for excluded in excluded_patterns):
                        # For UK-only scraping, only process URLs with /uk/runs/
                        if '/uk/runs/' in href and href != '/uk/runs' and href != '/uk/runs/':
                            # Check if there's a slug after /uk/runs/
                            parts = href.split('/uk/runs/')
                            if len(parts) > 1 and parts[1] and parts[1].strip():
                                is_event_link = True
                        # Don't process generic /runs/ links (they might be from other regions)
                        elif '/runs/' in href and '/uk/' not in href:
                            # Skip non-UK runs links
                            continue
                
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
        
        # If no event links found, log a warning
        if event_links_found == 0 and not listings_data:
            self.logger.warning("No event links or listings data found on page. This might be a JavaScript-heavy page.")
            # Try to save a sample of the HTML for debugging
            self.logger.debug(f"Page HTML length: {len(response.text)} characters")
            # Check if page contains AngularJS indicators
            if 'ng-app' in response.text or 'ng-controller' in response.text or '{{' in response.text:
                self.logger.warning("Page appears to be AngularJS-based. Content may be loaded dynamically.")
        
        # Try to find event cards/data directly on the page
        event_card_selectors = [
            'a[href*="/runs/"]',
            'a[href*="/event/"]',
            'a[href*="/race/"]',
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
                        # UK-only filter: Skip non-UK URLs
                        if self.uk_only and not self.is_uk_url(absolute_url):
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
                # UK-only filter: Skip non-UK pagination links
                if self.uk_only and not self.is_uk_url(next_page):
                    self.logger.debug(f"Skipping non-UK pagination link: {next_page}")
                    continue
                self.logger.info(f"Following pagination: {next_page}")
                try:
                    yield response.follow(next_page, self.parse, errback=self.handle_error)
                except Exception as e:
                    self.logger.error(f"Error following pagination link {next_page}: {e}")
        

    def parse_api_response(self, response):
        """Parse API response containing listings data."""
        self.logger.info(f"Parsing API response: {response.url}")
        self.logger.debug(f"API response status: {response.status}")
        
        # UK-only filter: Skip non-UK API responses
        if self.uk_only and not self.is_uk_url(response.url):
            self.logger.warning(f"Skipping non-UK API response: {response.url}")
            return
        
        # Skip non-200 responses
        if response.status != 200:
            self.logger.warning(f"API returned non-200 status: {response.status} for {response.url}")
            return
        
        try:
            import json
            data = json.loads(response.text)
            
            # Handle different API response formats
            listings = []
            if isinstance(data, list):
                listings = data
            elif isinstance(data, dict):
                # Try common keys
                listings = (data.get('listings') or 
                          data.get('events') or 
                          data.get('runs') or 
                          data.get('data') or 
                          data.get('results') or
                          data.get('items') or
                          data.get('content') or [])
            
            self.logger.info(f"Found {len(listings)} listings in API response from {response.url}")
            
            # Filter listings to only UK ones
            uk_listings = []
            for listing in listings:
                if isinstance(listing, dict):
                    # Extract event URL
                    event_url = (listing.get('url') or 
                                listing.get('link') or 
                                listing.get('slug') or
                                listing.get('id'))
                    
                    if event_url:
                        if not event_url.startswith('http'):
                            # Construct URL from slug or id (always use UK path)
                            if listing.get('slug'):
                                event_url = f"https://www.runguides.com/uk/runs/{listing.get('slug')}"
                            elif listing.get('id'):
                                event_url = f"https://www.runguides.com/uk/runs/{listing.get('id')}"
                            else:
                                event_url = response.urljoin(event_url)
                        
                        # UK-only filter: Only process UK URLs
                        if self.uk_only and not self.is_uk_url(event_url):
                            self.logger.debug(f"Skipping non-UK listing: {event_url}")
                            continue
                        
                        # If we have enough data, create item directly
                        if listing.get('name') or listing.get('title'):
                            item = self.create_item_from_listing(listing, event_url)
                            if item:
                                yield item
                        else:
                            # Follow link to get full details
                            if event_url not in self.seen_events:
                                self.seen_events.add(event_url)
                                yield response.follow(event_url, self.parse_event, errback=self.handle_error)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse API response as JSON: {e}")
            self.logger.debug(f"Response text (first 500 chars): {response.text[:500]}")
        except Exception as e:
            self.logger.error(f"Error parsing API response: {e}")

    def create_item_from_listing(self, listing, event_url):
        """Create an EventScrapingItem from a listing dictionary."""
        item = EventScrapingItem()
        item['category'] = self.category
        item['site'] = self.site_name
        item['url'] = event_url
        
        # Extract title/name
        title = listing.get('name') or listing.get('title') or listing.get('event_name')
        
        # Extract date
        date = (listing.get('date') or 
               listing.get('event_date') or 
               listing.get('listing_date') or
               listing.get('start_date'))
        raw_date = date
        
        # Convert date format if needed
        if date:
            date = self.convert_date_format(str(date))
        
        # Extract description
        short_description = (listing.get('description') or 
                           listing.get('short_description') or 
                           listing.get('tagline') or
                           listing.get('summary'))
        
        # Extract address
        address = (listing.get('address') or 
                  listing.get('location') or 
                  listing.get('venue') or
                  listing.get('geocoded_city'))
        
        # Extract coordinates
        coords = None
        if listing.get('latitude') and listing.get('longitude'):
            try:
                coords = {
                    'lat': float(listing.get('latitude')),
                    'lon': float(listing.get('longitude'))
                }
            except (ValueError, TypeError):
                pass
        
        # Extract category and subcategory
        event_category, event_subcategory = self.get_event_category(title, [short_description] if short_description else [])
        
        # Clean fields
        cleaned_title = self.clean_text(title) if title else None
        cleaned_description = self.clean_text(short_description) if short_description else None
        
        # Set item fields
        item['name'] = cleaned_title
        item['date'] = date
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
            'full_description': short_description,
            'address': address,
            'coordinates': coords,
        }
        
        # Check for duplicates (use URL as key for listings since date might be missing)
        item_key = item['url'] if item['url'] else f"{item['name']}_{item['date']}"
        if item_key in self.seen_events:
            self.logger.debug(f"Skipping duplicate item: {item['name']}")
            return None
        
        # Add to seen items
        self.seen_events.add(item_key)
        self.total_items_scraped += 1
        
        self.logger.info(f"Event extracted from listing - Name: {item['name'][:50] if item['name'] else 'N/A'}...")
        
        return item

    def parse_event(self, response):
        """Parse individual event pages to extract event details."""
        self.logger.info(f"Parsing event page: {response.url}")
        self.logger.debug(f"Event page status: {response.status}")
        
        # UK-only filter: Skip non-UK event pages
        if self.uk_only and not self.is_uk_url(response.url):
            self.logger.warning(f"Skipping non-UK event page: {response.url}")
            return
        
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
        desc_parts = (
            response.css('.description *::text, .event-description *::text, .content *::text').getall() or
            response.css('article *::text, .event-details *::text').getall() or
            response.css('p::text, .text::text').getall() or
            response.css('[class*="description"] *::text').getall()
        )
        
        # Enhanced date extraction
        date = None
        used_selector = None
        
        # Try various date selectors
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
        # Only check if we have a title, otherwise allow through (might be missing data)
        if title and not self.is_target_race_type(title, desc_parts):
            self.logger.info(f"Event does not match target race types - skipping: {title}")
            self.logger.debug(f"Title: {title}, Description preview: {desc_parts[:3] if desc_parts else 'None'}")
            return

        # Short description extraction
        short_description = None
        if desc_parts:
            import re
            joined = '\n'.join(desc_parts).strip()
            short_description = joined.split('\n')[0]
            if len(short_description) > 200:
                short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
            
            # If still no date, try to extract from short description
            if not date and short_description:
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

        # Address extraction
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

    def remove_location_text(self, address):
        """Remove 'Location' text and similar prefixes from address."""
        if not address:
            return address
        
        import re
        
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
            
            # Pattern 1: "Sat, 15th Nov, 2025" or "15th Nov, 2025"
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

    def geocode_address(self, address, event_data=None):
        """Get coordinates from address using multiple geocoding services with fallback.
        
        Tries services in order:
        1. LocationIQ - requires API key (skipped if not configured) - faster and more reliable
        2. Nominatim (OpenStreetMap) - no API key required - free fallback option
        
        If event_data is provided and check_db_before_geocoding is True,
        it will check if the event exists in the database first to avoid
        unnecessary geocoding API calls.
        
        Returns coordinates dict {'lat': float, 'lon': float} or None if all services fail.
        """
        if not address:
            return None
        
        # Check database first if enabled and event_data provided
        if (self.check_db_before_geocoding and event_data and 
            hasattr(self, 'event_exists_in_db')):
            existing_post_id = self.event_exists_in_db(event_data)
            if existing_post_id:
                self.logger.debug(f"Skipping geocoding - event already exists in DB (post ID: {existing_post_id})")
                return None  # Skip geocoding for existing events
        
        # Check cache first to avoid repeated API calls
        if address in self.geocoding_cache:
            self.logger.debug(f"Using cached coordinates for '{address}'")
            return self.geocoding_cache[address]
        
        # Build list of available geocoding services
        # LocationIQ first (if API key is configured), then Nominatim as fallback
        geocoding_services = []
        
        # Add LocationIQ first if API key is configured
        if self.settings.get('LOCATIONIQ_API_KEY'):
            geocoding_services.append(('locationiq', self._geocode_locationiq))
        else:
            self.logger.debug("LocationIQ API key not configured, skipping LocationIQ service")
        
        # Add Nominatim as fallback (no API key required)
        geocoding_services.append(('nominatim', self._geocode_nominatim))
        
        # Try each available geocoding service in order until one succeeds
        for service_name, geocode_func in geocoding_services:
            try:
                coords = geocode_func(address)
                if coords:
                    # Cache the result
                    self.geocoding_cache[address] = coords
                    self.logger.info(f"Geocoded '{address}' using {service_name} -> {coords['lat']}, {coords['lon']}")
                    return coords
            except Exception as e:
                self.logger.warning(f"Geocoding with {service_name} failed for '{address}': {e}")
                # Continue to next service
                continue
        
        self.logger.error(f"All geocoding services failed for '{address}'")
        return None
    
    def _geocode_nominatim(self, address):
        """Geocode using Nominatim (OpenStreetMap) API."""
        import requests
        import time
        
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': address,
            'format': 'json',
            'limit': 1,
            'countrycodes': 'gb',  # UK only
            'addressdetails': 1
        }
        
        # Proper User-Agent as required by Nominatim policy
        headers = {
            'User-Agent': 'RunGuidesSpider/1.0 (contact@example.com)'
        }
        
        # Rate limiting: 1 second delay between requests (Nominatim policy)
        time.sleep(1.1)
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        # Check for access blocked or rate limit errors
        if response.status_code == 403 or response.status_code == 429:
            raise Exception(f"Nominatim access blocked or rate limited (status {response.status_code})")
        
        if response.status_code != 200:
            raise Exception(f"Nominatim returned status {response.status_code}")
        
        data = response.json()
        if not data:
            raise Exception("No results from Nominatim")
        
        lat = float(data[0]['lat'])
        lon = float(data[0]['lon'])
        
        # Validate coordinates are within UK bounds
        if not (49 <= lat <= 61 and -8 <= lon <= 2):
            raise Exception(f"Coordinates {lat}, {lon} are outside UK bounds")
        
        return {'lat': lat, 'lon': lon}
    
    def _geocode_locationiq(self, address):
        """Geocode using LocationIQ API."""
        import requests
        import time
        
        # Get API key from settings (should already be checked, but double-check)
        api_key = self.settings.get('LOCATIONIQ_API_KEY')
        if not api_key:
            raise Exception("LocationIQ API key not configured")
        
        url = "https://us1.locationiq.com/v1/search.php"
        params = {
            'key': api_key,
            'q': address,
            'format': 'json',
            'limit': 1,
            'countrycodes': 'gb',  # UK only
            'addressdetails': 1
        }
        
        # Rate limiting: LocationIQ free tier allows 2 requests/second
        time.sleep(0.6)
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 403 or response.status_code == 401:
            raise Exception(f"LocationIQ authentication failed (status {response.status_code})")
        
        if response.status_code == 429:
            raise Exception(f"LocationIQ rate limit exceeded (status {response.status_code})")
        
        if response.status_code != 200:
            raise Exception(f"LocationIQ returned status {response.status_code}")
        
        data = response.json()
        
        # LocationIQ returns error as dict with 'error' key
        if isinstance(data, dict) and 'error' in data:
            raise Exception(f"LocationIQ error: {data.get('error', 'Unknown error')}")
        
        if not data or not isinstance(data, list) or len(data) == 0:
            raise Exception("No results from LocationIQ")
        
        lat = float(data[0]['lat'])
        lon = float(data[0]['lon'])
        
        # Validate coordinates are within UK bounds
        if not (49 <= lat <= 61 and -8 <= lon <= 2):
            raise Exception(f"Coordinates {lat}, {lon} are outside UK bounds")
        
        return {'lat': lat, 'lon': lon}
    
    def _geocode_opencage(self, address):
        """Geocode using OpenCage Geocoding API."""
        import requests
        import time
        
        # Get API key from settings (should already be checked, but double-check)
        api_key = self.settings.get('OPENCAGE_API_KEY')
        if not api_key:
            raise Exception("OpenCage API key not configured")
        
        url = "https://api.opencagedata.com/geocode/v1/json"
        params = {
            'key': api_key,
            'q': address,
            'limit': 1,
            'countrycode': 'gb',  # UK only
            'no_annotations': 1  # Reduce response size
        }
        
        # Rate limiting: OpenCage free tier allows 1 request/second
        time.sleep(1.1)
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 403 or response.status_code == 401:
            raise Exception(f"OpenCage authentication failed (status {response.status_code})")
        
        if response.status_code == 429:
            raise Exception(f"OpenCage rate limit exceeded (status {response.status_code})")
        
        if response.status_code != 200:
            raise Exception(f"OpenCage returned status {response.status_code}")
        
        data = response.json()
        
        # OpenCage returns status in response
        if data.get('status', {}).get('code') != 200:
            error_msg = data.get('status', {}).get('message', 'Unknown error')
            raise Exception(f"OpenCage error: {error_msg}")
        
        results = data.get('results', [])
        if not results:
            raise Exception("No results from OpenCage")
        
        geometry = results[0].get('geometry', {})
        lat = float(geometry.get('lat', 0))
        lon = float(geometry.get('lng', 0))
        
        if lat == 0 and lon == 0:
            raise Exception("Invalid coordinates from OpenCage")
        
        # Validate coordinates are within UK bounds
        if not (49 <= lat <= 61 and -8 <= lon <= 2):
            raise Exception(f"Coordinates {lat}, {lon} are outside UK bounds")
        
        return {'lat': lat, 'lon': lon}

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

