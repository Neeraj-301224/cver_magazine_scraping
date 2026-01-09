from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class RunningCalendarSpider(BaseSpider):
    """Spider for https://www.runningcalendar.co.uk/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Filters for specific race types: 5K, 10K, Half Marathon, Ultras
    """
    name = "runningcalendar"
    category = "fitness_training"
    site_name = "runningcalendar"
    allowed_domains = ["runningcalendar.co.uk"]
    start_urls = [
        "https://www.runningcalendar.co.uk/",
        "https://www.runningcalendar.co.uk/events/",
        "https://www.runningcalendar.co.uk/races/",
        "https://www.runningcalendar.co.uk/running/",
        "https://www.runningcalendar.co.uk/cycling/",
        "https://www.runningcalendar.co.uk/swimming/",
        "https://www.runningcalendar.co.uk/triathlon/",
        "https://www.runningcalendar.co.uk/ultra/",
        "https://www.runningcalendar.co.uk/marathon/",
        "https://www.runningcalendar.co.uk/half-marathon/",
        "https://www.runningcalendar.co.uk/10k/",
        "https://www.runningcalendar.co.uk/5k/"
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
        import pdb
        
        print(f"\nDEBUGGING PARSE FUNCTION - FUNCTION CALLED!")
        print(f"URL: {response.url}")
        print(f"Status: {response.status}")
        print(f"Response size: {len(response.body)} bytes")
        print(f"Spider name: {self.name}")
        print(f"Start URLs: {self.start_urls}")
        
        # BREAKPOINT: Start of parse function
        print("\nBREAKPOINT: Starting parse function")
        print("Available commands: 'c' (continue), 'n' (next), 's' (step), 'p variable_name' (print), 'l' (list code), 'h' (help)")
        print("Type 'c' to continue, or inspect variables with 'p variable_name'")
        #pdb.set_trace()
        
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # Debug: Log page content structure
        page_title = response.css('title::text').get()
        all_links = response.css('a::attr(href)').getall()
        
        print(f"\nPAGE ANALYSIS:")
        print(f"Page title: {page_title}")
        print(f"Total links found: {len(all_links)}")
        print(f"Sample links: {all_links[:5]}")
        
        # BREAKPOINT: After page analysis
        print("\nBREAKPOINT: After page analysis")
        print(f"All links: {all_links}")
        #pdb.set_trace()
        
        # find event links on the homepage or listings
        # common selectors: links with '/event/' or pages under /events/
        event_links_found = 0
        event_links = []
        
        for href in all_links:
            if href and ('/event/' in href or '/events/' in href or '/races/' in href):
                # Check if we've already seen this event (duplicate detection)
                if href in self.seen_events:
                    print(f"⏭️  Skipping duplicate event: {href}")
                    continue
                
                # Add to seen events
                self.seen_events.add(href)
                
                event_links_found += 1
                event_links.append(href)
                print(f"Found event link #{event_links_found}: {href}")
                
                # BREAKPOINT: For each event link (limit to first 3)
                if event_links_found <= 3:
                    print(f"\nBREAKPOINT: Processing event link #{event_links_found}")
                    print(f"Link: {href}")
                    print(f"Total event links found so far: {event_links_found}")
                    #pdb.set_trace()
                
                try:
                    yield response.follow(href, self.parse_event, errback=self.handle_error)
                except Exception as e:
                    self.logger.error(f"Error following event link {href}: {e}")
        
        print(f"\n📈 EVENT LINKS SUMMARY:")
        print(f"Total event links found: {event_links_found}")
        print(f"All event links: {event_links}")
        
        # BREAKPOINT: After finding event links
        print("\nBREAKPOINT: After finding event links")
        #pdb.set_trace()
        
        # Pagination - use the correct selector first
        pagination_links_raw = response.css('[class*="paging"] a::attr(href)').getall()
        
        # If no links found with primary selector, try fallback selectors
        if not pagination_links_raw:
            pagination_links_raw = (
                response.css('a.next::attr(href), a.pagination-next::attr(href)').getall() or
                response.css('a[rel="next"]::attr(href)').getall() or
                response.css('.pagination a::attr(href)').getall() or
                response.css('.page-numbers a::attr(href)').getall() or
                response.css('[class*="next"] a::attr(href)').getall() or
                response.css('[class*="pagination"] a::attr(href)').getall()
            )
        
        # Convert to absolute URLs and remove duplicates
        pagination_links = []
        seen_urls = set()
        current_url = response.url
        
        # Track this page as visited
        self.pages_visited.add(current_url)
        
        for link in pagination_links_raw:
            if link:
                # Convert to absolute URL
                absolute_url = response.urljoin(link)
                # Skip if it's the current page, already seen, or already visited
                if (absolute_url != current_url and 
                    absolute_url not in seen_urls and 
                    absolute_url not in self.pages_visited):
                    pagination_links.append(absolute_url)
                    seen_urls.add(absolute_url)
        
        print(f"\nPAGINATION ANALYSIS:")
        print(f"Current page: {current_url}")
        print(f"Pages visited so far: {len(self.pages_visited)}")
        print(f"Pagination links found: {len(pagination_links)}")
        print(f"Pagination URLs: {pagination_links[:5]}")  # Show first 5
        
        # BREAKPOINT: Before processing pagination
        print("\nBREAKPOINT: Before processing pagination")
        #pdb.set_trace()
        
        for next_page in pagination_links:
            if next_page:
                print(f"Following pagination: {next_page}")
                try:
                    yield response.follow(next_page, self.parse, errback=self.handle_error)
                except Exception as e:
                    self.logger.error(f"Error following pagination link {next_page}: {e}")
        
        # If no event links found, log potential selectors for debugging
        if event_links_found == 0:
            print(f"\n⚠️  NO EVENT LINKS FOUND - TRYING ALTERNATIVES")
            self.logger.warning("No event links found. Checking for alternative selectors...")
            
            # BREAKPOINT: Before trying alternatives
            print("\nBREAKPOINT: Before trying alternative selectors")
            #pdb.set_trace()
            
            # Try alternative selectors
            alt_selectors = [
                'a[href*="event"]::attr(href)',
                'a[href*="race"]::attr(href)', 
                'a[href*="run"]::attr(href)',
                'a[href*="marathon"]::attr(href)',
                'a[href*="ultra"]::attr(href)',
                '.event a::attr(href)',
                '.race a::attr(href)',
                '.event-item a::attr(href)',
                '.race-item a::attr(href)',
                '[class*="event"] a::attr(href)',
                '[class*="race"] a::attr(href)',
                'h2 a::attr(href), h3 a::attr(href)',
                '.title a::attr(href)',
                '.name a::attr(href)'
            ]
            
            for selector in alt_selectors:
                alt_links = response.css(selector).getall()
                if alt_links:
                    print(f"Found {len(alt_links)} links with selector '{selector}': {alt_links[:5]}")
                    
                    # BREAKPOINT: For alternative links
                    print(f"\nBREAKPOINT: Processing alternative links with selector '{selector}'")
                    #pdb.set_trace()
                    
                    for link in alt_links:
                        try:
                            yield response.follow(link, self.parse_event, errback=self.handle_error)
                        except Exception as e:
                            self.logger.error(f"Error following alternative link {link}: {e}")
        
        # Check if we should continue scraping (optional limit)
        if self.total_items_scraped >= 1000:  # Safety limit to prevent infinite scraping
            print(f"\n🛑 STOPPING: Reached safety limit of 1000 items")
            self.logger.warning("Reached safety limit of 1000 items. Stopping spider.")
            return
        
        print(f"\nPARSE FUNCTION COMPLETED")
        print(f"Event links found: {event_links_found}")
        print(f"Pagination links found: {len(pagination_links)}")
        print(f"Total pages visited: {len(self.pages_visited)}")
        print(f"Total items scraped so far: {self.total_items_scraped}")

    def parse_event(self, response):
        """Parse individual event pages to extract event details."""
        import pdb
        
        print(f"\nDEBUGGING PARSE_EVENT FUNCTION")
        print(f"Event URL: {response.url}")
        print(f"Status: {response.status}")
        
        # BREAKPOINT: Start of parse_event function
        print("\nBREAKPOINT: Starting parse_event function")
        print("Available commands: 'c' (continue), 'n' (next), 's' (step), 'p variable_name' (print), 'l' (list code), 'h' (help)")
        #pdb.set_trace()
        
        self.logger.info(f"Parsing event page: {response.url}")
        self.logger.debug(f"Event page status: {response.status}")
        
        item = EventScrapingItem()
        item['category'] = self.category
        item['site'] = self.site_name
        item['url'] = response.url

        # Try some common selectors; fallback to generic extraction
        title = response.css('h1::text, .event-title::text, .title::text').get()
        
        # Enhanced description extraction FIRST
        desc_parts = (
            response.css('.description *::text, .event-description *::text, .content *::text').getall() or
            response.css('article *::text, .event-details *::text').getall() or
            response.css('p::text, .text::text').getall()
        )
        
        # Enhanced date extraction with multiple selectors (prioritizing the correct ones)
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
            ('.event-summary::text', response.css('.event-summary::text').get())
        ]
        
        date = None
        used_selector = None
        for selector_name, selector_result in date_selectors:
            if selector_result:
                date = selector_result
                used_selector = selector_name
                print(f"Found date using selector '{selector_name}': {date}")
                break
        
        # If still no date, try to extract from description (now that we have desc_parts)
        if not date and desc_parts:
            import re
            desc_text = ' '.join(desc_parts)
            print(f"Searching for dates in description: {desc_text[:200]}...")
            
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
                    print(f"Found date in description: {date}")
                    break
        
        # Store raw date before conversion
        raw_date = date
        
        # Convert date to MM/DD/YYYY format
        if date:
            print(f"Converting date: {date}")
            date = self.convert_date_format(date)
            print(f"Converted date: {date}")
        else:
            print(f"No date found for event: {title}")
            
            # Final fallback: try to extract from the full description text
            if desc_parts:
                full_desc_text = ' '.join(desc_parts)
                print(f"Final fallback - searching full description: {full_desc_text[:200]}...")
                
                import re
                date_patterns = [
                    r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                    r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                    r'(\d{1,2}/\d{1,2}/\d{4})',
                    r'(\d{1,2}-\d{1,2}-\d{4})',
                    r'(\d{4}-\d{1,2}-\d{1,2})'
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, full_desc_text, re.IGNORECASE)
                    if match:
                        date = match.group(1)
                        raw_date = date  # Store raw date
                        print(f"Found date in full description: {date}")
                        date = self.convert_date_format(date)
                        print(f"Converted date: {date}")
                        break

        print(f"\nDATA EXTRACTION:")
        print(f"Raw title: {title}")
        print(f"Raw date: {date}")
        print(f"Description parts count: {len(desc_parts)}")
        print(f"Description parts: {desc_parts[:3]}")

        # BREAKPOINT: After data extraction
        print("\nBREAKPOINT: After data extraction")
        print(f"Title: {title}")
        print(f"Date: {date}")
        print(f"Description parts: {desc_parts}")
        #pdb.set_trace()
        
        # Check if this event matches our target race types
        if not self.is_target_race_type(title, desc_parts):
            print(f"Event does not match target race types - skipping")
            return

        # Short description: first 2 sentences or first 200 chars
        short_description = None
        if desc_parts:
            joined = '\n'.join(desc_parts).strip()
            short_description = joined.split('\n')[0]
            if len(short_description) > 200:
                short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
            print(f"Short description: {short_description[:100]}...")
            
            # If still no date, try to extract from short description
            if not date and short_description:
                import re
                print(f"Searching for dates in short description: {short_description}")
                
                # Look for date patterns in short description
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
                        raw_date = date  # Store raw date
                        print(f"Found date in short description: {date}")
                        # Convert date to MM/DD/YYYY format
                        date = self.convert_date_format(date)
                        print(f"Converted date: {date}")
                        break

        # BREAKPOINT: After description processing
        print("\nBREAKPOINT: After description processing")
        print(f"Short description: {short_description}")
        #pdb.set_trace()

        # Address extraction
        address = self.extract_address(response)
        print(f"Address found: {address}")
        
        # Coordinates extraction using multiple heuristics
        coords = self.extract_coordinates(response)
        print(f"Coordinates found: {coords}")
        
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
                print(f"Coordinates from geocoding: {coords}")

        # BREAKPOINT: After coordinates extraction
        print("\nBREAKPOINT: After coordinates extraction")
        print(f"Address: {address}")
        print(f"Coordinates: {coords}")
        #pdb.set_trace()

        # Determine event category and subcategory
        event_category, event_subcategory = self.get_event_category(title, desc_parts)
        
        # Clean and set item fields
        cleaned_title = self.clean_text(title)
        parsed_date = date  # Use the already converted date directly
        cleaned_description = self.clean_text(short_description)
        
        print(f"\nCLEANED DATA:")
        print(f"Cleaned title: {cleaned_title}")
        print(f"Raw date: {raw_date}")
        print(f"Parsed date: {parsed_date}")
        print(f"Cleaned description: {cleaned_description[:100]}...")

        item['name'] = cleaned_title
        item['date'] = parsed_date
        item['raw_date'] = raw_date  # Original date as found (no conversion)
        item['short_description'] = cleaned_description
        item['coordinates'] = coords
        item['address'] = address
        item['category'] = event_category  # Main category (e.g., "Running", "Cycling")
        item['subcategory'] = event_subcategory  # Subcategory (e.g., "Road running", "Trail running")
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
            print(f"⏭️  Skipping duplicate item: {item['name']}")
            return
        
        # Add to seen items
        self.seen_events.add(item_key)
        
        # Increment total items scraped
        self.total_items_scraped += 1
        
        # BREAKPOINT: Before yielding item
        print("\nBREAKPOINT: Before yielding item")
        print(f"Final item data: {dict(item)}")
        print(f"Total items scraped: {self.total_items_scraped}")
        #pdb.set_trace()
        
        # Log final item data
        self.logger.info(f"Event extracted - Name: {item['name'][:50]}...")
        self.logger.debug(f"Full item data: {dict(item)}")
        
        print(f"\nEVENT PARSING COMPLETED")
        print(f"Event name: {item['name']}")
        print(f"Event date: {item['date']}")
        print(f"Event description: {item['short_description'][:50]}...")
        print(f"Total items scraped: {self.total_items_scraped}")
        
        yield item

    def is_target_race_type(self, title, description_parts):
        """Check if the event matches any of our target categories."""
        if not title:
            return False
        
        # Combine title and description for analysis
        full_text = title.lower()
        if description_parts:
            full_text += " " + " ".join(description_parts).lower()
        
        print(f"\nCATEGORY FILTERING:")
        print(f"Full text to analyze: {full_text[:200]}...")
        
        # Check for any matching keywords
        for keyword in self.ALL_KEYWORDS:
            if keyword.lower() in full_text:
                print(f"Found matching keyword: {keyword}")
                return True
        
        print(f"No matching categories found in: {title}")
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
                        print(f"Event categorized as: {category_group} -> {subcategory}")
                        return category_group, subcategory
        
        return "Other", "General"

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
            '.event-contact::text'
        ]
        
        for selector in address_selectors:
            address = response.css(selector).get()
            if address and len(address.strip()) > 5:  # Valid address should be longer than 5 chars
                return self.clean_text(address)
        
        # Try to extract from description or content
        content_text = ' '.join(response.css('*::text').getall())
        
        # Look for common address patterns
        import re
        
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
                    '%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y', '%b %d, %Y'
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
            print(f"Date conversion failed for '{date_str}': {e}")
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
                        import re
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
            import re
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