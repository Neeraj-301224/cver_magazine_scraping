import scrapy
import re
import time
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class BHFSpider(BaseSpider):
    """Spider for https://www.bhf.org.uk/how-you-can-help/events

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Handles pagination using ?page= parameter.
    """
    name = "bhf"
    category = "community_social"
    site_name = "bhf"
    allowed_domains = ["bhf.org.uk"]
    start_urls = [
        "https://www.bhf.org.uk/how-you-can-help/events"
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
        self.seen_urls = set()  # Track seen URLs to avoid following same link twice
        self.processed_items = set()  # Track processed items to avoid duplicates
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
        
        # Try multiple selectors for event links on BHF site
        event_link_selectors = [
            'a[href*="/how-you-can-help/events/"]::attr(href)',
            'a[href*="/events/"]::attr(href)',
            '[class*="event"] a::attr(href)',
            '[data-event-id] a::attr(href)',
            'article a::attr(href)',
            '.event-card a::attr(href)',
            '[class*="Event"] a::attr(href)',
            'a[href*="bhf.org.uk/events"]::attr(href)',
        ]
        
        event_links_found = 0
        seen_urls = set()
        
        for selector in event_link_selectors:
            links = response.css(selector).getall()
            for link in links:
                if link:
                    absolute_url = response.urljoin(link)
                    # Filter for actual event pages (not listing pages)
                    # Exclude the main listing page itself and pagination
                    if ('/how-you-can-help/events/' in absolute_url or '/events/' in absolute_url) and \
                       '?page=' not in absolute_url and \
                       absolute_url != response.url and \
                       absolute_url not in seen_urls and \
                       absolute_url not in self.seen_urls:
                        seen_urls.add(absolute_url)
                        self.seen_urls.add(absolute_url)
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
        ]
        
        for selector in pagination_selectors:
            links = response.css(selector).getall()
            if links:
                for href in links:
                    if href and '?page=' in href:
                        absolute_url = response.urljoin(href)
                        if absolute_url not in seen_pagination_urls and absolute_url not in self.pages_visited:
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
            response.css('h2::text').get()
        )
        
        if title:
            title = title.strip()
        
        # Enhanced description extraction - exclude sidebar content
        desc_parts = []
        
        # Try BHF-specific selectors first - exclude sidebar
        # Main content area (exclude sidebar)
        main_content = response.css('main, .main-content, [class*="main"], article, [class*="content"]')
        if main_content:
            # Exclude sidebar elements
            main_content = main_content.css(':not([class*="sidebar"]):not([class*="side-bar"]):not([class*="event-sidebar"])')
            
            # Try to find description in main content
            desc_selectors = [
                '.description *::text',
                '.event-description *::text',
                '.content *::text',
                'article *::text',
                '.event-details *::text',
                'p::text',
                '[class*="description"] *::text',
            ]
            
            for selector in desc_selectors:
                parts = main_content.css(selector).getall()
                if parts:
                    # Filter out common sidebar/promotional text (less strict)
                    filtered_parts = []
                    for part in parts:
                        part = part.strip()
                        if part and len(part) > 10:  # Skip very short text
                            # Only skip if it's clearly promotional (exact matches or starts with)
                            skip_patterns = [
                                'volunteer in our shops a great way',
                                'find bhf near you find your nearest',
                                'donate items we pick up furniture',
                                'donate money bhf funds the science',
                            ]
                            part_lower = part.lower()
                            # Only skip if it starts with or is exactly a promotional pattern
                            should_skip = False
                            for pattern in skip_patterns:
                                if part_lower.startswith(pattern) or part_lower == pattern:
                                    should_skip = True
                                    break
                            
                            if not should_skip:
                                filtered_parts.append(part)
                    
                    if filtered_parts:
                        desc_parts = filtered_parts
                        self.logger.debug(f"Found description using selector: {selector}")
                        break
        
        # Fallback to general selectors if main content didn't work (very lenient)
        if not desc_parts:
            desc_selectors = [
                'main p::text',
                'article p::text',
                '.content p::text',
                '[class*="description"] p::text',
                'p::text',  # Last resort - get all paragraphs
            ]
            
            for selector in desc_selectors:
                parts = response.css(selector).getall()
                if parts:
                    # Very lenient filtering - only skip obvious promotional sections
                    filtered_parts = []
                    for part in parts:
                        part = part.strip()
                        if part and len(part) > 10:
                            # Only skip if it's clearly a promotional section (exact match)
                            skip_patterns = [
                                'volunteer in our shops a great way to meet',
                                'find bhf near you find your nearest bhf shop',
                            ]
                            part_lower = part.lower()
                            should_skip = any(pattern in part_lower for pattern in skip_patterns)
                            
                            if not should_skip:
                                filtered_parts.append(part)
                    
                    if filtered_parts:
                        desc_parts = filtered_parts
                        self.logger.debug(f"Found description using fallback selector: {selector}")
                        break
        
        # Last resort: if still no description, get first few paragraphs from main content (very lenient)
        if not desc_parts:
            # Get first 3 paragraphs from main content area
            main_paragraphs = response.css('main p::text, article p::text').getall()[:3]
            if main_paragraphs:
                desc_parts = [p.strip() for p in main_paragraphs if p.strip() and len(p.strip()) > 10]
                if desc_parts:
                    self.logger.debug(f"Found description using last resort method: {len(desc_parts)} paragraphs")
        
        # Log if no description found
        if not desc_parts:
            self.logger.warning(f"No description found for event: {title or response.url}")
        
        # Enhanced date extraction - BHF-specific selectors
        date = None
        raw_date = None
        
        # Strategy: Try to get date from main content first, then sidebar
        # This avoids getting promotional dates from sidebars
        
        # First, try to find date in main content area (near the title)
        main_content = response.css('main, .main-content, [class*="main"], article')
        if main_content:
            main_content = main_content.css(':not([class*="sidebar"]):not([class*="side-bar"]):not([class*="event-sidebar"])')
            
            # Look for dates near the title (in header or first section)
            date_selectors = [
                ('time::attr(datetime)', main_content.css('time::attr(datetime)').get()),
                ('time::text', main_content.css('time::text').get()),
                ('h1 ~ * [class*="date"]::text', response.css('h1 ~ * [class*="date"]::text').get()),
                ('[class*="date"]::text', main_content.css('[class*="date"]:not([class*="sidebar"])::text').get()),
                ('.event-date::text', main_content.css('.event-date::text').get()),
            ]
            
            for selector_name, selector_result in date_selectors:
                if selector_result:
                    potential_date = selector_result.strip()
                    if potential_date:
                        date = potential_date
                        raw_date = date
                        self.logger.debug(f"Found date in main content using '{selector_name}': {date}")
                        break
        
        # If not found in main content, try BHF-specific sidebar selector
        # Date is found in event-sidebar-info__details__item--info class inside event-sidebar-info__details__item class
        if not date:
            # Find all event-sidebar-info__details__item containers
            date_containers = response.css('.event-sidebar-info__details__item')
            for date_container in date_containers:
                # Find the event-sidebar-info__details__item--info inside the container
                bhf_date_info = date_container.css('.event-sidebar-info__details__item--info')
                if bhf_date_info:
                    # Get all text from the info element
                    date_text = ' '.join(bhf_date_info.css('*::text').getall())
                    if not date_text or len(date_text.strip()) < 5:
                        date_text = ' '.join(bhf_date_info.css('::text').getall())
                    
                    if date_text and len(date_text.strip()) > 5:
                        date_text = date_text.strip()
                        date = date_text
                        raw_date = date
                        self.logger.debug(f"Found date from event-sidebar-info__details__item container: {date}")
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
        
        # Short description extraction - clean up repetitive content
        short_description = None
        if desc_parts:
            # Filter out promotional/sidebar content more aggressively
            filtered_parts = []
            promotional_keywords = [
                'volunteer in our shops',
                'find bhf near you',
                'find your nearest bhf shop',
                'our research: heart statistics',
                'read the most comprehensive statistics',
                'donate items we pick up',
                'donate money bhf funds',
                'the items you donate are sold',
                'your donation helps us discover',
                'book and clothing bank',
                'effects, prevention, treatment, and costs',
            ]
            
            for part in desc_parts:
                part = part.strip()
                if part and len(part) > 10:
                    part_lower = part.lower()
                    # Skip if contains promotional keywords
                    is_promotional = any(keyword in part_lower for keyword in promotional_keywords)
                    if not is_promotional:
                        filtered_parts.append(part)
            
            # Use filtered parts or fall back to original
            desc_parts_to_use = filtered_parts if filtered_parts else desc_parts
            
            # Join and clean up
            joined = ' '.join(desc_parts_to_use).strip()
            
            # Remove duplicate sentences/phrases (common in BHF pages)
            sentences = joined.split('.')
            seen_sentences = set()
            unique_sentences = []
            
            for sentence in sentences:
                sentence = sentence.strip()
                if sentence and len(sentence) > 10:
                    # Normalize for comparison (lowercase, remove extra spaces)
                    normalized = ' '.join(sentence.lower().split())
                    if normalized not in seen_sentences:
                        seen_sentences.add(normalized)
                        unique_sentences.append(sentence)
            
            # Rejoin unique sentences
            cleaned_desc = '. '.join(unique_sentences)
            if cleaned_desc and not cleaned_desc.endswith('.'):
                cleaned_desc += '.'
            
            # Get first sentence or first 150 chars (reduced from 200)
            if cleaned_desc:
                short_description = cleaned_desc.split('.')[0]
                if len(short_description) > 150:
                    short_description = short_description[:150].rsplit(' ', 1)[0] + '...'
                elif len(cleaned_desc) > 150:
                    short_description = cleaned_desc[:150].rsplit(' ', 1)[0] + '...'
                else:
                    short_description = cleaned_desc

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
        # Clean full description - remove duplicates and limit length
        full_description = None
        if desc_parts:
            # Filter out promotional/sidebar content more aggressively
            filtered_parts = []
            promotional_keywords = [
                'volunteer in our shops',
                'find bhf near you',
                'find your nearest bhf shop',
                'our research: heart statistics',
                'read the most comprehensive statistics',
                'donate items we pick up',
                'donate money bhf funds',
                'the items you donate are sold',
                'your donation helps us discover',
                'book and clothing bank',
                'effects, prevention, treatment, and costs',
            ]
            
            for part in desc_parts:
                part = part.strip()
                if part and len(part) > 10:
                    part_lower = part.lower()
                    # Skip if contains promotional keywords
                    is_promotional = any(keyword in part_lower for keyword in promotional_keywords)
                    if not is_promotional:
                        filtered_parts.append(part)
            
            # Use filtered parts or fall back to original
            desc_parts_to_use = filtered_parts if filtered_parts else desc_parts
            
            # Join and remove duplicate content
            joined = ' '.join(desc_parts_to_use).strip()
            sentences = joined.split('.')
            seen_sentences = set()
            unique_sentences = []
            
            for sentence in sentences:
                sentence = sentence.strip()
                if sentence and len(sentence) > 10:
                    normalized = ' '.join(sentence.lower().split())
                    if normalized not in seen_sentences:
                        seen_sentences.add(normalized)
                        unique_sentences.append(sentence)
            
            # Join unique sentences and limit to 500 characters
            full_description = '. '.join(unique_sentences)
            if full_description and not full_description.endswith('.'):
                full_description += '.'
            
            # Limit full description to 500 characters
            if full_description and len(full_description) > 500:
                full_description = full_description[:500].rsplit(' ', 1)[0] + '...'
        
        item['raw'] = {
            'title': title,
            'date': date,
            'desc_preview': short_description,
            'full_description': full_description,
            'address': address,
            'coordinates': coords,
        }
        
        # Check for duplicate items based on URL (most reliable)
        # Use URL as primary key since it's unique per event
        item_key_url = item['url']
        
        # Check if we've processed this URL before
        if item_key_url in self.processed_items:
            self.logger.debug(f"Skipping duplicate item (URL): {item['name']}")
            return
        
        # Add to processed items
        self.processed_items.add(item_key_url)
        
        # Also track name+date for additional duplicate detection (but don't skip based on this alone)
        if item['name'] and item['date']:
            item_key_name_date = f"{item['name']}_{item['date']}"
            if item_key_name_date in self.processed_items:
                self.logger.debug(f"Warning: Same name+date found, but URL is different: {item['name']}")
            # Don't add to processed_items to avoid false positives
        
        # Increment total items scraped
        self.total_items_scraped += 1
        
        # Log final item data
        self.logger.info(f"Event extracted - Name: {item['name'][:50] if item['name'] else 'N/A'}, Date: {item['date'] or 'N/A'}, URL: {item['url']}")
        self.logger.debug(f"Description length: {len(desc_parts) if desc_parts else 0} parts, Short desc: {len(short_description) if short_description else 0} chars")
        
        # Yield item even if some fields are missing
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
        # Try BHF-specific selector: 
        # 1. First find the parent with class: bhfi bhfi-event-location event-sidebar-info__details__item--icon
        # 2. Then find the sibling element with class: event-sidebar-info__details__item--info (beside the icon)
        # 3. Extract address from that info element
        
        # Find the location icon element first
        bhf_location_icon = response.css('.bhfi.bhfi-event-location.event-sidebar-info__details__item--icon, [class*="bhfi-event-location"][class*="event-sidebar-info__details__item--icon"]')
        
        if bhf_location_icon:
            # Find the sibling element with class event-sidebar-info__details__item--info
            # This should be beside (sibling of) the icon element
            # Try adjacent sibling first (+), then general sibling (~)
            bhf_address_info = bhf_location_icon.xpath('./following-sibling::*[contains(@class, "event-sidebar-info__details__item--info")] | ./preceding-sibling::*[contains(@class, "event-sidebar-info__details__item--info")]')
            
            if not bhf_address_info:
                # Try CSS sibling selectors
                bhf_address_info = response.css('.bhfi.bhfi-event-location.event-sidebar-info__details__item--icon ~ .event-sidebar-info__details__item--info, .bhfi.bhfi-event-location.event-sidebar-info__details__item--icon + .event-sidebar-info__details__item--info')
            
            if bhf_address_info:
                # Get all text from the info element (including child elements)
                address_text = ' '.join(bhf_address_info.css('*::text').getall())
                if not address_text or len(address_text.strip()) < 5:
                    # Try direct text if no child text found
                    address_text = ' '.join(bhf_address_info.css('::text').getall())
                
                if address_text and len(address_text.strip()) > 5:
                    # Check if it looks like a date (to avoid extracting dates)
                    date_patterns = [r'\d{1,2}(st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)', 
                                   r'\d{1,2}/\d{1,2}/\d{4}', r'\d{4}-\d{2}-\d{2}']
                    is_date = any(re.search(pattern, address_text, re.IGNORECASE) for pattern in date_patterns)
                    
                    if not is_date:
                        self.logger.debug(f"Found address using BHF location structure: {address_text[:50]}")
                        return self.clean_text(address_text)
        
        # Alternative: Find the info element that is in the same parent container as the location icon
        # Look for a parent container that has both elements
        location_container = response.css('[class*="event-sidebar-info__details__item"]:has(.bhfi-event-location)')
        if location_container:
            bhf_address_info = location_container.css('.event-sidebar-info__details__item--info')
            if bhf_address_info:
                address_text = ' '.join(bhf_address_info.css('*::text').getall())
                if not address_text or len(address_text.strip()) < 5:
                    address_text = ' '.join(bhf_address_info.css('::text').getall())
                
                if address_text and len(address_text.strip()) > 5:
                    # Check if it looks like a date
                    date_patterns = [r'\d{1,2}(st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)', 
                                   r'\d{1,2}/\d{1,2}/\d{4}', r'\d{4}-\d{2}-\d{2}']
                    is_date = any(re.search(pattern, address_text, re.IGNORECASE) for pattern in date_patterns)
                    
                    if not is_date:
                        self.logger.debug(f"Found address from container: {address_text[:50]}")
                        return self.clean_text(address_text)
        
        # Try multiple selectors for address (fallback)
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

