import scrapy
import re
from ..base_spider import BaseSpider
from ...items import EventScrapingItem
from ...utils.common import get_event_category as classify_event_by_keywords


class TimeOutdoorsSpider(BaseSpider):
    """Spider for https://www.timeoutdoors.com/events

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    
    Filters for specific race types: 5K, 10K, Half Marathon, Ultras
    """
    name = "timeoutdoors"
    category = "fitness_training"
    site_name = "timeoutdoors"
    allowed_domains = ["timeoutdoors.com"]
    start_urls = [
        "https://www.timeoutdoors.com/events/trail-runs",
        "https://www.timeoutdoors.com/charity/runs",
        "https://www.timeoutdoors.com/events/5k-runs",
        "https://www.timeoutdoors.com/events/10k-runs",
        "https://www.timeoutdoors.com/events/marathons",
        "https://www.timeoutdoors.com/events/ultra-runs",
    ]

    # Charity hub pages whose outbound links we still crawl (other /charity/* hubs are skipped).
    charity_listing_path_substrings = ("/charity/runs",)

    # When classifying raw <a href> as a possible event URL (first loop in parse()).
    url_event_indicators = [
        "run",
        "race",
        "marathon",
        "10k",
        "5k",
        "half",
        "ultra",
        "trail",
        "triathlon",
        "duathlon",
        "swim",
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
            'Time Trials': ['time trial', 'cycling time trial', 'bike time trial'],
            'Road Races': ['road race', 'cycling race', 'bike race', 'road cycling'],
            'Cyclocross': ['cyclocross', 'cx', 'cyclo-cross'],
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
            'Bootcamps & Fitness Challenges': ['bootcamp', 'fitness challenge', 'fitness bootcamp']
        },
        'Multi-Discipline': {
            'Triathlon': ['triathlon', 'triathlete', 'triathlon race'],
            'Duathlon': ['duathlon', 'duathlete', 'duathlon race'],
            'Aquathlon': ['aquathlon', 'aquathlete', 'aquathlon race'],
            'Adventure Races': ['adventure race', 'multi sport', 'multi-sport', 'adventure challenge']
        }
    }
    
    # Flatten all keywords for easier matching
    ALL_KEYWORDS = []
    for category_group, subcategories in CATEGORY_KEYWORDS.items():
        for subcategory, keywords in subcategories.items():
            ALL_KEYWORDS.extend(keywords)

    # Copied repeatedly into #isPasted with real event prose — strip so JSON matches event copy only.
    TIMEOUTDOORS_LEADING_PROMO_PHRASES = (
        "More runs, rides, swims, walks and triathlons than any other site in the UK.",
        "Got an event to list on the website?",
        "The team adventure challenge that pushes you from dawn to dusk. Ride, hike and kayak to beat the sunset.",
        "Challenge yourself and change lives at the same time.",
        "Want to list your charity places, events and challenges on the website?",
        "100s of activity days, weekends, trips, holidays, retreats and training camps.",
        "Weekends, holidays, retreats and training camps for runners of all abilities.",
        "The perfect way to see somewhere new.",
        "Actionpacked adventures!",
        "Open water adventures for those wanting to roam wide and free.",
        "Walking weekends, holidays, retreats and adventures for rambers, walkers and hikers of all abilities.",
        "Journey into some of the most spectacular locations on the planet.",
        "Want to list your trip on the website?",
        "Make new friends and get help with training.",
        "From your local area to far flung corners of the world.",
        "Event guides, gear advice, places to go and tips on wellbeing.",
        "Log your achievements, plan whats next and socialise with friends and family.",
    )
    TIMEOUTDOORS_FOOTER_MARKERS = (
        "A good nights sleep is an essential foundation",
        "#FreeYourself",
        "\u00a9 Copyright",
        "\u00a9 copyright",
        "TOD.com Limited",
        "Sign up to our newsletter for new events, exclusive offers",
        "We are a registered data controller with the ICO",
        "\U0001f36a We use cookies",
        "Please enter a valid email",
        "Thanks please check your Inbox",
    )

    def _sanitize_timeoutdoors_description_text(self, text):
        """Drop sitewide promos merged into #isPasted plus footer/newsletter/cookie blocks."""
        if not text or not str(text).strip():
            return text
        text = re.sub(r"\s+", " ", str(text)).strip()
        idxs = [text.find(m) for m in self.TIMEOUTDOORS_FOOTER_MARKERS if m and text.find(m) != -1]
        if idxs:
            text = text[: min(idxs)].rstrip()
        max_passes = 80
        for _ in range(max_passes):
            stripped = False
            for phrase in self.TIMEOUTDOORS_LEADING_PROMO_PHRASES:
                if text.startswith(phrase):
                    text = text[len(phrase) :].lstrip()
                    if text.startswith((",", ".", ";")):
                        text = text.lstrip(",.;").strip()
                    stripped = True
                    break
            if not stripped:
                break
        return text.strip()

    def _first_substantive_preview_sentence(self, text, min_len=45):
        """Use first reasonably long sentence — avoids sitewide one-liners if any remain."""
        if not text:
            return None
        for chunk in re.split(r"(?<=[.!?])\s+", text.strip()):
            c = chunk.strip()
            if len(c) >= min_len:
                if len(c) > 200:
                    return c[:200].rsplit(" ", 1)[0] + "..."
                return c
        if text.strip():
            t = text.strip()
            if len(t) > 200:
                return t[:200].rsplit(" ", 1)[0] + "..."
            return t
        return None

    def _extract_event_overview_description_parts(self, response):
        """Text under Event Overview: primary source is element with id=\"overview\"."""
        # 1) #overview (section / div used for Event Overview body)
        root = response.css("#overview")
        if root:
            parts = root.css("*::text").getall()
            parts = [p.strip() for p in parts if p and str(p).strip()]
            if parts:
                self.logger.debug("Description from #overview (%s parts)", len(parts))
                return parts

        parts = response.xpath('//*[@id="overview"]//text()[normalize-space()]').getall()
        parts = [p.strip() for p in parts if p and str(p).strip()]
        if parts:
            self.logger.debug("Description from xpath @id=overview (%s parts)", len(parts))
            return parts

        # 2) First "Event Overview" heading, then nearest following #overview
        for _hdr in response.xpath(
            '//*[self::h2 or self::h3 or self::h4]'
            '[contains(translate(normalize-space(string(.)), '
            '"ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "event overview")]'
        ):
            following = _hdr.xpath(
                "./following::*[@id=\"overview\"][1]//text()[normalize-space()]"
            ).getall()
            following = [t.strip() for t in following if t and str(t).strip()]
            if following:
                self.logger.debug(
                    "Description from Event Overview header + following #overview (%s parts)",
                    len(following),
                )
                return following

        return []

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
        all_links = response.css('a::attr(href)').getall()
        event_links_found = 0
        event_links = []
        
        # Common patterns for TimeOutdoors event URLs
        event_patterns = ['/events/', '/event/', '/race/', '/races/']
        
        for href in all_links:
            if href:
                # Convert to absolute URL
                absolute_url = response.urljoin(href)
                
                # Check if this is an event link
                is_event_link = False
                
                # Check if URL contains event-like patterns but exclude common non-event pages
                if any(pattern in href for pattern in event_patterns):
                    # Exclude common non-event pages
                    # Note: /charity/runs is a valid listing page, so we allow it
                    excluded_patterns = ['/about', '/contact', '/faq', '/login', '/signup', '/cart', 
                                        '/wishlist', '/results', '/photos', '/videos', '/blog', 
                                        '/news', '/terms', '/privacy', '/cookie', '/search',
                                        '/distances/', '/regions/', '/cities/', '/venues/', '/series/',
                                        '/gift', '/membership', '/calendar', '/race-info', '/corporate',
                                        '/foundation', '/kit', '/coach', '/retreats',
                                        '/partners', '/volunteer', '/careers', '/sustainability',
                                        '/community', '/pacing', '/prizes', '/club', '/injury',
                                        '/tips', '/fundraising', '/events']  # Exclude the main listing page
                    
                    # Allow configured charity listing hubs; skip other charity pages
                    if "/charity/" in href and not any(
                        p in href for p in self.charity_listing_path_substrings
                    ):
                        continue

                    # If it's an event URL and not excluded, it might be an event
                    if not any(excluded in href for excluded in excluded_patterns):
                        # Check if URL looks like an event slug (contains common race terms or is a specific event page)
                        event_indicators = self.url_event_indicators
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
        
        self.logger.info(f"Total event links found: {event_links_found}")
        
        # Try to find event cards/data directly on the page
        event_card_selectors = [
            'a[href*="/event/"]',
            'a[href*="/events/"]',
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
        
        # Description: Event Overview body — element id=\"overview\" (see _extract_event_overview_description_parts)
        desc_parts = self._extract_event_overview_description_parts(response)

        # Legacy fallback if markup changes (avoid sitewide #isPasted mega-blocks when possible)
        if not desc_parts:
            is_pasted = response.css("#isPasted")
            if is_pasted:
                desc_parts = is_pasted.css("*::text").getall()
                direct_text = is_pasted.css("::text").getall()
                if direct_text:
                    desc_parts = direct_text + desc_parts
                desc_parts = [part.strip() for part in desc_parts if part.strip()]
                self.logger.debug("Fallback description from #isPasted: %s parts", len(desc_parts))

        if not desc_parts:
            desc_parts = response.css(
                "#overview *::text, [id=\"overview\"] *::text, "
                ".event-overview *::text, [class*=\"event-overview\"] *::text"
            ).getall()
            desc_parts = [p.strip() for p in desc_parts if p and str(p).strip()]

        if desc_parts:
            merged = " ".join(p for p in desc_parts if p and str(p).strip())
            cleaned = self._sanitize_timeoutdoors_description_text(merged)
            desc_parts = [cleaned] if cleaned else []

        # Enhanced date extraction
        # Date is in class "tod-date badge-sm"
        date = None
        
        # Try TimeOutdoors specific date selector first
        date = response.css('.tod-date.badge-sm::text').get() or response.css('[class*="tod-date"][class*="badge-sm"]::text').get()
        
        if date:
            date = date.strip()
            self.logger.debug(f"Found date using .tod-date.badge-sm: {date}")
        else:
            # Fallback to other date selectors
            date_selectors = [
                ('[class="dtstart dtend"]::text', response.css('[class="dtstart dtend"]::text').get()),
                ('.date::text', response.css('.date::text').get()),
                ('time::attr(datetime)', response.css('time::attr(datetime)').get()),
                ('.event-date::text', response.css('.event-date::text').get()),
                ('[class*="date"]::text', response.css('[class*="date"]::text').get()),
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

        # Short description extraction (first line was always sitewide tagline before sanitise)
        short_description = None
        if desc_parts:
            joined = " ".join(desc_parts).strip()
            short_description = self._first_substantive_preview_sentence(joined)

        # Address extraction
        # Use specific xpath to get the first p tag in #collapseLocation/div
        address = None
        
        # Try TimeOutdoors specific location selector using xpath
        # Get the first p tag: //*[@id="collapseLocation"]/div/p[1]
        location_p = response.xpath('//*[@id="collapseLocation"]/div/p[1]')
        
        if location_p:
            # Get text from the p tag
            address = ' '.join(location_p.css('::text').getall())
            if address:
                address = address.strip()
                # Clean up multiple spaces and newlines
                address = re.sub(r'\s+', ' ', address).strip()
                self.logger.debug(f"Found location using xpath //*[@id=\"collapseLocation\"]/div/p[1]: {address}")
        
        # Fallback 1: Try class "tod-location" if not found in collapseLocation
        if not address:
            tod_location = response.css('.tod-location')
            if tod_location:
                # Get text only from p tags
                p_tags = tod_location.css('p')
                location_text_parts = []
                for p_tag in p_tags:
                    p_text = ' '.join(p_tag.css('::text').getall())
                    if p_text.strip():
                        location_text_parts.append(p_text.strip())
                
                if location_text_parts:
                    address = ' '.join([part.strip() for part in location_text_parts if part.strip()])
                    # Clean up multiple spaces and newlines
                    address = re.sub(r'\s+', ' ', address).strip()
                    self.logger.debug(f"Found location using .tod-location p tags: {address}")
        
        # Fallback 2: General address extraction if specific selectors didn't work
        if not address:
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
            'full_description': desc_parts[0] if desc_parts else None,
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
        cat, sub = classify_event_by_keywords(title, description_parts, self.CATEGORY_KEYWORDS)
        if cat and sub:
            self.logger.debug(f"Event categorized as: {cat} -> {sub}")
        return cat, sub

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

