import scrapy
import re
import time
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class YogaWithManonSpider(BaseSpider):
    """Spider for https://yogawithmanon.co.uk/retreats/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    """
    name = "yogawithmanon"
    category = "wellness_mind"
    site_name = "yogawithmanon"
    allowed_domains = ["yogawithmanon.co.uk"]
    start_urls = [
        "https://yogawithmanon.co.uk/retreats/"
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_events = set()  # Track seen events to avoid duplicates
        self.geocoding_cache = {}  # Cache geocoding results to avoid repeated API calls
        self.total_items_scraped = 0

    def parse(self, response):
        """Parse the page and extract event links."""
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # Find event links on the page
        self.logger.info("Extracting event links from page...")
        
        # Try multiple selectors for event links
        event_link_selectors = [
            'a[href*="/retreats/"]::attr(href)',
            'a[href*="/retreat/"]::attr(href)',
            '[class*="retreat"] a::attr(href)',
            '[class*="event"] a::attr(href)',
            'article a::attr(href)',
            '.event-card a::attr(href)',
            '[class*="Event"] a::attr(href)',
            'a[href*="yogawithmanon.co.uk/retreat"]::attr(href)',
        ]
        
        event_links_found = 0
        seen_urls = set()
        
        for selector in event_link_selectors:
            links = response.css(selector).getall()
            for link in links:
                if link:
                    absolute_url = response.urljoin(link)
                    # Filter for actual event pages (not listing pages)
                    if '/retreats/' in absolute_url or '/retreat/' in absolute_url:
                        if absolute_url != response.url and \
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
        
        self.logger.info(f"Total event links found: {event_links_found}")
        
        # If no links found, try to extract events directly from listing page
        if event_links_found == 0:
            self.logger.info("No event links found, trying to extract from listing page...")
            # Try to extract event data directly from cards/containers
            event_cards = response.css('[class*="retreat"], [class*="event"], article, .card')
            for card in event_cards:
                try:
                    item = self.extract_event_from_card(card, response)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.debug(f"Error extracting from card: {e}")

    def parse_event(self, response):
        """Parse individual event pages to extract event details."""
        self.logger.info(f"Parsing event page: {response.url}")
        
        item = EventScrapingItem()
        item['category'] = self.category
        item['site'] = self.site_name
        item['url'] = response.url

        # Extract title
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
        
        # Extract description
        desc_parts = []
        desc_selectors = [
            '.description *::text',
            '.event-description *::text',
            '.content *::text',
            'article *::text',
            '.event-details *::text',
            'p::text',
            '[class*="description"] *::text',
            '[class*="content"] *::text',
        ]
        
        for selector in desc_selectors:
            parts = response.css(selector).getall()
            if parts:
                desc_parts = [part.strip() for part in parts if part.strip()]
                if desc_parts:
                    break
        
        # Extract date
        date = None
        raw_date = None
        date_selectors = [
            ('.date::text', response.css('.date::text').get()),
            ('time::attr(datetime)', response.css('time::attr(datetime)').get()),
            ('.event-date::text', response.css('.event-date::text').get()),
            ('[class*="date"]::text', response.css('[class*="date"]::text').get()),
            ('time::text', response.css('time::text').get()),
        ]
        
        for selector_name, selector_result in date_selectors:
            if selector_result:
                date = selector_result.strip()
                raw_date = date
                break
        
        # Extract address
        address = self.extract_address(response)
        if address:
            address = self.remove_location_text(address)
        
        # Extract coordinates
        coords = self.extract_coordinates(response)
        
        # Build event_data for database check before geocoding
        event_data = {
            'name': title,
            'date': date,
            'url': response.url
        }
        
        # Pass event_data to enable database check before geocoding
        if address:
            geocoded_coords = self.geocode_address(address, event_data=event_data)
            if geocoded_coords:
                if not coords:
                    coords = geocoded_coords
        
        # Convert date format
        if date:
            date = self.convert_date_format(date)
        
        # Short description
        short_description = None
        if desc_parts:
            joined = '\n'.join(desc_parts).strip()
            short_description = joined.split('\n')[0]
            if len(short_description) > 200:
                short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
        
        # Set item fields
        item['name'] = self.clean_text(title) if title else None
        item['date'] = date
        item['raw_date'] = raw_date
        item['short_description'] = self.clean_text(short_description) if short_description else None
        item['coordinates'] = coords
        item['address'] = address
        item['category'] = "Wellness & Mind"
        item['subcategory'] = "Yoga"
        item['raw'] = {
            'title': title,
            'date': raw_date,
            'desc_preview': short_description,
            'full_description': ' '.join(desc_parts) if desc_parts else None,
            'address': address,
            'coordinates': coords,
        }
        
        # Check for duplicates
        item_key = f"{item['name']}_{item['date']}"
        if item_key in self.seen_events:
            self.logger.debug(f"Skipping duplicate item: {item['name']}")
            return
        
        self.seen_events.add(item_key)
        self.total_items_scraped += 1
        
        self.logger.info(f"Event extracted - Name: {item['name'][:50] if item['name'] else 'N/A'}...")
        yield item

    def extract_event_from_card(self, card, response):
        """Extract event data from a card/container element on listing page."""
        try:
            item = EventScrapingItem()
            item['category'] = self.category
            item['site'] = self.site_name
            
            # Extract URL
            url = card.css('a::attr(href)').get()
            if url:
                url = response.urljoin(url)
            item['url'] = url or response.url
            
            # Extract title
            title = (
                card.css('h1::text, h2::text, h3::text').get() or
                card.css('[class*="title"]::text').get() or
                card.css('a::text').get()
            )
            if title:
                title = title.strip()
            
            # Extract date
            date = card.css('[class*="date"]::text, time::text').get()
            raw_date = date
            
            # Extract description
            desc = ' '.join(card.css('p::text, [class*="description"]::text').getall())
            
            # Extract address
            address = ' '.join(card.css('[class*="location"], [class*="address"]::text').getall())
            if address:
                address = self.remove_location_text(address)
            
            # Convert date
            if date:
                date = self.convert_date_format(date)
            
            # Build event_data for database check before geocoding
            geocode_event_data = {
                'name': title,
                'date': date,
                'url': response.url
            }
            
            # Geocode address
            # Pass event_data to enable database check before geocoding
            coords = None
            if address:
                coords = self.geocode_address(address, event_data=geocode_event_data)
            
            # Short description
            short_description = desc[:200] + '...' if len(desc) > 200 else desc
            
            item['name'] = self.clean_text(title) if title else None
            item['date'] = date
            item['raw_date'] = raw_date
            item['short_description'] = self.clean_text(short_description) if short_description else None
            item['coordinates'] = coords
            item['address'] = address
            item['category'] = "Wellness & Mind"
            item['subcategory'] = "Yoga"
            item['raw'] = {
                'title': title,
                'date': raw_date,
                'desc_preview': short_description,
                'full_description': desc,
                'address': address,
                'coordinates': coords,
            }
            
            # Check for duplicates
            item_key = f"{item['name']}_{item['date']}"
            if item_key in self.seen_events:
                return None
            
            self.seen_events.add(item_key)
            self.total_items_scraped += 1
            
            return item
        except Exception as e:
            self.logger.debug(f"Error extracting from card: {e}")
            return None

    def remove_location_text(self, address):
        """Remove 'Location' text and similar prefixes from address."""
        if not address:
            return address
        
        patterns_to_remove = [
            r'^location\s*:?\s*-?\s*',
            r'^location\s+',
            r'\blocation\s*:?\s*-?\s*',
        ]
        
        cleaned_address = address
        for pattern in patterns_to_remove:
            cleaned_address = re.sub(pattern, '', cleaned_address, flags=re.IGNORECASE)
        
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address).strip()
        return cleaned_address if cleaned_address else address

    def extract_address(self, response):
        """Extract full address from the page."""
        address_selectors = [
            '.address::text',
            '.location::text', 
            '.venue::text',
            '.event-location::text',
            '[class*="address"]::text',
            '[class*="location"]::text',
            '[class*="venue"]::text',
        ]
        
        for selector in address_selectors:
            address = response.css(selector).get()
            if address and len(address.strip()) > 5:
                return self.clean_text(address)
        
        return None

    def convert_date_format(self, date_str):
        """Convert various date formats to MM/DD/YYYY format."""
        if not date_str:
            return None
        
        try:
            from datetime import datetime
            
            date_str = date_str.strip()
            month_names = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12',
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
            }
            
            patterns = [
                (r'(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+(\d{4})', month_names),
                (r'(\d{1,2})\s+(\w+)\s+(\d{4})', month_names),
                (r'(\d{1,2})/(\d{1,2})/(\d{4})', None),
                (r'(\d{1,2})-(\d{1,2})-(\d{4})', None),
                (r'(\d{4})-(\d{1,2})-(\d{1,2})', None),
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
                            if pattern.startswith(r'(\d{4})'):
                                year, month, day = parts
                                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
                            else:
                                day, month, year = parts
                                return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
            
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
        """Attempt to find coordinates in the page."""
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
        
        return None

