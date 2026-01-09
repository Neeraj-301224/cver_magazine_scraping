import scrapy
import re
import time
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class MindfulnessUKSpider(BaseSpider):
    """Spider for https://mindfulnessuk.com/retreats

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    """
    name = "mindfulnessuk"
    category = "wellness_mind"
    site_name = "mindfulnessuk"
    allowed_domains = ["mindfulnessuk.com"]
    start_urls = [
        "https://mindfulnessuk.com/retreats"
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_events = set()
        self.geocoding_cache = {}
        self.total_items_scraped = 0

    def parse(self, response):
        """Parse the page and extract event links."""
        self.logger.info(f"Parsing page: {response.url}")
        
        event_link_selectors = [
            'a[href*="/retreats/"]::attr(href)',
            'a[href*="/retreat/"]::attr(href)',
            '[class*="retreat"] a::attr(href)',
            '[class*="event"] a::attr(href)',
            'article a::attr(href)',
        ]
        
        event_links_found = 0
        seen_urls = set()
        
        for selector in event_link_selectors:
            links = response.css(selector).getall()
            for link in links:
                if link:
                    absolute_url = response.urljoin(link)
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
        
        if event_links_found == 0:
            self.logger.info("No event links found, trying to extract from listing page...")
            event_cards = response.css('[class*="retreat"], [class*="event"], article, .card')
            for card in event_cards:
                try:
                    item = self.extract_event_from_card(card, response)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.debug(f"Error extracting from card: {e}")

    def parse_event(self, response):
        """Parse individual event pages."""
        self.logger.info(f"Parsing event page: {response.url}")
        
        item = EventScrapingItem()
        item['category'] = self.category
        item['site'] = self.site_name
        item['url'] = response.url

        title = (
            response.css('h1::text').get() or
            response.css('.event-title::text').get() or
            response.css('.title::text').get() or
            response.css('[class*="title"]::text').get() or
            response.css('h2::text').get()
        )
        
        if title:
            title = title.strip()
        
        desc_parts = []
        for selector in ['.description *::text', '.content *::text', 'article *::text', 'p::text']:
            parts = response.css(selector).getall()
            if parts:
                desc_parts = [part.strip() for part in parts if part.strip()]
                if desc_parts:
                    break
        
        date = None
        raw_date = None
        for selector in ['.date::text', 'time::attr(datetime)', '[class*="date"]::text']:
            result = response.css(selector).get()
            if result:
                date = result.strip()
                raw_date = date
                break
        
        address = self.extract_address(response)
        if address:
            address = self.remove_location_text(address)
        
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
            if geocoded_coords and not coords:
                coords = geocoded_coords
        
        if date:
            date = self.convert_date_format(date)
        
        short_description = None
        if desc_parts:
            joined = '\n'.join(desc_parts).strip()
            short_description = joined.split('\n')[0]
            if len(short_description) > 200:
                short_description = short_description[:200].rsplit(' ', 1)[0] + '...'
        
        item['name'] = self.clean_text(title) if title else None
        item['date'] = date
        item['raw_date'] = raw_date
        item['short_description'] = self.clean_text(short_description) if short_description else None
        item['coordinates'] = coords
        item['address'] = address
        item['category'] = "Wellness & Mind"
        item['subcategory'] = "Mindfulness"
        item['raw'] = {
            'title': title,
            'date': raw_date,
            'desc_preview': short_description,
            'full_description': ' '.join(desc_parts) if desc_parts else None,
            'address': address,
            'coordinates': coords,
        }
        
        item_key = f"{item['name']}_{item['date']}"
        if item_key in self.seen_events:
            return
        
        self.seen_events.add(item_key)
        self.total_items_scraped += 1
        
        self.logger.info(f"Event extracted - Name: {item['name'][:50] if item['name'] else 'N/A'}...")
        yield item

    def extract_event_from_card(self, card, response):
        """Extract event data from a card element."""
        try:
            item = EventScrapingItem()
            item['category'] = self.category
            item['site'] = self.site_name
            
            url = card.css('a::attr(href)').get()
            if url:
                url = response.urljoin(url)
            item['url'] = url or response.url
            
            title = card.css('h1::text, h2::text, h3::text, [class*="title"]::text, a::text').get()
            if title:
                title = title.strip()
            
            date = card.css('[class*="date"]::text, time::text').get()
            raw_date = date
            
            desc = ' '.join(card.css('p::text, [class*="description"]::text').getall())
            
            address = ' '.join(card.css('[class*="location"], [class*="address"]::text').getall())
            if address:
                address = self.remove_location_text(address)
            
            if date:
                date = self.convert_date_format(date)
            
            coords = None
            if address:
                coords = self.geocode_address(address)
            
            short_description = desc[:200] + '...' if len(desc) > 200 else desc
            
            item['name'] = self.clean_text(title) if title else None
            item['date'] = date
            item['raw_date'] = raw_date
            item['short_description'] = self.clean_text(short_description) if short_description else None
            item['coordinates'] = coords
            item['address'] = address
            item['category'] = "Wellness & Mind"
            item['subcategory'] = "Mindfulness"
            item['raw'] = {
                'title': title,
                'date': raw_date,
                'desc_preview': short_description,
                'full_description': desc,
                'address': address,
                'coordinates': coords,
            }
            
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
        """Remove 'Location' text from address."""
        if not address:
            return address
        cleaned = re.sub(r'^location\s*:?\s*-?\s*', '', address, flags=re.IGNORECASE)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned if cleaned else address

    def extract_address(self, response):
        """Extract address from the page."""
        for selector in ['.address::text', '.location::text', '[class*="address"]::text', '[class*="location"]::text']:
            address = response.css(selector).get()
            if address and len(address.strip()) > 5:
                return self.clean_text(address)
        return None

    def convert_date_format(self, date_str):
        """Convert date to MM/DD/YYYY format."""
        if not date_str:
            return None
        try:
            from datetime import datetime
            date_str = date_str.strip()
            formats = ['%d %B %Y', '%d %b %Y', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d']
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt).strftime('%m/%d/%Y')
                except ValueError:
                    continue
            return date_str
        except Exception as e:
            self.logger.error(f"Date conversion failed: {e}")
            return date_str

    # geocode_address is inherited from BaseSpider, which uses the common function
    # that tries LocationIQ first (if API key is configured), then falls back to Nominatim.
    # It also checks the database before geocoding if event_data is provided and
    # check_db_before_geocoding is enabled.

    def extract_coordinates(self, response):
        """Extract coordinates from page."""
        lat = response.css('meta[property="place:location:latitude"]::attr(content)').get()
        lon = response.css('meta[property="place:location:longitude"]::attr(content)').get()
        if lat and lon:
            try:
                lat_f, lon_f = float(lat.strip()), float(lon.strip())
                if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                    return {'lat': lat_f, 'lon': lon_f}
            except ValueError:
                pass
        return None

