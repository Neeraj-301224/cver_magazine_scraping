import scrapy
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class MindfulnessAssociationSpider(BaseSpider):
    """Spider for https://www.mindfulnessassociation.net/mindfulness-courses/all-courses-and-retreats/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    """
    name = "mindfulnessassociation"
    category = "wellness_mind"
    site_name = "mindfulnessassociation"
    allowed_domains = ["mindfulnessassociation.net"]
    start_urls = [
        "https://www.mindfulnessassociation.net/mindfulness-courses/all-courses-and-retreats/"
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_events = set()
        # geocoding_cache is now initialized in BaseSpider
        self.total_items_scraped = 0

    def parse(self, response):
        """Parse the page and extract event links."""
        self.logger.info(f"Parsing page: {response.url}")
        
        event_link_selectors = [
            'a[href*="/mindfulness-courses/"]::attr(href)',
            'a[href*="/course/"]::attr(href)',
            'a[href*="/retreat/"]::attr(href)',
            '[class*="course"] a::attr(href)',
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
                    if '/mindfulness-courses/' in absolute_url or '/course/' in absolute_url or '/retreat/' in absolute_url:
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
            event_cards = response.css('[class*="course"], [class*="event"], [class*="retreat"], article, .card')
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
        
        # Extract date from mec-start-date-label and mec-end-date-label
        start_date = None
        end_date = None
        
        # Try to get start date
        start_date_elem = response.css('.mec-start-date-label::text').get()
        if start_date_elem:
            start_date = start_date_elem.strip()
        
        # Try to get end date
        end_date_elem = response.css('.mec-end-date-label::text').get()
        if end_date_elem:
            end_date = end_date_elem.strip()
        
        # If start date exists, use it (with or without end date)
        if start_date:
            if end_date:
                # Both dates exist - combine them
                if start_date == end_date:
                    raw_date = start_date
                else:
                    raw_date = f"{start_date} - {end_date}"
            else:
                # Only start date exists - use only start date
                raw_date = start_date
        elif end_date:
            # Only end date exists (unusual but handle it)
            raw_date = end_date
        else:
            # No MEC dates found - fallback to other date selectors
            for selector in ['.date::text', 'time::attr(datetime)', '[class*="date"]::text']:
                result = response.css(selector).get()
                if result:
                    raw_date = result.strip()
                    break
        
        if raw_date:
            date = self.convert_date_format(raw_date)
        
        # Extract address from mec-sl-location-pin
        address = None
        location_elem = response.css('.mec-sl-location-pin::text').get()
        if location_elem:
            address = location_elem.strip()
            address = self.remove_location_text(address)
        
        # Fallback to other address selectors if mec class not found
        if not address:
            address = self.extract_address(response)
            if address:
                address = self.remove_location_text(address)
        
        coords = self.extract_coordinates(response)
        if address:
            geocoded_coords = self.geocode_address(address)
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
            
            # Extract date from mec-start-date-label and mec-end-date-label
            start_date = None
            end_date = None
            
            # Try to get start date
            start_date_elem = card.css('.mec-start-date-label::text').get()
            if start_date_elem:
                start_date = start_date_elem.strip()
            
            # Try to get end date
            end_date_elem = card.css('.mec-end-date-label::text').get()
            if end_date_elem:
                end_date = end_date_elem.strip()
            
            # If start date exists, use it (with or without end date)
            if start_date:
                if end_date:
                    # Both dates exist - combine them
                    if start_date == end_date:
                        raw_date = start_date
                    else:
                        raw_date = f"{start_date} - {end_date}"
                else:
                    # Only start date exists - use only start date
                    raw_date = start_date
            elif end_date:
                # Only end date exists (unusual but handle it)
                raw_date = end_date
            else:
                # No MEC dates found - fallback to other date selectors
                raw_date = card.css('[class*="date"]::text, time::text').get()
            
            date = raw_date
            
            desc = ' '.join(card.css('p::text, [class*="description"]::text').getall())
            
            # Extract address from mec-sl-location-pin
            address = card.css('.mec-sl-location-pin::text').get()
            if address:
                address = address.strip()
                address = self.remove_location_text(address)
            else:
                # Fallback to other location selectors
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

    def extract_address(self, response):
        """Extract address from the page with site-specific selector first.
        
        This overrides the base class method to check for site-specific
        .mec-sl-location-pin selector first, then falls back to base class method.
        """
        # First try site-specific selector: mec-sl-location-pin
        address = response.css('.mec-sl-location-pin::text').get()
        if address and len(address.strip()) > 5:
            return self.clean_text(address.strip())
        
        # Fallback to base class method which tries multiple generic selectors
        return super().extract_address(response)
    
    # Note: The following methods are now inherited from BaseSpider:
    # - remove_location_text() - automatically available
    # - convert_date_format() - automatically available  
    # - geocode_address() - automatically available with error logging
    # - extract_coordinates() - automatically available
    # All these methods now have comprehensive error logging built-in!

