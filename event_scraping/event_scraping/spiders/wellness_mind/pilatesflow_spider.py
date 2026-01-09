import scrapy
import re
import time
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class PilatesFlowSpider(BaseSpider):
    """Spider for https://pilatesflow.uk/workshops

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    """
    name = "pilatesflow"
    category = "wellness_mind"
    site_name = "pilatesflow"
    allowed_domains = ["pilatesflow.uk"]
    start_urls = [
        "https://pilatesflow.uk/workshops"
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_events = set()
        self.geocoding_cache = {}
        self.total_items_scraped = 0

    def parse(self, response):
        """Parse the page and extract all event data directly from listing page."""
        self.logger.info(f"Parsing page: {response.url}")
        
        # Use the specific xpath to find the parent container with multiple div items
        # XPath: //*[@id="content"]/article/div/div/section[3]/div
        parent_container = response.xpath('//*[@id="content"]/article/div/div/section[3]/div')
        
        if not parent_container:
            self.logger.warning("Could not find parent container using xpath //*[@id=\"content\"]/article/div/div/section[3]/div")
            return
        
        # Find all child div items within the parent container
        event_divs = parent_container.xpath('./div')
        
        self.logger.info(f"Found {len(event_divs)} event divs in parent container")
        
        if len(event_divs) == 0:
            self.logger.warning("No event divs found in parent container")
            return
        
        # Iterate through each event div item
        for idx, event_div in enumerate(event_divs):
            try:
                item = EventScrapingItem()
                item['category'] = self.category
                item['site'] = self.site_name
                item['url'] = response.url
                
                # Extract date - look for h2 in div[3] structure relative to this event div
                # Based on pattern: section[3]/div/div[1]/div/div[3]/div/h2
                date = None
                raw_date = None
                
                # Try to find date in h2 within div[3] structure
                date_elem = event_div.xpath('.//div[3]//h2 | .//div[contains(@class, "elementor")]//h2 | .//h2')
                if date_elem:
                    date_text = date_elem.xpath('.//text()').getall()
                    if date_text:
                        date = ' '.join([t.strip() for t in date_text if t.strip()])
                        raw_date = date
                        self.logger.debug(f"Found date in event div {idx + 1}: {date}")
                
                # Extract address/location - look for span[3] in p[3] within div[5] structure
                # Based on pattern: section[3]/div/div[1]/div/div[5]/div/p[3]/span[3]
                address = None
                
                # Try the exact path relative to this event div
                location_elem = event_div.xpath('.//div[5]//div//p[3]//span[3] | .//p[3]//span[3] | .//span[3]')
                if location_elem:
                    address_parts = location_elem.xpath('.//text()').getall()
                    if address_parts:
                        address = ' '.join([a.strip() for a in address_parts if a.strip()])
                        self.logger.debug(f"Found address in event div {idx + 1}: {address}")
                
                # Fallback: Try alternative location patterns
                if not address:
                    location_elem = event_div.xpath('.//p//span[last()] | .//p[contains(@class, "location")] | .//span[contains(@class, "location")] | .//*[contains(@class, "address")]')
                    if location_elem:
                        address_parts = location_elem.xpath('.//text()').getall()
                        if address_parts:
                            address = ' '.join([a.strip() for a in address_parts if a.strip()])
                            self.logger.debug(f"Found address using fallback in event div {idx + 1}: {address}")
                
                if not address:
                    self.logger.warning(f"Could not find address for event div {idx + 1}")
                
                # Extract description - get all text from p tags within this event div
                desc_parts = event_div.xpath('.//p//text()').getall()
                description = None
                if desc_parts:
                    description = ' '.join([d.strip() for d in desc_parts if d.strip()])
                
                # Extract title - try to find it in the event div
                title = None
                title_elem = event_div.xpath('.//h1 | .//h2 | .//h3 | .//*[contains(@class, "title")]')
                if title_elem:
                    title_text = title_elem.xpath('.//text()').getall()
                    if title_text:
                        title = ' '.join([t.strip() for t in title_text if t.strip()])
                
                # If no title found, use description first line or create from date
                if not title:
                    if description:
                        title = description.split('.')[0][:100]
                    elif date:
                        title = f"Pilates Workshop - {date}"
                    else:
                        title = f"Pilates Workshop #{idx + 1}"
                
                # Try to extract multiple date-location pairs from description
                # Pattern: "Date & Time: ... Location: ..."
                # Also check raw_date if it contains multiple dates
                full_text = description or ''
                if raw_date and len(raw_date) > 50:  # Likely contains multiple dates
                    full_text = raw_date + ' ' + (description or '')
                
                events_from_description = self.extract_multiple_events_from_description(
                    full_text, title, address, response.url
                )
                
                # If we found multiple events in description, yield them
                if events_from_description:
                    for event_data in events_from_description:
                        # Only keep events that have a location
                        if not event_data.get('address'):
                            self.logger.debug(f"Skipping event without location: {event_data.get('name', 'Unknown')}")
                            continue
                        
                        event_item = EventScrapingItem()
                        event_item['category'] = self.category
                        event_item['site'] = self.site_name
                        event_item['url'] = response.url
                        event_item['name'] = self.clean_text(event_data.get('title', title))
                        event_item['date'] = event_data.get('date')
                        event_item['raw_date'] = event_data.get('raw_date')
                        # Process address - remove brackets and location text
                        event_address = event_data.get('address')
                        if event_address:
                            event_address = self.remove_location_text(event_address)
                            event_address = self.remove_brackets_from_address(event_address)
                        event_item['address'] = self.clean_text(event_address)
                        
                        # Process description
                        event_description = event_data.get('description', '')
                        if event_description:
                            # Create short description (first 200 chars)
                            if len(event_description) > 200:
                                short_description = event_description[:200].rsplit(' ', 1)[0] + '...'
                            else:
                                short_description = event_description
                            event_item['short_description'] = self.clean_text(short_description)
                        else:
                            event_item['short_description'] = None
                        
                        event_item['category'] = "Wellness & Mind"
                        event_item['subcategory'] = "Pilates"
                        
                        # Build event_data for database check before geocoding
                        geocode_event_data = {
                            'name': event_item['name'],
                            'date': event_item['date'],
                            'url': event_item['url']
                        }
                        
                        # Geocode address
                        # Pass event_data to enable database check before geocoding
                        coords = self.geocode_address(event_item['address'], event_data=geocode_event_data)
                        event_item['coordinates'] = coords
                        
                        # Prepare full description for raw field
                        full_description = event_data.get('description', '')
                        desc_preview = None
                        if full_description:
                            desc_preview = full_description[:200] if len(full_description) > 200 else full_description
                        
                        event_item['raw'] = {
                            'title': event_data.get('title', title),
                            'date': event_data.get('raw_date'),
                            'desc_preview': self.clean_text(desc_preview) if desc_preview else None,
                            'full_description': self.clean_text(full_description) if full_description else None,
                            'address': event_item['address'],
                            'coordinates': coords,
                        }
                        
                        # Check for duplicates
                        item_key = f"{event_item['name']}_{event_item['date']}"
                        if item_key in self.seen_events:
                            self.logger.debug(f"Skipping duplicate item: {event_item['name']}")
                            continue
                        
                        self.seen_events.add(item_key)
                        self.total_items_scraped += 1
                        
                        self.logger.info(f"Extracted event #{self.total_items_scraped}: {event_item['name'][:50] if event_item['name'] else 'N/A'}...")
                        yield event_item
                    
                    # Skip the original item since we've extracted multiple events
                    continue
                
                # If no multiple events found, process as single event
                # Process date - skip if no date found or if date cannot be converted
                if not date:
                    self.logger.debug(f"Skipping item without date: {title or 'Unknown'}")
                    continue
                
                # Log the raw date for debugging
                self.logger.debug(f"Attempting to convert date: '{date}'")
                
                converted_date = self.convert_date_format(date)
                # Skip if date conversion failed (returns None)
                if not converted_date:
                    self.logger.warning(f"Skipping item with invalid date format: '{date}'")
                    continue
                
                self.logger.debug(f"Successfully converted date: '{date}' -> '{converted_date}'")
                
                # Only keep events that have a location
                if not address:
                    self.logger.debug(f"Skipping event without location: {title or 'Unknown'}")
                    continue
                
                item['date'] = converted_date
                item['raw_date'] = raw_date
                
                # Process address - remove brackets and location text
                address = self.remove_location_text(address)
                address = self.remove_brackets_from_address(address)
                item['address'] = self.clean_text(address)
                
                # Build event_data for database check before geocoding
                geocode_event_data = {
                    'name': item.get('name') or title,
                    'date': item['date'],
                    'url': item['url']
                }
                
                # Geocode address - always try to get coordinates
                # Pass event_data to enable database check before geocoding
                coords = self.geocode_address(address, event_data=geocode_event_data)
                item['coordinates'] = coords
                
                # Process description
                if description:
                    # Clean description - remove location and date if they appear
                    if address and address in description:
                        description = description.replace(address, '').strip()
                    if date and date in description:
                        description = description.replace(date, '').strip()
                    
                    if len(description) > 200:
                        short_description = description[:200].rsplit(' ', 1)[0] + '...'
                    else:
                        short_description = description
                    item['short_description'] = self.clean_text(short_description)
                else:
                    item['short_description'] = None
                
                # Set item fields
                item['name'] = self.clean_text(title) if title else None
                item['category'] = "Wellness & Mind"
                item['subcategory'] = "Pilates"
                item['raw'] = {
                    'title': title,
                    'date': raw_date,
                    'desc_preview': short_description if description else None,
                    'full_description': description,
                    'address': address,
                    'coordinates': item['coordinates'],
                }
                
                # Check for duplicates
                item_key = f"{item['name']}_{item['date']}"
                if item_key in self.seen_events:
                    self.logger.debug(f"Skipping duplicate item: {item['name']}")
                    continue
                
                self.seen_events.add(item_key)
                self.total_items_scraped += 1
                
                self.logger.info(f"Extracted event #{self.total_items_scraped}: {item['name'][:50] if item['name'] else 'N/A'}...")
                yield item
                
            except Exception as e:
                self.logger.error(f"Error extracting event from div {idx + 1}: {e}")
                import traceback
                self.logger.debug(traceback.format_exc())
                continue

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
            response.css('h1 *::text').get() or
            response.css('[class*="title"]::text').get() or
            response.css('h2::text').get()
        )
        
        if title:
            title = title.strip()
        
        desc_parts = []
        desc_selectors = [
            '.description *::text',
            '.event-description *::text',
            '.content *::text',
            'article *::text',
            'p::text',
        ]
        
        for selector in desc_selectors:
            parts = response.css(selector).getall()
            if parts:
                desc_parts = [part.strip() for part in parts if part.strip()]
                if desc_parts:
                    break
        
        date = None
        raw_date = None
        date_selectors = [
            ('.date::text', response.css('.date::text').get()),
            ('time::attr(datetime)', response.css('time::attr(datetime)').get()),
            ('[class*="date"]::text', response.css('[class*="date"]::text').get()),
        ]
        
        for selector_name, selector_result in date_selectors:
            if selector_result:
                date = selector_result.strip()
                raw_date = date
                break
        
        address = self.extract_address(response)
        if address:
            address = self.remove_location_text(address)
        
        coords = self.extract_coordinates(response)
        
        # Build event_data for database check before geocoding
        # Note: date will be converted later, but we use raw_date for the check
        event_data = {
            'name': title,
            'date': raw_date or date,  # Use raw_date if available, otherwise date
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
        item['subcategory'] = "Pilates"
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
            
            title = (
                card.css('h1::text, h2::text, h3::text').get() or
                card.css('[class*="title"]::text').get() or
                card.css('a::text').get()
            )
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
            
            # Build event_data for database check before geocoding
            geocode_event_data = {
                'name': title,
                'date': date,
                'url': response.url
            }
            
            coords = None
            # Pass event_data to enable database check before geocoding
            if address:
                coords = self.geocode_address(address, event_data=geocode_event_data)
            
            short_description = desc[:200] + '...' if len(desc) > 200 else desc
            
            item['name'] = self.clean_text(title) if title else None
            item['date'] = date
            item['raw_date'] = raw_date
            item['short_description'] = self.clean_text(short_description) if short_description else None
            item['coordinates'] = coords
            item['address'] = address
            item['category'] = "Wellness & Mind"
            item['subcategory'] = "Pilates"
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

    def extract_multiple_events_from_description(self, description, base_title, fallback_address, url):
        """Extract multiple date-location pairs from description.
        
        Looks for patterns like:
        - "Date & Time: ... Location: ..."
        - "Date: ... Location: ..."
        
        Returns list of event dicts with title, date, raw_date, address, description.
        Only returns events that have a location.
        """
        if not description:
            return []
        
        events = []
        
        # Pattern to match "Date & Time: ... Location: ..."
        # Location should stop at "Why Join?" or next "Date & Time:" or end of string
        # Match location text that comes after "Location:" and stops before "Why Join?" or newline
        pattern = r'Date\s*&?\s*Time\s*:\s*([^L]+?)\s+Location\s*:\s*([^\n]+?)(?=\s+Why\s+Join\?|\s+Date\s*&?\s*Time\s*:|$)'
        
        matches = re.finditer(pattern, description, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            date_time_text = match.group(1).strip()
            location_text = match.group(2).strip()
            
            # Stop location text at "Why Join?" if present
            if 'Why Join?' in location_text:
                location_text = location_text.split('Why Join?')[0].strip()
            
            # Remove any trailing "Location:" text that might have been captured
            location_text = re.sub(r'\s*Location\s*:.*$', '', location_text, flags=re.IGNORECASE).strip()
            
            # Remove any text after a newline (location should be on one line)
            if '\n' in location_text:
                location_text = location_text.split('\n')[0].strip()
            
            # Additional cleanup: split on "Location:" if it appears in the middle (shouldn't happen but just in case)
            if 'Location:' in location_text and location_text.count('Location:') > 1:
                # Take only the first part before any subsequent "Location:"
                location_text = location_text.split('Location:')[0].strip()
            
            # Clean location text - remove trailing punctuation and extra whitespace
            location_text = re.sub(r'[^\w\s\(\)&,\.-]+$', '', location_text).strip()
            
            # Additional cleanup: remove any remaining "Location:" references at the start
            location_text = re.sub(r'^Location\s*:\s*', '', location_text, flags=re.IGNORECASE).strip()
            
            # Skip if location is empty or too short
            if not location_text or len(location_text) < 5:
                continue
            
            # Extract date from date_time_text
            # Patterns: "4th of January", "7th of January", "January 4, 2026", etc.
            date_match = None
            date_patterns = [
                r'(\d{1,2})(?:st|nd|rd|th)?\s+of\s+(January|February|March|April|May|June|July|August|September|October|November|December)',
                r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})',
                r'(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})',
            ]
            
            for date_pattern in date_patterns:
                date_match_obj = re.search(date_pattern, date_time_text, re.IGNORECASE)
                if date_match_obj:
                    date_match = date_match_obj.group(0)
                    break
            
            if not date_match:
                # Try to find any date pattern in the text
                date_match_obj = re.search(r'(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', date_time_text, re.IGNORECASE)
                if date_match_obj:
                    date_match = date_match_obj.group(0)
            
            if date_match:
                # Convert date to MM/DD/YYYY format
                converted_date = self.convert_date_format(date_match)
                if converted_date:
                    # Clean location text - remove "Location:" prefix if present
                    location_text = self.remove_location_text(location_text)
                    
                    # Extract title from base_title or create from date
                    event_title = base_title
                    if not event_title or len(event_title) < 10:
                        event_title = f"Pilates Workshop - {date_match}"
                    
                    # Get full description for this event
                    # Start from the beginning of this "Date & Time:" section
                    start_pos = match.start()
                    # Find next "Date & Time:" or end of description
                    next_match = re.search(r'Date\s*&?\s*Time\s*:', description[match.end():], re.IGNORECASE)
                    if next_match:
                        end_pos = match.end() + next_match.start()
                    else:
                        end_pos = len(description)
                    
                    # Get the full description section for this event
                    event_desc = description[start_pos:end_pos].strip()
                    
                    # If description is too long, truncate but keep meaningful content
                    if len(event_desc) > 2000:
                        # Try to keep up to "Why Join?" section if present
                        why_join_pos = event_desc.find('Why Join?')
                        if why_join_pos > 0 and why_join_pos < 2000:
                            event_desc = event_desc[:why_join_pos + 100].strip()
                        else:
                            event_desc = event_desc[:2000].strip()
                    
                    events.append({
                        'title': event_title,
                        'date': converted_date,
                        'raw_date': date_match,
                        'address': location_text if location_text else None,
                        'description': event_desc,
                    })
        
        return events
    
    def remove_location_text(self, address):
        """Remove 'Location' text from address."""
        if not address:
            return address
        cleaned = re.sub(r'^location\s*:?\s*-?\s*', '', address, flags=re.IGNORECASE)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned if cleaned else address
    
    def remove_brackets_from_address(self, address):
        """Remove text inside brackets (parentheses) from address."""
        if not address:
            return address
        # Remove text inside brackets: (text) or [text]
        cleaned = re.sub(r'\([^)]*\)', '', address)
        cleaned = re.sub(r'\[[^\]]*\]', '', cleaned)
        # Clean up extra spaces
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
        """Convert date to MM/DD/YYYY format. Returns None if conversion fails."""
        if not date_str:
            return None
        try:
            from datetime import datetime
            date_str = date_str.strip()
            
            # Remove common prefixes/suffixes that might interfere
            date_str = re.sub(r'^[Oo]n\s+', '', date_str)  # Remove "on" prefix
            date_str = date_str.strip()
            
            # Check if it's already in MM/DD/YYYY format
            try:
                datetime.strptime(date_str, '%m/%d/%Y')
                return date_str  # Already in correct format
            except ValueError:
                pass
            
            # Try various date formats - order matters, try more specific first
            formats = [
                '%A, %B %d, %Y',     # Sunday, January 4, 2026
                '%A, %B %d',          # Sunday, January 4 (assume current year)
                '%A %B %d, %Y',      # Sunday January 4, 2026 (no comma after day)
                '%A %d %B %Y',       # Sunday 4 January 2026
                '%A, %b %d, %Y',     # Sunday, Jan 4, 2026
                '%A, %b %d',         # Sunday, Jan 4 (assume current year)
                '%B %d, %Y',         # January 4, 2026
                '%b %d, %Y',         # Jan 4, 2026
                '%d %B %Y',          # 4 January 2026
                '%d %b %Y',          # 4 Jan 2026
                '%d/%m/%Y',          # 4/01/2026
                '%d-%m-%Y',          # 4-01-2026
                '%Y-%m-%d',          # 2026-01-04
                '%A %d %B %Y',       # Saturday 31 January 2026
                '%A %d %b %Y',       # Saturday 31 Jan 2026
            ]
            
            for fmt in formats:
                try:
                    parsed = datetime.strptime(date_str, fmt)
                    # If year is missing (formats without %Y), use current year
                    if '%Y' not in fmt:
                        from datetime import date
                        current_year = date.today().year
                        parsed = parsed.replace(year=current_year)
                    return parsed.strftime('%m/%d/%Y')
                except ValueError:
                    continue
            
            # Try to extract date parts manually if standard formats fail
            # Pattern: DayName, MonthName Day, Year or DayName, MonthName Day
            pattern = r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})?'
            match = re.search(pattern, date_str, re.IGNORECASE)
            if match:
                month_name = match.group(1)
                day = match.group(2)
                year = match.group(3) if match.group(3) else str(datetime.now().year)
                
                month_names = {
                    'january': '01', 'february': '02', 'march': '03', 'april': '04',
                    'may': '05', 'june': '06', 'july': '07', 'august': '08',
                    'september': '09', 'october': '10', 'november': '11', 'december': '12',
                    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                    'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                    'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
                }
                
                month_num = month_names.get(month_name.lower())
                if month_num:
                    return f"{month_num}/{day.zfill(2)}/{year}"
            
            # If no format matched, return None to indicate invalid date
            self.logger.debug(f"Could not parse date format: '{date_str}'")
            return None
        except Exception as e:
            self.logger.debug(f"Date conversion failed for '{date_str}': {e}")
            return None

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

