import scrapy
import re
import time
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class MindspaceSpider(BaseSpider):
    """Spider for https://www.mindspace.org.uk/retreats/

    Targets event listings and individual event pages to extract:
    - title, date, location, url, description
    """
    name = "mindspace"
    category = "wellness_mind"
    site_name = "mindspace"
    allowed_domains = ["mindspace.org.uk"]
    start_urls = [
        "https://www.mindspace.org.uk/retreats/"
    ]
    
    custom_settings = {
        'HTTPERROR_ALLOWED_CODES': [403, 404],  # Allow 403 responses
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_events = set()
        self.geocoding_cache = {}
        self.total_items_scraped = 0

    def parse(self, response):
        """Parse the page and extract event links."""
        self.logger.info(f"Parsing page: {response.url}")
        self.logger.info(f"Response status: {response.status}")
        
        # If we got a 403, log it but try to parse anyway
        if response.status == 403:
            self.logger.warning("Received 403 Forbidden, but attempting to parse response anyway...")
        
        # Find event links on the page
        event_link_selectors = [
            'a[href*="/retreats/"]::attr(href)',
            'a[href*="/retreat/"]::attr(href)',
            '[class*="retreat"] a::attr(href)',
            '[class*="event"] a::attr(href)',
            'article a::attr(href)',
            'a[href*="mindspace.org.uk/retreat"]::attr(href)',
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
        
        # Extract events directly from listing page
        # Structure: .et_pb_section.et_pb_section_2.et_section_regular > .et_pb_row > .et_pb_text_inner > h3 (location) + p (date, name, description)
        self.logger.info("Extracting events directly from listing page...")
        
        # Find all text_inner divs (these contain the event data)
        text_inners = response.css('.et_pb_text_inner')
        self.logger.info(f"Found {len(text_inners)} divs with class et_pb_text_inner")
        
        # If no text_inner found, try alternative selectors
        if len(text_inners) == 0:
            self.logger.warning("No et_pb_text_inner found, trying alternative selectors...")
            text_inners = response.css('[class*="et_pb_text"], [class*="text_inner"]')
            self.logger.info(f"Found {len(text_inners)} divs with alternative selectors")
        
        # Also try XPath in case CSS doesn't work
        if len(text_inners) == 0:
            self.logger.warning("Trying XPath selector...")
            text_inners = response.xpath('//div[contains(@class, "et_pb_text_inner")]')
            self.logger.info(f"Found {len(text_inners)} divs with XPath")
        
        if len(text_inners) == 0:
            self.logger.error("CRITICAL: No et_pb_text_inner divs found! Cannot extract events.")
            self.logger.info("Trying to find any h3 elements on the page...")
            all_h3 = response.css('h3')
            self.logger.info(f"Found {len(all_h3)} h3 elements total")
            if len(all_h3) > 0:
                for i, h3 in enumerate(all_h3[:5]):  # Show first 5
                    h3_text = h3.css('::text').get()
                    self.logger.info(f"  H3 {i+1}: {h3_text}")
            return
        
        # Process each text_inner div
        self.logger.info(f"Processing {len(text_inners)} text_inner divs...")
        for idx, text_inner in enumerate(text_inners):
            self.logger.info(f"--- Processing text_inner {idx + 1}/{len(text_inners)} ---")
            try:
                # Get location from h3 inside et_pb_text_inner
                location_elem = text_inner.css('h3')
                if not location_elem:
                    self.logger.info(f"Text_inner {idx + 1}: No h3 found, skipping")
                    continue
                
                location_text = location_elem.css('::text').get()
                if not location_text:
                    self.logger.info(f"Text_inner {idx + 1}: h3 has no text, skipping")
                    continue
                
                location_text = location_text.strip()
                self.logger.info(f"Text_inner {idx + 1}: Found h3 text: '{location_text}'")
                
                # Use the h3 text as location (don't require exact match to known locations)
                # Check if this heading contains a known location (preferred)
                known_locations = ['Cannock Chase', 'Malvern Hills', 'Warwick', 'Hammersmith', 'London', 'Surrey', 'Kings Heath', 'Edgbaston', 'Tuscany', 'Thailand', 'Lake District', 'Sutton Coldfield', 'West London']
                location_match = None
                for loc in known_locations:
                    if loc.lower() in location_text.lower():
                        location_match = loc
                        self.logger.debug(f"Text_inner {idx + 1}: Matched known location: {location_match}")
                        break
                
                # If no known location match, use the h3 text itself as location
                if not location_match:
                    location_match = location_text
                    self.logger.info(f"Text_inner {idx + 1}: Using h3 text as location: '{location_match}'")
                
                # Get p tag inside et_pb_text_inner - contains date, name, description
                p_elem = text_inner.css('p')
                if not p_elem:
                    self.logger.info(f"Text_inner {idx + 1}: No p tag found, skipping")
                    continue
                
                # Get all text from p tag
                p_text = ' '.join(p_elem.css('::text').getall()).strip()
                if not p_text:
                    self.logger.info(f"Text_inner {idx + 1}: p tag has no text, skipping")
                    continue
                
                self.logger.info(f"Text_inner {idx + 1}: p tag text: '{p_text[:150]}...'")
                
                # Extract date (first in p tag)
                date_match = None
                date_patterns = [
                    r'(Saturday|Sunday|Monday|Tuesday|Wednesday|Thursday|Friday)\s+(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
                    r'(Saturday|Sunday|Monday|Tuesday|Wednesday|Thursday|Friday)\s+(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\s+(\d{4})',
                    r'(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
                    r'Friday\s+(\d{1,2})(?:st|nd|rd|th)?-(\d{1,2})(?:st|nd|rd|th)?\s+(June|July|August|September|October|November|December),?\s+(\d{4})',  # Date ranges
                    r'Tuesday\s+(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\s+–\s+Thursday\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})',  # Date ranges like "Tuesday September 1st – Thursday 3rd, 2026"
                    r'Late\s+(November|December)\s+(\d{4})',  # "Late November 2026"
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, p_text, re.IGNORECASE)
                    if match:
                        date_match = match.group(0)
                        break
                
                # Only create item if we have a date (required)
                if not date_match:
                    self.logger.warning(f"Text_inner {idx + 1}: Skipping event without date. Location: {location_match}, p_text: '{p_text[:100]}...'")
                    continue
                
                self.logger.info(f"Text_inner {idx + 1}: Found event - Location: {location_match}, Date: {date_match}")
                
                # Extract title/name from bold/strong text in p tag (comes after date)
                title = None
                title_elem = p_elem.css('strong::text, b::text')
                if title_elem:
                    title_parts = title_elem.getall()
                    if title_parts:
                        # Get the first bold text that's not the date
                        for title_part in title_parts:
                            title_part = title_part.strip()
                            if title_part and title_part.lower() != date_match.lower():
                                title = title_part
                                break
                
                # Extract description - everything after date and title in p tag
                description = p_text
                # Remove date from description
                if date_match and date_match in description:
                    description = description.replace(date_match, '', 1).strip()
                # Remove title from description if found
                if title and title in description:
                    description = description.replace(title, '', 1).strip()
                # Remove location if present
                if location_match and location_match in description:
                    description = description.replace(location_match, '', 1).strip()
                
                # Clean up description
                description = re.sub(r'\s+', ' ', description).strip()
                
                # Create item
                item = EventScrapingItem()
                item['category'] = self.category
                item['site'] = self.site_name
                item['url'] = response.url
                
                # Build name from location and title
                name_parts = []
                if location_match:
                    name_parts.append(location_match)
                if title:
                    name_parts.append(title)
                elif date_match:
                    name_parts.append(date_match)
                
                item['name'] = self.clean_text(' - '.join(name_parts)) if name_parts else None
                
                # Process date - validate conversion
                raw_date = date_match
                converted_date = self.convert_date_format(date_match)
                if not converted_date:
                    self.logger.warning(f"Skipping event with invalid date format: {date_match}")
                    continue
                
                item['date'] = converted_date
                item['raw_date'] = raw_date
                
                # Process description
                if description:
                    if len(description) > 200:
                        short_description = description[:200].rsplit(' ', 1)[0] + '...'
                    else:
                        short_description = description
                    item['short_description'] = self.clean_text(short_description)
                else:
                    item['short_description'] = None
                
                # Process location/address
                address = location_match
                if address:
                    # Build event_data for database check before geocoding
                    geocode_event_data = {
                        'name': item['name'],
                        'date': item['date'],
                        'url': item['url']
                    }
                    
                    # Try to geocode
                    # Pass event_data to enable database check before geocoding
                    coords = self.geocode_address(address, event_data=geocode_event_data)
                    item['coordinates'] = coords
                    item['address'] = self.clean_text(address)
                else:
                    item['address'] = None
                    item['coordinates'] = None
                
                item['category'] = "Wellness & Mind"
                item['subcategory'] = "Mindfulness"
                item['raw'] = {
                    'title': title,
                    'date': raw_date,
                    'desc_preview': description,
                    'full_description': description,
                    'address': address,
                    'coordinates': item['coordinates'],
                }
                
                # Check for duplicates
                item_key = f"{item['name']}_{item['date']}"
                if item_key in self.seen_events:
                    continue
                
                self.seen_events.add(item_key)
                self.total_items_scraped += 1
                
                self.logger.info(f"Extracted event: {item['name'][:50] if item['name'] else 'N/A'}...")
                yield item
                    
            except Exception as e:
                self.logger.debug(f"Error extracting from section: {e}")
                continue
        
        # Also try to extract from structured elements (if page uses specific classes)
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
            month_names = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12',
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
            }
            
            # Handle patterns like "Saturday 31st January 2026"
            pattern1 = r'(?:Saturday|Sunday|Monday|Tuesday|Wednesday|Thursday|Friday)\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+(\d{4})'
            match = re.search(pattern1, date_str, re.IGNORECASE)
            if match:
                day, month_name, year = match.groups()
                month_num = month_names.get(month_name.lower())
                if month_num:
                    return f"{month_num}/{day.zfill(2)}/{year}"
            
            # Handle patterns like "31st January 2026"
            pattern2 = r'(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+(\d{4})'
            match = re.search(pattern2, date_str, re.IGNORECASE)
            if match:
                day, month_name, year = match.groups()
                month_num = month_names.get(month_name.lower())
                if month_num:
                    return f"{month_num}/{day.zfill(2)}/{year}"
            
            # Handle date ranges like "Friday 5th-7th June, 2026"
            pattern3 = r'(\d{1,2})(?:st|nd|rd|th)?-(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+(\d{4})'
            match = re.search(pattern3, date_str, re.IGNORECASE)
            if match:
                day_start, day_end, month_name, year = match.groups()
                month_num = month_names.get(month_name.lower())
                if month_num:
                    # Use the start date
                    return f"{month_num}/{day_start.zfill(2)}/{year}"
            
            # Handle standard formats
            formats = ['%d %B %Y', '%d %b %Y', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%B %d, %Y', '%b %d, %Y']
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt).strftime('%m/%d/%Y')
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

