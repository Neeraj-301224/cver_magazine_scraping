"""Base spider class with common functionality for all spiders.

This class centralizes small helpers and common attributes used by
category-specific spiders. Child spiders should set:

- `name` (Scrapy spider name)
- `category` (one of: 'fitness_training', 'wellness_mind', 'lifestyle')
- `site_name` (short identifier for the site)
- `start_urls`

It also exposes convenience wrappers around common utilities in
`event_scraping.utils.common`.
"""
import scrapy
import re
import time
import traceback
from functools import wraps
from ..utils.common import (
    clean_text, 
    get_absolute_url, 
    extract_date,
    geocode_address as geocode_address_util,
    geocode_locationiq,
    geocode_nominatim,
    remove_location_text as remove_location_text_util,
    convert_date_format as convert_date_format_util,
    get_event_category as get_event_category_util,
    check_event_exists_in_db,
    validate_uk_coordinates
)


def log_errors(func):
    """Decorator to log errors in common functions."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            func_name = func.__name__
            error_msg = f"Error in {func_name}: {str(e)}"
            self.log_error(error_msg, exc_info=True, context={
                'function': func_name,
                'args': str(args)[:200],
                'kwargs': str(kwargs)[:200]
            })
            # Return None or original value on error depending on function
            if func_name in ['convert_date_format', 'geocode_address', 'extract_coordinates', 'extract_address']:
                return None if func_name != 'convert_date_format' else args[0] if args else None
            return None
    return wrapper


class BaseSpider(scrapy.Spider):
    """Base spider class with common methods and attributes.

    Attributes:
        category (str): logical category for the spider (set in subclasses)
        site_name (str): short name of the source site (set in subclasses)
        geocoding_cache (dict): Cache for geocoding results
    """

    category = None
    site_name = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize geocoding cache if not already present
        if not hasattr(self, 'geocoding_cache'):
            self.geocoding_cache = {}
        # Flag to enable/disable database checking (default: True to save geocoding API calls)
        self.check_db_before_geocoding = kwargs.get('check_db_before_geocoding', True)

    def clean_text(self, text):
        """Normalize and clean text using shared utility."""
        try:
            return clean_text(text)
        except Exception as e:
            self.log_error(f"Error cleaning text: {e}", context={'text': str(text)[:100]})
            return str(text) if text else ""

    def get_absolute_url(self, relative_url):
        """Convert a relative URL to an absolute URL using the first start_url."""
        try:
            if not self.start_urls:
                return relative_url
            return get_absolute_url(self.start_urls[0], relative_url)
        except Exception as e:
            self.log_error(f"Error converting URL: {e}", context={'url': relative_url})
            return relative_url

    def parse_date(self, date_str):
        """Parse a date string into a standardized format using shared util.

        Returns whatever `extract_date` produces; implement additional
        normalization here if needed.
        """
        try:
            return extract_date(date_str)
        except Exception as e:
            self.log_error(f"Error parsing date: {e}", context={'date_str': date_str})
            return date_str

    def log_error(self, message, level='error', exc_info=False, context=None):
        """Comprehensive error logging with context.
        
        Args:
            message (str): Error message
            level (str): Log level ('error', 'warning', 'info', 'debug')
            exc_info (bool): Include exception traceback
            context (dict): Additional context information
        """
        log_func = getattr(self.logger, level, self.logger.error)
        
        # Build context string
        context_str = ""
        if context:
            context_parts = [f"{k}={v}" for k, v in context.items()]
            context_str = f" | Context: {', '.join(context_parts)}"
        
        # Include spider info
        spider_info = f"[{self.name}]" if hasattr(self, 'name') else "[Unknown]"
        
        full_message = f"{spider_info} {message}{context_str}"
        
        if exc_info:
            log_func(full_message, exc_info=True)
        else:
            log_func(full_message)
        
        # Also log to error file if available
        if hasattr(self, 'error_log_file'):
            try:
                with open(self.error_log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {full_message}\n")
                    if exc_info:
                        f.write(traceback.format_exc() + "\n")
            except Exception:
                pass  # Don't fail if file logging fails

    def handle_error(self, failure):
        """Enhanced error handling for failed requests with comprehensive logging."""
        try:
            url = failure.request.url if hasattr(failure, 'request') and failure.request else "<unknown>"
        except Exception:
            url = "<unknown>"
        
        # Determine error type
        error_type = str(failure.type) if hasattr(failure, 'type') else "Unknown"
        error_value = str(failure.value) if hasattr(failure, 'value') else "Unknown"
        
        # Build context
        context = {
            'url': url,
            'error_type': error_type,
            'error_value': error_value[:200]  # Truncate long error messages
        }
        
        # Check if this is an HTTP error with a response
        if hasattr(failure.value, 'response') and failure.value.response:
            response = failure.value.response
            status = response.status
            context['status_code'] = status
            
            # Handle different status codes appropriately
            if status == 404:
                self.log_error(f"Page not found (404): {url}", level='warning', context=context)
            elif status == 403:
                self.log_error(f"Access forbidden (403): {url}", level='warning', context=context)
            elif status == 429:
                self.log_error(f"Rate limited (429): {url}", level='warning', context=context)
            elif status >= 500:
                self.log_error(f"Server error ({status}): {url}", level='error', context=context)
            else:
                self.log_error(f"HTTP error ({status}): {url}", level='error', context=context)
        else:
            # Non-HTTP error (timeout, DNS, etc.)
            self.log_error(f"Request failed: {url}", level='error', exc_info=True, context=context)
        
        return failure

    @log_errors
    def remove_location_text(self, address):
        """Remove 'Location' text and similar prefixes from address.
        
        Uses the common utility function from utils.common.
        """
        return remove_location_text_util(address)

    @log_errors
    def extract_address(self, response):
        """Extract full address from the page using multiple heuristics."""
        if not response:
            return None
        
        # Try multiple selectors for address (generic approach)
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
            '[itemprop="address"]::text',
            '[data-location]::attr(data-location)',
            '[data-venue]::attr(data-venue)',
            '[data-address]::attr(data-address)'
        ]
        
        for selector in address_selectors:
            try:
                address = response.css(selector).get()
                if address and len(address.strip()) > 5:
                    # Check if it looks like a date (to avoid extracting dates)
                    date_patterns = [
                        r'\d{1,2}(st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)', 
                        r'\d{1,2}/\d{1,2}/\d{4}', 
                        r'\d{4}-\d{2}-\d{2}'
                    ]
                    is_date = any(re.search(pattern, address, re.IGNORECASE) for pattern in date_patterns)
                    
                    if not is_date:
                        return self.clean_text(address)
            except Exception as e:
                self.log_error(f"Error with selector {selector}: {e}", level='debug')
                continue
        
        # Try to extract from description or content using postcode pattern
        try:
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
        except Exception as e:
            self.log_error(f"Error extracting address from content: {e}", level='debug')
        
        return None

    @log_errors
    def convert_date_format(self, date_str):
        """Convert various date formats to MM/DD/YYYY format.
        
        Uses the common utility function from utils.common.
        """
        try:
            result = convert_date_format_util(date_str)
            return result
        except Exception as e:
            self.log_error(f"Date conversion failed for '{date_str}': {e}", context={'date_str': date_str})
            return date_str if date_str else None

    def event_exists_in_db(self, event):
        """Check if an event already exists in the database.
        
        This can be used before geocoding to skip processing duplicate events.
        
        Args:
            event (dict): Event dictionary with 'url', 'name', and 'date' keys
            
        Returns:
            int or None: Post ID if event exists, None otherwise
        """
        try:
            # Get database config from settings if available
            db_config = None
            if hasattr(self, 'settings') and self.settings:
                # Try to get DB config from settings
                db_config = {
                    'host': self.settings.get('DB_HOST', 'localhost'),
                    'database': self.settings.get('DB_NAME', 'local'),
                    'user': self.settings.get('DB_USER', 'root'),
                    'password': self.settings.get('DB_PASSWORD', 'root'),
                    'port': self.settings.get('DB_PORT', 10017)
                }
            
            return check_event_exists_in_db(event, db_config)
        except Exception as e:
            self.log_error(f"Error checking if event exists in DB: {e}", level='debug')
            return None
    
    def should_process_event(self, name, date, url=None):
        """Check if an event should be processed (not a duplicate in database).
        
        This method checks the database for duplicates BEFORE geocoding,
        helping to save geocoding API calls.
        
        Usage in spiders:
            if not self.should_process_event(title, date, url):
                return  # Skip this event, it's already in the database
        
        Args:
            name (str): Event name/title
            date (str): Event date (MM/DD/YYYY format)
            url (str, optional): Event URL
            
        Returns:
            bool: True if event should be processed (not a duplicate), False otherwise
        """
        if not self.check_db_before_geocoding:
            return True  # Skip check if disabled
        
        try:
            event_data = {
                'name': name,
                'date': date,
                'url': url
            }
            existing_post_id = self.event_exists_in_db(event_data)
            if existing_post_id:
                self.logger.info(f"Skipping duplicate event (exists as post ID {existing_post_id}): {name[:50]}")
                return False
            return True
        except Exception as e:
            self.log_error(f"Error in should_process_event: {e}", level='warning')
            return True  # If check fails, allow processing to continue
    
    @log_errors
    def geocode_address(self, address, event_data=None):
        """Get coordinates from address using LocationIQ first, then fallback to Nominatim.
        
        This method uses the common geocoding utilities from utils.common.
        It handles caching and settings automatically.
        
        If event_data is provided and check_db_before_geocoding is True,
        it will check if the event exists in the database first to avoid
        unnecessary geocoding API calls.
        
        Tries services in order:
        1. LocationIQ (if API key is configured) - faster and more reliable
        2. Nominatim (OpenStreetMap) - free fallback option
        
        Args:
            address (str): Address to geocode
            event_data (dict, optional): Event dictionary to check for duplicates
        
        Returns:
            dict: {'lat': float, 'lon': float} or None if all services fail
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
        
        # Initialize cache if needed
        if not hasattr(self, 'geocoding_cache'):
            self.geocoding_cache = {}
        
        # Get LocationIQ API key from settings
        locationiq_api_key = None
        if hasattr(self, 'settings') and self.settings:
            locationiq_api_key = self.settings.get('LOCATIONIQ_API_KEY')
        
        # Get user agent for Nominatim
        user_agent = f'{self.__class__.__name__}/1.0'
        
        # Use common geocoding utility
        try:
            coords = geocode_address_util(
                address=address,
                locationiq_api_key=locationiq_api_key,
                user_agent=user_agent,
                cache=self.geocoding_cache
            )
            
            if coords:
                # Validate coordinates are within UK bounds
                is_valid, reason = validate_uk_coordinates(coords)
                if not is_valid:
                    self.log_error(f"Geocoded coordinates are invalid: {reason}. Address: {address[:50]}", 
                                 level='warning', context={'address': address[:100], 'coords': coords})
                    return None  # Don't return invalid coordinates
                
                self.logger.debug(f"Geocoded '{address[:50]}...' -> {coords['lat']}, {coords['lon']}")
            
            return coords
        except Exception as e:
            self.log_error(f"Geocoding failed: {e}", 
                         level='warning', context={'address': address[:100]})
            return None

    @log_errors
    def extract_coordinates(self, response):
        """Attempt to find coordinates (lat, lon) in the page using multiple heuristics."""
        if not response:
            return None
        
        # Try meta tags
        lat = response.css('meta[property="place:location:latitude"]::attr(content)').get()
        lon = response.css('meta[property="place:location:longitude"]::attr(content)').get()
        
        if lat and lon:
            try:
                lat_f = float(lat.strip())
                lon_f = float(lon.strip())
                if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                    return {'lat': lat_f, 'lon': lon_f}
            except (ValueError, TypeError):
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
            except (ValueError, TypeError):
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
                except (ValueError, IndexError, TypeError):
                    continue
        
        return None

    def get_event_category(self, title, description_parts):
        """Determine the specific category and subcategory for an event.
        
        Uses the common utility function from utils.common.
        Automatically uses CATEGORY_KEYWORDS if available on the spider.
        
        For community_social spiders, always returns "Charity Events" as the category.
        
        Returns (category, subcategory) tuple or (None, None).
        """
        # All community_social spiders should be categorized as "Charity Events"
        if hasattr(self, 'category') and self.category == "community_social":
            # Still determine subcategory based on event content for better categorization
            category_keywords = getattr(self, 'CATEGORY_KEYWORDS', None)
            try:
                _, subcategory = get_event_category_util(title, description_parts, category_keywords)
                # Return "Charity Events" as category, but keep the subcategory if found
                if subcategory:
                    self.logger.debug(f"Event categorized as: Charity Events -> {subcategory}")
                    return "Charity Events", subcategory
                else:
                    return "Charity Events", "General"
            except Exception as e:
                self.log_error(f"Error categorizing event: {e}", context={'title': title[:100]})
                return "Charity Events", "General"
        
        # For other spiders, use normal keyword-based categorization
        category_keywords = getattr(self, 'CATEGORY_KEYWORDS', None)
        
        try:
            category, subcategory = get_event_category_util(title, description_parts, category_keywords)
            
            if category and subcategory:
                self.logger.debug(f"Event categorized as: {category} -> {subcategory}")
            
            return category, subcategory
        except Exception as e:
            self.log_error(f"Error categorizing event: {e}", context={'title': title[:100]})
            return None, None