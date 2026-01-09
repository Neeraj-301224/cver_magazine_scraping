"""Common utilities for all spiders."""
import re
import time
from datetime import datetime


def clean_text(text):
    """Clean and normalize text."""
    if text is None:
        return ""
    return " ".join(text.strip().split())


def extract_date(date_str):
    """Extract and standardize date from various formats."""
    # Add date parsing logic as needed
    pass


def get_absolute_url(base_url, relative_url):
    """Convert relative URL to absolute URL."""
    from urllib.parse import urljoin
    return urljoin(base_url, relative_url)


def geocode_locationiq(address, api_key):
    """Geocode using LocationIQ API.
    
    Args:
        address (str): Address to geocode
        api_key (str): LocationIQ API key
        
    Returns:
        dict: {'lat': float, 'lon': float} or None
        
    Raises:
        Exception: If geocoding fails
    """
    import requests
    
    url = "https://us1.locationiq.com/v1/search.php"
    params = {
        'key': api_key,
        'q': address,
        'format': 'json',
        'limit': 1,
        'countrycodes': 'gb',  # UK only
        'addressdetails': 1
    }
    
    # Rate limiting: LocationIQ allows 2 requests/second (free tier)
    # Reduced delay for faster processing - adjust if you hit rate limits
    time.sleep(0.1)
    
    response = requests.get(url, params=params, timeout=10)
    
    # Handle specific error codes
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


def geocode_nominatim(address, user_agent='EventScrapingBot/1.0'):
    """Geocode using Nominatim (OpenStreetMap) API.
    
    Args:
        address (str): Address to geocode
        user_agent (str): User agent string for the request
        
    Returns:
        dict: {'lat': float, 'lon': float} or None
        
    Raises:
        Exception: If geocoding fails
    """
    import requests
    
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': address,
        'format': 'json',
        'limit': 1,
        'countrycodes': 'gb',  # UK only
        'addressdetails': 1
    }
    
    headers = {
        'User-Agent': user_agent
    }
    
    # Rate limiting (Nominatim requirement: 1 request per second)
    time.sleep(1.1)
    
    response = requests.get(url, params=params, headers=headers, timeout=10)
    
    if response.status_code == 200:
        data = response.json()
        if data:
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            
            # Validate UK bounds
            if 49 <= lat <= 61 and -8 <= lon <= 2:
                return {'lat': lat, 'lon': lon}
    else:
        raise Exception(f"Nominatim returned status {response.status_code}")
    
    return None


def geocode_address(address, locationiq_api_key=None, user_agent='EventScrapingBot/1.0', cache=None):
    """Geocode an address using LocationIQ first, then fallback to Nominatim.
    
    Tries services in order:
    1. LocationIQ (if API key is provided) - faster and more reliable
    2. Nominatim (OpenStreetMap) - free fallback option
    
    Args:
        address (str): Address to geocode
        locationiq_api_key (str, optional): LocationIQ API key. If None, skips LocationIQ.
        user_agent (str): User agent for Nominatim requests
        cache (dict, optional): Cache dictionary to store results. If provided, checks cache first.
        
    Returns:
        dict: {'lat': float, 'lon': float} or None if all services fail
    """
    if not address:
        return None
    
    # Check cache first if provided
    if cache is not None and address in cache:
        return cache[address]
    
    # Try LocationIQ first (if API key is provided)
    if locationiq_api_key:
        try:
            coords = geocode_locationiq(address, locationiq_api_key)
            if coords:
                # Store in cache if provided
                if cache is not None:
                    cache[address] = coords
                return coords
        except Exception:
            # Silently fail and try Nominatim
            pass
    
    # Fallback to Nominatim (OpenStreetMap)
    try:
        coords = geocode_nominatim(address, user_agent)
        if coords:
            # Store in cache if provided
            if cache is not None:
                cache[address] = coords
            return coords
    except Exception:
        # Silently fail
        pass
    
    return None


def remove_location_text(address):
    """Remove 'Location' text and similar prefixes from address.
    
    Args:
        address (str): Address string that may contain location prefixes
        
    Returns:
        str: Cleaned address with location prefixes removed
    """
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


def convert_date_format(date_str):
    """Convert various date formats to MM/DD/YYYY format.
    
    Supports multiple date formats:
    - ISO format: 2025-11-29
    - UK format: 30 October 2025, 30/10/2025
    - US format: 10/30/2025
    - And many more variations
    
    Args:
        date_str (str): Date string in various formats
        
    Returns:
        str: Date in MM/DD/YYYY format, or original string if conversion fails
    """
    if not date_str:
        return None
    
    try:
        # Clean the date string
        date_str = str(date_str).strip()
        
        # Month name mappings
        month_names = {
            'january': '01', 'february': '02', 'march': '03', 'april': '04',
            'may': '05', 'june': '06', 'july': '07', 'august': '08',
            'september': '09', 'october': '10', 'november': '11', 'december': '12',
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        
        # Handle ISO datetime format (e.g., "2025-11-29T00:00:00Z")
        iso_pattern = r'(\d{4})-(\d{1,2})-(\d{1,2})'
        iso_match = re.search(iso_pattern, date_str)
        if iso_match:
            year, month, day = iso_match.groups()
            return f"{month.zfill(2)}/{day.zfill(2)}/{year}"
        
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
                    month_num = month_names.get(month_name.lower())
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
            '%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y', '%b %d, %Y',
            '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S%z'
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%m/%d/%Y')
            except ValueError:
                continue
        
        return date_str
        
    except Exception:
        # Return original string on any error
        return date_str


def get_event_category(title, description_parts, category_keywords=None):
    """Determine the specific category and subcategory for an event based on keywords.
    
    Args:
        title (str): Event title
        description_parts (list or str): Event description parts
        category_keywords (dict, optional): Dictionary mapping categories to keywords.
            Format: {
                'Category': {
                    'Subcategory': ['keyword1', 'keyword2', ...]
                }
            }
            If None, returns (None, None)
        
    Returns:
        tuple: (category, subcategory) or (None, None) if no match found
    """
    if not category_keywords:
        return None, None
    
    if not title:
        return None, None
    
    try:
        # Combine title and description for analysis
        full_text = title.lower()
        if description_parts:
            if isinstance(description_parts, list):
                full_text += " " + " ".join(str(p).lower() for p in description_parts)
            else:
                full_text += " " + str(description_parts).lower()
        
        # Check each category group and subcategory
        for category_group, subcategories in category_keywords.items():
            for subcategory, keywords in subcategories.items():
                for keyword in keywords:
                    if keyword.lower() in full_text:
                        return category_group, subcategory
        
        return "Other", "General"
    except Exception:
        return None, None


def validate_uk_coordinates(coords):
    """Validate that coordinates are valid UK coordinates.
    
    Checks:
    1. Coordinates are not None or empty
    2. Coordinates are within UK bounds (lat 49-61, lon -8 to 2)
    3. Coordinates are not obviously wrong (like 0,0 or in the middle of ocean)
    
    Args:
        coords (dict or None): Coordinates dict with 'lat' and 'lon' keys, or None
        
    Returns:
        tuple: (is_valid: bool, reason: str)
            - is_valid: True if coordinates are valid UK coordinates
            - reason: Explanation if invalid, empty string if valid
    """
    if not coords:
        return False, "Coordinates are missing"
    
    if not isinstance(coords, dict):
        return False, "Coordinates must be a dictionary"
    
    lat = coords.get('lat')
    lon = coords.get('lon')
    
    if lat is None or lon is None:
        return False, "Latitude or longitude is missing"
    
    try:
        lat = float(lat)
        lon = float(lon)
    except (ValueError, TypeError):
        return False, "Invalid coordinate values (not numeric)"
    
    # Check for obviously wrong coordinates (0,0 is in the Gulf of Guinea)
    if lat == 0.0 and lon == 0.0:
        return False, "Coordinates are (0,0) - likely invalid"
    
    # UK bounds: approximately lat 49-61, lon -8 to 2
    # Using slightly wider bounds to account for coastal areas
    UK_LAT_MIN = 49.0
    UK_LAT_MAX = 61.5
    UK_LON_MIN = -8.5
    UK_LON_MAX = 2.0
    
    if not (UK_LAT_MIN <= lat <= UK_LAT_MAX):
        return False, f"Latitude {lat} is outside UK bounds ({UK_LAT_MIN}-{UK_LAT_MAX})"
    
    if not (UK_LON_MIN <= lon <= UK_LON_MAX):
        return False, f"Longitude {lon} is outside UK bounds ({UK_LON_MIN}-{UK_LON_MAX})"
    
    # Check for coordinates that are clearly in the ocean (rough check)
    # Areas far from UK landmass
    # This is a simple heuristic - coordinates in the middle of the Atlantic
    if lat < 50 and lon < -10:
        return False, "Coordinates appear to be in the ocean (far west of UK)"
    
    if lat < 50 and lon > 0:
        return False, "Coordinates appear to be in the ocean (far east of UK)"
    
    return True, ""


def check_event_exists_in_db(event, db_config=None):
    """Check if an event already exists in the WordPress database.
    
    This function can be used before geocoding to avoid processing duplicate events.
    Checks by URL first (most reliable), then by name + date combination.
    Only checks published posts (excludes trashed posts).
    
    Args:
        event (dict): Event dictionary with 'url', 'name', and 'date' keys
        db_config (dict, optional): Database configuration. If None, imports from db_connection.
        
    Returns:
        int or None: Post ID if event exists, None otherwise
    """
    try:
        if db_config is None:
            # Try to get settings from Scrapy if available
            settings = None
            try:
                from scrapy.utils.project import get_project_settings
                settings = get_project_settings()
            except Exception:
                pass
            
            # Import here to avoid circular dependencies
            import sys
            from pathlib import Path
            # Add parent directory to path to import db_connection
            parent_dir = Path(__file__).parent.parent.parent
            if str(parent_dir) not in sys.path:
                sys.path.insert(0, str(parent_dir))
            from db_connection import get_connection
            connection = get_connection(settings)
        else:
            # Create a custom get_connection function using provided db_config
            import mysql.connector
            connection = mysql.connector.connect(**db_config)
        
        if not connection:
            return None
        
        cursor = connection.cursor()
        
        url = event.get('url', '')
        name = event.get('name', '')
        date_str = event.get('date', '')
        
        # Only check for 'publish' status (exclude trashed posts)
        # First, try to find by URL (stored in postmeta _event_url or in post_content)
        if url:
            # Check if URL is stored in postmeta (join with wp_posts to check status)
            url_check_sql = """
            SELECT pm.post_id FROM wp_postmeta pm
            JOIN wp_posts p ON pm.post_id = p.ID
            WHERE pm.meta_key = '_event_url' 
            AND pm.meta_value = %s
            AND p.post_type = 'oum-location'
            AND p.post_status = 'publish'
            LIMIT 1
            """
            cursor.execute(url_check_sql, (url,))
            result = cursor.fetchone()
            if result:
                cursor.close()
                connection.close()
                return result[0]
            
            # Also check in post_content (some events might have URL in content)
            content_check_sql = """
            SELECT ID FROM wp_posts 
            WHERE post_content LIKE %s 
            AND post_type = 'oum-location'
            AND post_status = 'publish'
            LIMIT 1
            """
            cursor.execute(content_check_sql, (f'%{url}%',))
            result = cursor.fetchone()
            if result:
                cursor.close()
                connection.close()
                return result[0]
        
        # If URL check fails, try name + date combination
        if name and date_str:
            try:
                # Parse date to match format in database
                post_date = datetime.strptime(date_str, '%m/%d/%Y')
                post_date_str = post_date.strftime('%Y-%m-%d')
            except:
                post_date_str = None
            
            if post_date_str:
                # Check by post_title and post_date (only published posts)
                name_date_sql = """
                SELECT ID FROM wp_posts 
                WHERE post_title = %s 
                AND DATE(post_date) = %s 
                AND post_type = 'oum-location'
                AND post_status = 'publish'
                LIMIT 1
                """
                cursor.execute(name_date_sql, (name, post_date_str))
                result = cursor.fetchone()
                if result:
                    cursor.close()
                    connection.close()
                    return result[0]
        
        cursor.close()
        connection.close()
        return None
        
    except Exception:
        # Silently fail - if database check fails, allow processing to continue
        try:
            if connection:
                connection.close()
        except:
            pass
        return None