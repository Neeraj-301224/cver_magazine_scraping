"""
Insert event data into WordPress database (wp_posts and wp_postmeta).
Processes all JSON files from scraped_data folder and checks for duplicates.
"""
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from db_connection import get_connection

# Add event_scraping to path to import validation function and settings
# insert_event.py is in event_scraping/, and we need to import from event_scraping/event_scraping/utils/common.py
event_scraping_parent = Path(__file__).parent.parent
if str(event_scraping_parent) not in sys.path:
    sys.path.insert(0, str(event_scraping_parent))
from event_scraping.utils.common import validate_uk_coordinates

# Try to load settings for database configuration
def get_db_settings():
    """Get database settings from Scrapy settings file."""
    try:
        from scrapy.utils.project import get_project_settings
        return get_project_settings()
    except Exception:
        # If Scrapy settings not available, return None (db_connection will use defaults)
        return None


def event_exists(event):
    """Check if an event already exists in the database.
    
    Checks by URL first (most reliable), then by name + date combination.
    Only checks published posts (excludes trashed posts).
    
    Args:
        event (dict): Event dictionary with 'url', 'name', and 'date' keys
        
    Returns:
        int or None: Post ID if event exists, None otherwise
    """
    settings = get_db_settings()
    connection = get_connection(settings)
    if not connection:
        return None
    
    try:
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
            AND p.post_type = 'oum-location-dev'
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
            AND post_type = 'oum-location-dev'
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
                AND post_type = 'oum-location-dev'
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
        
    except Exception as e:
        print(f"Error checking if event exists: {e}")
        if connection:
            connection.close()
        return None


def get_term_id_by_name(cursor, category_name, subcategory_name=None):
    """Get term_id from wp_terms table based on category or subcategory name.
    
    Tries subcategory first, then falls back to category name.
    Compares against a list of valid categories using LIKE operator, then queries database.
    
    Args:
        cursor: Database cursor
        category_name (str): Category name (e.g., "Running Events", "Yoga and Pilates")
        subcategory_name (str, optional): Subcategory name
        
    Returns:
        int or None: term_id if found, None otherwise
    """
    if not cursor:
        return None
    
    # List of valid category names from wp_terms
    valid_categories = [
        'Charity Events',
        'Crossfit',
        'Endurance events',
        'Family Fitness',
        'Local Club Events',
        'Mindfulness',
        'Running Events',
        'Strength and Endurance',
        "Women's Fitness",
        'Yoga and Pilates',
        'Cycling',
        'Swimming'
    ]
    
    # Build list of names to search (subcategory first, then category)
    search_names = []
    if subcategory_name and subcategory_name.strip():
        search_names.append(subcategory_name.strip())
    if category_name and category_name.strip():
        search_names.append(category_name.strip())
    
    # Query to get term_id from database
    query_exact = """
    SELECT term_id FROM wp_terms 
    WHERE LOWER(name) = LOWER(%s)
    LIMIT 1
    """
    
    # For each search name, compare with valid categories using LIKE operator
    for name in search_names:
        if not name:
            continue
        
        name_lower = name.lower().strip()
        
        # Compare with each valid category using LIKE operator
        for valid_cat in valid_categories:
            valid_cat_lower = valid_cat.lower()
            
            # Use LIKE operator to check if name matches valid category
            # Check if name is contained in valid category or vice versa
            if name_lower in valid_cat_lower or valid_cat_lower in name_lower:
                # Found a match in the list, query database with the matched valid category name
                cursor.execute(query_exact, (valid_cat,))
                result = cursor.fetchone()
                if result:
                    return result[0]
    
    return None


def serialize_location_meta(event):
    """Create serialized PHP array for location meta.
    
    Note: This function should only be called with events that have valid coordinates.
    Invalid coordinates should be filtered out before calling insert_event().
    """
    # Get location data from event
    address = event.get('name', '')
    coords = event.get('coordinates', {})
    
    # Coordinates should already be validated before this function is called
    # But we'll still extract them safely
    lat = coords.get('lat', 0)
    lng = coords.get('lon', 0)
    
    zoom = 16
    
    # Build text with <br> tags for line breaks using event data
    name = event.get('name', '')
    raw_date = event.get('raw_date', '')
    description = event.get('short_description', '')
    url = event.get('url', '')
    
    # Build text parts
    text_parts = []
    if name and raw_date:
        text_parts.append(f"{name} {raw_date}")
    elif name:
        text_parts.append(name)
    
    if description:
        text_parts.append(description)
    
    if url:
        text_parts.append(f'<a href="{url}">Find out more</a>')
    
    # Join with <br><br> and ensure no actual newline characters
    text = '<br><br>'.join(text_parts)
    text = text.replace('\n', '<br>').replace('\r', '')
    
    # PHP serialized array format - all on one line, newlines in text converted to <br> tags
    serialized = f'a:8:{{s:7:"address";s:{len(address)}:"{address}";s:3:"lat";d:{lat};s:3:"lng";d:{lng};s:4:"zoom";i:{zoom};s:4:"text";s:{len(text)}:"{text}";s:11:"author_name";s:0:"";s:12:"author_email";s:0:"";s:5:"video";s:0:"";}}'
    return serialized


def insert_event(event):
    """Insert a single event into WordPress."""
    settings = get_db_settings()
    connection = get_connection(settings)
    if not connection:
        return None
    
    try:
        cursor = connection.cursor()
        
        # Prepare post data
        name = event.get('name', '')
        description = event.get('short_description', '')
        full_description = event.get('raw', {}).get('full_description', description)
        url = event.get('url', '')
        
        # Parse date
        date_str = event.get('date', '')
        try:
            post_date = datetime.strptime(date_str, '%m/%d/%Y')
        except:
            post_date = datetime.now()
        
        post_date_str = post_date.strftime('%Y-%m-%d %H:%M:%S')
        post_date_gmt = post_date_str
        
        # Create slug from name
        slug = name.lower().replace(' ', '-').replace(':', '').replace('&', 'and')[:200]
        
        # Insert into wp_posts
        post_sql = """
        INSERT INTO wp_posts (
            post_author, post_date, post_date_gmt, post_content, post_title,
            post_excerpt, post_status, comment_status, ping_status, post_password,
            post_name, to_ping, pinged, post_modified, post_modified_gmt,
            post_content_filtered, post_parent, guid, menu_order, post_type, 
            post_mime_type, comment_count
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        """
        
        post_values = (
            1,  # post_author
            post_date_str,  # post_date
            post_date_gmt,  # post_date_gmt
            full_description,  # post_content
            name,  # post_title
            description,  # post_excerpt
            'publish',  # post_status
            'closed',  # comment_status
            'closed',  # ping_status
            '',  # post_password
            slug,  # post_name
            '',  # to_ping
            '',  # pinged
            post_date_str,  # post_modified
            post_date_gmt,  # post_modified_gmt
            '',  # post_content_filtered
            0,  # post_parent
            '',  # guid (will update after)
            0,  # menu_order
            'oum-location-dev',  # post_type - change if needed
            '',  # post_mime_type
            0  # comment_count
        )
        
        cursor.execute(post_sql, post_values)
        post_id = cursor.lastrowid
        print(f"Inserted post with ID: {post_id}")
        
        # Update GUID
        guid = f"http://localhost/?p={post_id}"
        cursor.execute("UPDATE wp_posts SET guid = %s WHERE ID = %s", (guid, post_id))
        
        # Insert postmeta
        meta_entries = [
            ('_oum_location_key', serialize_location_meta(event)),
            ('_oum_location_image', ''),
            ('_oum_location_audio', ''),
            ('_edit_last', 1),
            ('_event_category', event.get('category', '')),
            ('_event_subcategory', event.get('subcategory', '')),
            ('_event_url', url)  # Store URL for duplicate checking
        ]
        
        meta_sql = "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) VALUES (%s, %s, %s)"
        
        for meta_key, meta_value in meta_entries:
            cursor.execute(meta_sql, (post_id, meta_key, meta_value))
        
        print(f"Inserted {len(meta_entries)} meta entries for post {post_id}")
        
        # Insert category relationship in wp_term_relationships
        # Get category_term_id from wp_terms based on category/subcategory name
        category = event.get('category', '')
        subcategory = event.get('subcategory', '')
        
        category_term_id = get_term_id_by_name(cursor, category, subcategory)
        
        if category_term_id:
            # Get term_taxonomy_id from wp_term_taxonomy
            taxonomy_query = "SELECT term_taxonomy_id FROM wp_term_taxonomy WHERE term_id = %s"
            cursor.execute(taxonomy_query, (category_term_id,))
            taxonomy_result = cursor.fetchone()
            
            if taxonomy_result:
                term_taxonomy_id = taxonomy_result[0]
                
                # Insert into wp_term_relationships
                relationship_sql = """
                INSERT INTO wp_term_relationships (object_id, term_taxonomy_id, term_order)
                VALUES (%s, %s, %s)
                """
                cursor.execute(relationship_sql, (post_id, term_taxonomy_id, 0))
                print(f"Inserted category relationship: post_id={post_id}, term_taxonomy_id={term_taxonomy_id} (term_id={category_term_id}, category={category}, subcategory={subcategory})")
            else:
                print(f"Warning: Could not find term_taxonomy_id for term_id {category_term_id} (category={category}, subcategory={subcategory})")
        else:
            print(f"Warning: Could not find term_id for category='{category}', subcategory='{subcategory}'. Event inserted without category relationship.")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return post_id
        
    except Exception as e:
        print(f"Error inserting event: {e}")
        connection.rollback()
        connection.close()
        return None


def cleanup_old_backups(backup_folder, days_to_keep=None):
    """Remove backup files older than specified number of days.
    
    Args:
        backup_folder (Path): Path to backup folder
        days_to_keep (int, optional): Number of days to keep backups. 
            If None, reads from settings file (default: 7)
    
    Returns:
        int: Number of files deleted
    """
    # Get days_to_keep from settings if not provided
    if days_to_keep is None:
        try:
            from scrapy.utils.project import get_project_settings
            settings = get_project_settings()
            days_to_keep = settings.get('BACKUP_RETENTION_DAYS', 7)
        except Exception:
            days_to_keep = 7  # Default fallback
    if not backup_folder.exists():
        return 0
    
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    deleted_count = 0
    total_size_freed = 0
    
    try:
        for file_path in backup_folder.glob('*.json'):
            # Get file modification time
            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            
            # Delete if older than cutoff date
            if file_mtime < cutoff_date:
                file_size = file_path.stat().st_size
                try:
                    file_path.unlink()
                    deleted_count += 1
                    total_size_freed += file_size
                except Exception as e:
                    print(f"  ⚠️  Warning: Could not delete old backup file {file_path.name}: {e}")
        
        if deleted_count > 0:
            size_mb = total_size_freed / (1024 * 1024)
            print(f"  🗑️  Cleaned up {deleted_count} old backup file(s) (freed {size_mb:.2f} MB)")
        
    except Exception as e:
        print(f"  ⚠️  Warning: Error during backup cleanup: {e}")
    
    return deleted_count


def main(json_folder=None):
    """Process all JSON files from scraped_data folder and insert new events.
    
    Args:
        json_folder (str, optional): Path to folder containing JSON files.
            If None, uses default 'scraped_data' folder in event_scraping directory.
    """
    # Get the script directory and set default folder
    script_dir = Path(__file__).parent
    if json_folder is None:
        json_folder = script_dir / 'scraped_data'
    else:
        json_folder = Path(json_folder)
    
    if not json_folder.exists():
        print(f"Error: Folder '{json_folder}' does not exist!")
        print(f"Creating folder '{json_folder}'...")
        json_folder.mkdir(parents=True, exist_ok=True)
        print(f"Folder created. Please ensure JSON files exist in this folder.")
        return
    
    # Create backup folder for processed files
    backup_folder = script_dir / 'scraped_data_backup'
    backup_folder.mkdir(exist_ok=True)
    
    # Clean up old backup files (reads retention days from settings)
    print("=" * 80)
    print("🧹 CLEANING UP OLD BACKUP FILES")
    print("=" * 80)
    try:
        from scrapy.utils.project import get_project_settings
        settings = get_project_settings()
        retention_days = settings.get('BACKUP_RETENTION_DAYS', 7)
        print(f"Retention period: {retention_days} days (from settings)")
    except Exception:
        retention_days = 7
        print(f"Retention period: {retention_days} days (default)")
    cleanup_old_backups(backup_folder, days_to_keep=retention_days)
    print("=" * 80)
    
    # Process all JSON files in the folder
    json_files = list(json_folder.glob('*.json'))
    
    if not json_files:
        print(f"Error: No JSON files found in '{json_folder}'")
        print("Please ensure JSON files exist in the scraped_data folder.")
        return
    
    print("=" * 80)
    print(f"Processing {len(json_files)} JSON file(s)")
    print("=" * 80)
    
    total_events = 0
    total_successful = 0
    total_failed = 0
    total_duplicates = 0
    total_invalid_coords = 0
    for json_file in json_files:
        print(f"\n📄 Processing file: {json_file.name}")
        print("-" * 80)
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                events = json.load(f)
            
            if not events:
                print(f"  ⚠️  No events found in {json_file.name}")
                continue
            
            if not isinstance(events, list):
                # If it's a single event object, wrap it in a list
                events = [events]
            
            num_events = len(events)
            total_events += num_events
            print(f"  Found {num_events} event(s) in {json_file.name}")
            
            file_successful = 0
            file_failed = 0
            file_duplicates = 0
            file_invalid_coords = 0
            
            for i, event in enumerate(events, 1):
                event_name = event.get('name', 'Unknown')[:50]
                event_url = event.get('url', 'N/A')[:50]
                
                # Check if event already exists
                existing_post_id = event_exists(event)
                if existing_post_id:
                    print(f"  [{i}/{num_events}] ⏭️  Skipping duplicate: {event_name} (exists as post ID: {existing_post_id})")
                    file_duplicates += 1
                    total_duplicates += 1
                    continue
                
                # Validate coordinates before insertion - skip if invalid or missing
                coords = event.get('coordinates', {})
                is_valid, reason = validate_uk_coordinates(coords)
                if not is_valid:
                    print(f"  [{i}/{num_events}] ⏭️  Skipping event with invalid/missing coordinates: {event_name} - {reason}")
                    file_invalid_coords += 1
                    total_invalid_coords += 1
                    continue
                
                # Insert new event
                print(f"  [{i}/{num_events}] ➕ Inserting: {event_name}")
                post_id = insert_event(event)
                
                if post_id:
                    print(f"      ✅ Successfully inserted (post ID: {post_id})")
                    file_successful += 1
                    total_successful += 1
                else:
                    print(f"      ❌ Failed to insert")
                    file_failed += 1
                    total_failed += 1
            
            print(f"\n  📊 File Summary for {json_file.name}:")
            print(f"     ✅ Successful: {file_successful}")
            print(f"     ⏭️  Duplicates: {file_duplicates}")
            print(f"     ⚠️  Invalid coordinates: {file_invalid_coords}")
            print(f"     ❌ Failed: {file_failed}")
            
            # Move processed file to backup folder
            try:
                backup_path = backup_folder / json_file.name
                # If file already exists in backup, add timestamp to avoid overwriting
                if backup_path.exists():
                    from datetime import datetime
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_path = backup_folder / f"{json_file.stem}_{timestamp}{json_file.suffix}"
                
                json_file.rename(backup_path)
                print(f"  📦 Moved processed file to backup: {backup_path.name}")
            except Exception as e:
                print(f"  ⚠️  Warning: Could not move file to backup: {e}")
            
        except json.JSONDecodeError as e:
            print(f"  ❌ Error: Invalid JSON in {json_file.name}: {e}")
            total_failed += 1
            # Move invalid JSON file to backup as well
            try:
                backup_path = backup_folder / f"{json_file.stem}_invalid{json_file.suffix}"
                json_file.rename(backup_path)
                print(f"  📦 Moved invalid file to backup: {backup_path.name}")
            except Exception:
                pass
        except Exception as e:
            print(f"  ❌ Error processing {json_file.name}: {e}")
            total_failed += 1
            # Try to move file to backup even on error
            try:
                backup_path = backup_folder / f"{json_file.stem}_error{json_file.suffix}"
                json_file.rename(backup_path)
                print(f"  📦 Moved error file to backup: {backup_path.name}")
            except Exception:
                pass
    
    # Final summary
    print("\n" + "=" * 80)
    print("📊 FINAL SUMMARY")
    print("=" * 80)
    print(f"JSON files processed: {len(json_files)}")
    print(f"Total events found: {total_events}")
    print(f"✅ Successfully inserted: {total_successful}")
    print(f"⏭️  Duplicates skipped: {total_duplicates}")
    print(f"⚠️  Invalid coordinates skipped: {total_invalid_coords}")
    print(f"❌ Failed: {total_failed}")
    print("=" * 80)


if __name__ == "__main__":
    main()

