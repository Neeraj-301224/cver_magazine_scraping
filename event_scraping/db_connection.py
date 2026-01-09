"""
Database connection module for WordPress MySQL database.
Reads configuration from Scrapy settings.
"""
import mysql.connector
from mysql.connector import Error


def get_connection(settings=None):
    """
    Create and return a MySQL database connection.
    
    Args:
        settings: Scrapy settings object. If None, tries to get from scrapy.utils.project
        
    Returns:
        mysql.connector.connection.MySQLConnection or None
    """
    # Try to get settings if not provided
    if settings is None:
        try:
            from scrapy.utils.project import get_project_settings
            settings = get_project_settings()
        except Exception:
            # Fallback to default values if settings not available
            # Production connection (currently active)
            #settings = {       
            #    'DB_HOST': 'sql7.nur4.host-h.net',
            #    'DB_NAME': 'cveropfnwf_wp7fbf',
            #    'DB_USER': 'cveropfnwf_998',
            #    'DB_PASSWORD': '95RnJXcDDkmV16s7VUav',
            #    'DB_PORT': 3306
            #}
            # Local connection (commented out for reference)
             settings = {
                 'DB_HOST': 'localhost',
                 'DB_NAME': 'local',
                 'DB_USER': 'root',
                 'DB_PASSWORD': 'root',
                 'DB_PORT': 10017
             }
    
    # Build DB config from settings
    # Production connection defaults (currently active)
    #db_config = {
    #    'host': settings.get('DB_HOST', 'sql7.nur4.host-h.net'),
    #    'database': settings.get('DB_NAME', 'cveropfnwf_wp7fbf'),
    #    'user': settings.get('DB_USER', 'cveropfnwf_998'),
    #    'password': settings.get('DB_PASSWORD', '95RnJXcDDkmV16s7VUav'),
    #    'port': settings.get('DB_PORT', 3306)
    #}
    # Local connection defaults (commented out for reference)
    db_config = {
         'host': settings.get('DB_HOST', 'localhost'),
         'database': settings.get('DB_NAME', 'local'),
         'user': settings.get('DB_USER', 'root'),
         'password': settings.get('DB_PASSWORD', 'root'),
         'port': settings.get('DB_PORT', 10017)
     }
    
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to WordPress database")
            return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None


def test_connection(settings=None):
    """Test the database connection.
    
    Args:
        settings: Scrapy settings object. If None, will try to get from scrapy.utils.project
    """
    connection = get_connection(settings)
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            db_name = cursor.fetchone()
            print(f"Connected to database: {db_name[0]}")
            
            # Show WordPress tables
            cursor.execute("SHOW TABLES LIKE 'wp_%';")
            tables = cursor.fetchall()
            print(f"Found {len(tables)} WordPress tables")
            
            cursor.close()
            connection.close()
            print("Connection test successful!")
            return True
        except Error as e:
            print(f"Error during test: {e}")
            return False
    return False


if __name__ == "__main__":
    # Try to get settings for testing
    try:
        from scrapy.utils.project import get_project_settings
        settings = get_project_settings()
        test_connection(settings)
    except Exception:
        # If settings not available, test with defaults
        test_connection()

