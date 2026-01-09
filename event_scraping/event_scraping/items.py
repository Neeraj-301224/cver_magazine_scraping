# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EventScrapingItem(scrapy.Item):
    # Fields requested by the user
    name = scrapy.Field()  # Name of event
    date = scrapy.Field()  # Date of event (converted to MM/DD/YYYY format)
    raw_date = scrapy.Field()  # Original date as found in description (no conversion)
    short_description = scrapy.Field()  # Short description of event
    url = scrapy.Field()  # URL of event
    coordinates = scrapy.Field()  # Coordinates of starting point (dict: {'lat': float, 'lon': float})
    address = scrapy.Field()  # Full address of the event

    # Additional metadata
    category = scrapy.Field()  # Main category (e.g., "Running", "Cycling", "Swimming")
    subcategory = scrapy.Field()  # Subcategory (e.g., "Road running", "Trail running", "Triathlon")
    site = scrapy.Field()
    raw = scrapy.Field()  # raw dictionary of scraped values for debugging
