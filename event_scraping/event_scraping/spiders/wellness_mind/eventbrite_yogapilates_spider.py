try:
    # Package import (works when run via scrapy / run_*.py scripts)
    from ..community_social.eventbrite_spider import EventbriteSpider
except ImportError:
    # Fallback for debugger "run file" execution
    from event_scraping.spiders.community_social.eventbrite_spider import EventbriteSpider


class EventbriteYogaPilatesSpider(EventbriteSpider):
    """Eventbrite spider configured for UK yoga and pilates listings.

    This reuses the existing Eventbrite parsing and JSON mapping logic, while
    targeting the wellness feed and forcing the category assignment to
    Wellness & Mind -> Yoga and Pilates.
    """

    name = "eventbrite_yogapilates"
    category = "wellness_mind"
    wellness_subcategories = ["Yoga and Pilates"]
    site_name = "eventbrite"
    custom_settings = {
        "CLOSESPIDER_ITEMCOUNT": 100,
    }
    start_urls = [
        "https://www.eventbrite.com/d/united-kingdom/yoga-pilates/",
    ]

    def parse_event(self, response):
        """Parse event page and apply class-based fallback for date/location."""
        fallback_values = response.xpath(
            "//*[contains(@class, 'Typography_root__487rx') "
            "and contains(@class, 'Typography_body-md__487rx') "
            "and contains(@class, 'event-card__clamp-line--one') "
            "and contains(@class, 'Typography_align-match-parent__487rx')]//text()"
        ).getall()
        fallback_values = [self.clean_text(v) for v in fallback_values if self.clean_text(v)]

        fallback_date = fallback_values[0] if len(fallback_values) > 0 else None
        fallback_location = fallback_values[1] if len(fallback_values) > 1 else None

        for item in super().parse_event(response):
            if fallback_date:
                if not item.get("raw_date"):
                    item["raw_date"] = fallback_date
                if not item.get("date"):
                    item["date"] = self.convert_date_format(fallback_date)

            if fallback_location and not item.get("address"):
                item["address"] = self.remove_location_text(fallback_location)

            raw_block = item.get("raw") or {}
            if fallback_date and not raw_block.get("date"):
                raw_block["date"] = fallback_date
            if fallback_location and not raw_block.get("address"):
                raw_block["address"] = item.get("address")
            item["raw"] = raw_block

            yield item
