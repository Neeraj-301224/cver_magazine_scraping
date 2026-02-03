"""Spider for https://team-aretas.com/competitions — Hyrox and CrossFit events."""
import re
import scrapy
from ..base_spider import BaseSpider
from ...items import EventScrapingItem


class TeamAretasSpider(BaseSpider):
    """Spider for Team Aretas competitions (Hyrox and CrossFit events).

    Targets https://team-aretas.com/competitions and competition detail pages.
    Only yields events matching Hyrox or CrossFit / functional fitness.
    """
    name = "teamaretas"
    category = "fitness_training"
    site_name = "teamaretas"
    allowed_domains = ["team-aretas.com"]
    start_urls = ["https://team-aretas.com/competitions"]

    # Hyrox and CrossFit only (matching this site's focus)
    CATEGORY_KEYWORDS = {
        "Functional Fitness": {
            "CrossFit Competitions": [
                "crossfit", "cross fit", "crossfit competition", "crossfit games",
                "throwdown", "wodfest", "wod fest", "teams of four", "team comp",
            ],
            "Hyrox / DEKA FIT": [
                "hyrox", "deka fit", "deka", "hyrox race", "deka race",
                "hybrid", "hybrid race", "crossrox",
            ],
        }
    }

    ALL_KEYWORDS = []
    for _cat, subcats in CATEGORY_KEYWORDS.items():
        for _sub, kws in subcats.items():
            ALL_KEYWORDS.extend(kws)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_events = set()
        self.seen_urls = set()
        self.geocoding_cache = {}
        self.pages_visited = set()

    def parse(self, response):
        """Extract competition links from listing; follow each to detail page."""
        self.logger.info("Parsing competitions listing: %s", response.url)
        self.pages_visited.add(response.url)

        # Links like /competitions/3066, /competitions/2913 (exclude /competitions/create)
        hrefs = response.css('a[href*="/competitions/"]::attr(href)').getall()
        for href in hrefs:
            if not href:
                continue
            full = response.urljoin(href)
            if full in self.seen_urls:
                continue
            if "/competitions/create" in full or full.rstrip("/").endswith("/competitions"):
                continue
            match = re.search(r"/competitions/(\d+)/?$", full)
            if not match:
                continue
            self.seen_urls.add(full)
            yield response.follow(href, self.parse_event, errback=self.handle_error)

        # Pagination: next page / "load more" if present
        next_links = response.css('a[rel="next"]::attr(href), a[aria-label*="next"]::attr(href), [class*="pagination"] a[href*="page"]::attr(href)').getall()
        for next_href in next_links:
            if next_href:
                next_url = response.urljoin(next_href)
                if next_url not in self.pages_visited:
                    yield response.follow(next_href, self.parse, errback=self.handle_error)
                    break

    def parse_event(self, response):
        """Extract event fields from a competition detail page. Only yield if Hyrox/CrossFit."""
        self.logger.info("Parsing event: %s", response.url)

        title = (
            response.css("h1::text").get()
            or response.css("[class*='title']::text").get()
            or response.css("h1 *::text").get()
        )
        if not title:
            title = response.css("title::text").get() or ""
        title = (title or "").strip()

        # Must match Hyrox or CrossFit
        if not self._is_hyrox_or_crossfit(title, response):
            self.logger.info("Skipping non-Hyrox/CrossFit event: %s", title[:60])
            return

        desc_parts = (
            response.css("[class*='description'] *::text").getall()
            or response.css(".content *::text, article *::text").getall()
            or response.css("p::text").getall()
        )
        short_description = None
        if desc_parts:
            joined = " ".join(p.strip() for p in desc_parts if p and p.strip())
            short_description = joined[:500].rsplit(" ", 1)[0] + "..." if len(joined) > 500 else joined

        # Date: try time/datetime, then [class*="date"], then regex in text
        raw_date = None
        date_node = response.css("time::attr(datetime)").get()
        if date_node:
            raw_date = date_node.split("T")[0] if "T" in date_node else date_node
        if not raw_date:
            raw_date = (
                response.css("[class*='date']::text").get()
                or response.css("[class*='event-date']::text").get()
            )
        if not raw_date and desc_parts:
            text = " ".join(desc_parts)
            for pat in [
                r"(\d{1,2})\.(\d{1,2})\.(\d{4})",
                r"(\d{1,2})/(\d{1,2})/(\d{4})",
                r"(\d{1,2})\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})",
            ]:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    g = m.groups()
                    if len(g) == 3 and "." in pat:
                        raw_date = f"{g[0]}/{g[1]}/{g[2]}"
                    elif len(g) == 3:
                        raw_date = f"{g[0]}.{g[1]}.{g[2]}"
                    break

        if raw_date:
            raw_date = raw_date.strip()
        date = self.convert_date_format(raw_date) if raw_date else None

        address = self.extract_address(response)
        if address:
            address = self.remove_location_text(address)
        coords = self.extract_coordinates(response)
        event_data = {"name": title, "date": date, "url": response.url}
        if address:
            geocoded = self.geocode_address(address, event_data=event_data)
            if geocoded and not coords:
                coords = geocoded

        event_category, event_subcategory = self.get_event_category(title, desc_parts or [])
        if not event_category:
            event_category = "Functional Fitness"
        if not event_subcategory:
            event_subcategory = "CrossFit Competitions" if "hyrox" not in title.lower() and "hybrid" not in title.lower() else "Hyrox / DEKA FIT"

        item_key = f"{title}_{date}_{response.url}"
        if item_key in self.seen_events:
            return
        self.seen_events.add(item_key)

        if not self.should_process_event(title, date, response.url):
            return

        item = EventScrapingItem()
        item["name"] = self.clean_text(title)
        item["date"] = date
        item["raw_date"] = raw_date
        item["short_description"] = self.clean_text(short_description) if short_description else None
        item["url"] = response.url
        item["coordinates"] = coords
        item["address"] = address
        item["category"] = event_category
        item["subcategory"] = event_subcategory
        item["site"] = self.site_name
        item["raw"] = {
            "title": title,
            "raw_date": raw_date,
            "address": address,
            "coordinates": coords,
        }
        yield item

    def _is_hyrox_or_crossfit(self, title, response):
        """Return True if title or page content suggests Hyrox or CrossFit."""
        text = (title or "").lower()
        for kw in self.ALL_KEYWORDS:
            if kw.lower() in text:
                return True
        body = " ".join(response.css("*::text").getall() or []).lower()
        for kw in self.ALL_KEYWORDS:
            if kw.lower() in body:
                return True
        return False

    def convert_date_format(self, date_str):
        """Convert DD.MM.YYYY or similar to MM/DD/YYYY."""
        if not date_str:
            return None
        date_str = date_str.strip()
        # DD.MM.YYYY
        m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", date_str)
        if m:
            d, mo, y = m.groups()
            return f"{mo.zfill(2)}/{d.zfill(2)}/{y}"
        # YYYY-MM-DD
        m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", date_str)
        if m:
            y, mo, d = m.groups()
            return f"{mo.zfill(2)}/{d.zfill(2)}/{y}"
        return super().convert_date_format(date_str)
