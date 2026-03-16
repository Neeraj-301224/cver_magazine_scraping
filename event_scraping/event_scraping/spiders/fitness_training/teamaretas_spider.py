"""Spider for https://team-aretas.com/competitions — Hyrox and CrossFit events."""
import html
import json
import re
import time
import scrapy
from scrapy import Selector
from ..base_spider import BaseSpider
from ...items import EventScrapingItem
from ...utils.common import validate_uk_coordinates

# --- Text sanitization: normalize special chars so they never break JSON, DB, or UI ---
_RE_CONTROL = re.compile(r"[\x00-\x1f\x7f]+")
_RE_EMOJI = re.compile(
    r"[\u200b-\u200d\u2060\ufe0f\u203c\u2049\u20e3\u2122\u2139"
    r"\u2194-\u2199\u21a9-\u21aa\u231a-\u231b\u2328\u23cf\u23e9-\u23f3\u23f8-\u23fa"
    r"\u2300-\u23ff\u2600-\u26ff\u2700-\u27bf\u2934-\u2935\u2b05-\u2b07\u2b1b-\u2b1c\u2b50\u2b55\u3030\u303d\u3297\u3299"
    r"\U0001f300-\U0001f9ff]+",
    re.UNICODE,
)
_NORMALIZE_REPLACES = (
    ("\u201c", '"'), ("\u201d", '"'), ("\u2018", "'"), ("\u2019", "'"),
    ("\u2013", " "), ("\u2014", " "), ("\u2026", "..."),
    ("-", " "), ("{", " "), ("}", " "),
    ("&nbsp;", " "), ("\u00a0", " "),
)


def _strip_emoji_chars(s):
    """Remove chars in emoji/symbol code point ranges."""
    result = []
    for c in s:
        o = ord(c)
        if o <= 0x1F or o == 0x7F:
            continue
        if 0x2300 <= o <= 0x23FF or 0x2600 <= o <= 0x26FF or 0x2700 <= o <= 0x27BF:
            continue
        if 0x2934 <= o <= 0x2935 or 0x2B05 <= o <= 0x2B07 or o in (0x2B1B, 0x2B1C, 0x2B50, 0x2B55):
            continue
        if 0x3030 <= o <= 0x303D or o in (0x3297, 0x3299) or 0x1F300 <= o <= 0x1F9FF:
            continue
        if o in (0x200B, 0x200C, 0x200D, 0x2060, 0xFE0F):
            continue
        result.append(c)
    return "".join(result)


def _make_text_safe(s, for_html=False):
    """Make text safe for JSON/DB/UI. Normalizes unicode, strips control/emoji. If for_html=True, escapes for HTML."""
    if not isinstance(s, str):
        return s
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = _RE_CONTROL.sub("", s)
    s = _RE_EMOJI.sub("", s)
    s = _strip_emoji_chars(s)
    for old, new in _NORMALIZE_REPLACES:
        s = s.replace(old, new)
    try:
        s = html.unescape(s)
    except Exception:
        pass
    while "  " in s:
        s = s.replace("  ", " ")
    s = s.strip()
    if for_html and s:
        s = html.escape(s, quote=True)
    return s


def _sanitize_event_item(item):
    """Apply text sanitization to all text fields of an event item. Modifies item in place."""
    if not item:
        return
    for key in ("name", "short_description", "address"):
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            item[key] = _make_text_safe(val, for_html=False)
    raw = item.get("raw")
    if isinstance(raw, dict):
        for key in ("title", "address", "raw_date", "full_description"):
            val = raw.get(key)
            if isinstance(val, str) and val.strip():
                raw[key] = _make_text_safe(val, for_html=False)


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

    @staticmethod
    def _extract_competition_ids_from_json(obj):
        """Recursively extract numeric IDs from JSON that could be competition IDs (e.g. from __NEXT_DATA__)."""
        ids = []
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    # Common pattern: list of { "id": 3066, ... }
                    if "id" in item and isinstance(item["id"], int) and 1000 <= item["id"] <= 99999:
                        ids.append(item["id"])
                ids.extend(TeamAretasSpider._extract_competition_ids_from_json(item))
        elif isinstance(obj, dict):
            for v in obj.values():
                ids.extend(TeamAretasSpider._extract_competition_ids_from_json(v))
        return ids

    def parse_json_listing(self, response):
        """Handle JSON response for /competitions (when Accept: application/json returns a list)."""
        try:
            data = response.json()
        except (ValueError, TypeError):
            self.logger.warning("parse_json_listing: response is not JSON")
            return
        ids = self._extract_competition_ids_from_json(data)
        base = response.url.rstrip("/").rsplit("/", 1)[0]  # https://team-aretas.com/competitions
        for cid in ids:
            url = f"{base}/{cid}"
            if url in self.seen_urls:
                continue
            self.seen_urls.add(url)
            yield response.follow(url, self.parse_event, errback=self.handle_error)
        # Also support list of objects with "url" or "slug"
        if isinstance(data, list):
            items_list = data
        else:
            items_list = data.get("data") or data.get("competitions") or []
        for item in items_list:
            if not isinstance(item, dict):
                continue
            u = item.get("url") or item.get("link")
            if u and "/competitions/" in str(u) and "create" not in str(u):
                full = response.urljoin(u)
                if full not in self.seen_urls:
                    self.seen_urls.add(full)
                    yield response.follow(full, self.parse_event, errback=self.handle_error)

    def _get_competition_urls_selenium(self, listing_url):
        """Load the competitions listing with Selenium, scroll to load all data, return competition detail URLs."""
        import time
        urls = []
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
        except ImportError:
            self.logger.warning("Selenium not installed. pip install selenium webdriver-manager")
            return urls
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            try:
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
            except Exception:
                driver = webdriver.Chrome(options=options)
            try:
                self.logger.info("Loading competitions listing with Selenium: %s", listing_url)
                driver.get(listing_url)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(3)

                # Switch to "Upcoming" tab so we only scrape upcoming events
                upcoming_clicked = False
                for selector in [
                    (By.LINK_TEXT, "Upcoming"),
                    (By.PARTIAL_LINK_TEXT, "Upcoming"),
                    (By.XPATH, "//a[contains(translate(., 'UPCOMING', 'upcoming'), 'upcoming')]"),
                    (By.XPATH, "//button[contains(translate(., 'UPCOMING', 'upcoming'), 'upcoming')]"),
                    (By.XPATH, "//*[@role='tab' and contains(translate(., 'UPCOMING', 'upcoming'), 'upcoming')]"),
                    (By.XPATH, "//*[contains(@class,'tab') and contains(translate(., 'UPCOMING', 'upcoming'), 'upcoming')]"),
                ]:
                    try:
                        el = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(selector))
                        el.click()
                        upcoming_clicked = True
                        self.logger.info("Clicked Upcoming tab")
                        time.sleep(2)
                        break
                    except Exception:
                        continue
                if not upcoming_clicked:
                    self.logger.info("Upcoming tab not found, scraping default tab content")

                # Scroll repeatedly to trigger "load more" / infinite scroll until no new content
                max_scrolls = 30
                scroll_pause = 2
                no_new_count = 0
                max_no_new = 3
                last_count = 0

                for _ in range(max_scrolls):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(scroll_pause)
                    # Count competition links currently in DOM
                    links_el = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/competitions/"]')
                    current = 0
                    for link in links_el:
                        href = link.get_attribute("href")
                        if href and "/competitions/create" not in href and not href.rstrip("/").endswith("/competitions") and re.search(r"/competitions/(\d+)/?$", href):
                            current += 1
                    if current == last_count:
                        no_new_count += 1
                        if no_new_count >= max_no_new:
                            self.logger.info("No new links after %d scrolls, stopping", max_no_new)
                            break
                    else:
                        no_new_count = 0
                    last_count = current
                    # Optional: scroll by viewport height if page uses lazy load on scroll position
                    driver.execute_script("window.scrollBy(0, -400);")
                    time.sleep(0.5)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)

                # Collect all competition URLs
                links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/competitions/"]')
                for link in links:
                    href = link.get_attribute("href")
                    if not href or "/competitions/create" in href or href.rstrip("/").endswith("/competitions"):
                        continue
                    if re.search(r"/competitions/(\d+)/?$", href):
                        if href not in urls:
                            urls.append(href)
                self.logger.info("Selenium found %d competition links after scrolling", len(urls))
            finally:
                driver.quit()
        except Exception as e:
            self.logger.warning("Selenium failed: %s", e)
        return urls

    def _fetch_detail_page_html_selenium(self, url):
        """Load a competition detail page with Selenium and return its HTML (so date/location are in the DOM)."""
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
        except ImportError:
            return None
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            try:
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
            except Exception:
                driver = webdriver.Chrome(options=options)
            try:
                driver.get(url)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(3)
                return driver.page_source
            finally:
                driver.quit()
        except Exception as e:
            self.logger.warning("Selenium detail fetch failed for %s: %s", url, e)
        return None

    def parse(self, response):
        """Extract competition links from listing; follow each to detail page."""
        self.logger.info("Parsing competitions listing: %s", response.url)
        self.pages_visited.add(response.url)

        # 1) Try CSS: links like /competitions/3066 (works when server-rendered or static)
        hrefs = response.css('a[href*="/competitions/"]::attr(href)').getall()
        # 2) Fallback: site may be JS-rendered; competition IDs/URLs appear in script/JSON or anywhere in source
        if not hrefs and response.text:
            # 2a) Regex on raw HTML (e.g. in __NEXT_DATA__, data attributes, or script)
            for match in re.finditer(r'/competitions/(\d+)/?', response.text):
                path = match.group(0)
                hrefs.append(path)
            for match in re.finditer(r'["\']https?://[^"\']*?/competitions/(\d+)/?["\']', response.text):
                full = match.group(0).strip('"\'').strip()
                hrefs.append(full)
            # 2b) Embedded JSON (e.g. __NEXT_DATA__, or script type="application/json")
            for script in response.css('script[type="application/json"]::text, script#__NEXT_DATA__::text').getall() or []:
                try:
                    data = json.loads(script)
                    ids = self._extract_competition_ids_from_json(data)
                    for cid in ids:
                        hrefs.append(f"/competitions/{cid}")
                except (json.JSONDecodeError, TypeError):
                    pass
        # Normalize to full URLs and dedupe
        seen_full = set()
        for href in hrefs:
            if not href:
                continue
            full = response.urljoin(href) if not href.startswith("http") else href
            if full in self.seen_urls or full in seen_full:
                continue
            if "/competitions/create" in full or full.rstrip("/").endswith("/competitions"):
                continue
            match = re.search(r"/competitions/(\d+)/?$", full)
            if not match:
                match = re.search(r"/competitions/([^/]+)/?$", full)
            if not match:
                continue
            seen_full.add(full)
            self.seen_urls.add(full)
            yield response.follow(full, self.parse_event, errback=self.handle_error)

        # 3) If still no links, load page with Selenium (site is JS-rendered)
        if not seen_full:
            urls = self._get_competition_urls_selenium(response.url)
            for url in urls:
                if url in self.seen_urls:
                    continue
                self.seen_urls.add(url)
                yield response.follow(url, self.parse_event, errback=self.handle_error)
            return

        # Pagination: next page / "load more" if present
        next_links = response.css('a[rel="next"]::attr(href), a[aria-label*="next"]::attr(href), [class*="pagination"] a[href*="page"]::attr(href)').getall()
        for next_href in next_links:
            if next_href:
                next_url = response.urljoin(next_href)
                if next_url not in self.pages_visited:
                    yield response.follow(next_href, self.parse, errback=self.handle_error)
                    break

    def parse_event(self, response):
        """Extract event fields from a competition detail page. Yields all competitions (category from keywords when matched)."""
        self.logger.info("Parsing event: %s", response.url)

        # Title: primary from contentContainer XPath, then class-based, then page title
        title = response.xpath('//*[@id="contentContainer"]/div/div[3]/div[4]/div[2]/h1//text()').getall()
        if title:
            title = " ".join(t.strip() for t in title if t and t.strip()).strip()
        if not title:
            title = (
                response.css("h1.text-left.no-margin.hidesmall::text").get()
                or response.css(".h1.text-left.no-margin.hidesmall::text").get()
                or response.css("h1.text-left.no-margin.hidesmall *::text").get()
                or response.css("h1::text").get()
                or response.css("[class*='title']::text").get()
                or response.css("h1 *::text").get()
            )
        if not title:
            title = response.css("title::text").get() or ""
        title = (title or "").strip()
        # Remove "|" and everything after it from title
        if "|" in title:
            title = title.split("|", 1)[0].strip()

        if not title:
            self.logger.warning("Skipping event with no title: %s", response.url)
            return

        # Description: primary from contentContainer XPath (div[2]/div[10]/div), then class "description-content from_text_editor"
        desc_parts = response.xpath(
            '//*[@id="contentContainer"]/div/div[3]/div[4]/div[2]/div[10]/div//text()'
        ).getall()
        if not desc_parts:
            desc_parts = response.css(".description-content.from_text_editor *::text").getall()
        if not desc_parts:
            desc_parts = (
                response.css("[class*='description'] *::text").getall()
                or response.css(".content *::text, article *::text").getall()
                or response.css("p::text").getall()
            )
        short_description = None
        if desc_parts:
            joined = " ".join(p.strip() for p in desc_parts if p and p.strip())
            short_description = joined[:500].rsplit(" ", 1)[0] + "..." if len(joined) > 500 else joined

        # Date: primary from contentContainer XPath, then class "info-data st600", then fallbacks
        raw_date_parts = response.xpath('//*[@id="contentContainer"]/div/div[3]/div[4]/div[1]/div[5]/div[2]/div[2]//text()').getall()
        raw_date = " ".join(t.strip() for t in raw_date_parts if t and t.strip()).strip() if raw_date_parts else None
        if not raw_date:
            raw_date = response.xpath("/html/body/div/div[3]/div/div[3]/div[4]/div[1]/div[5]/div[2]/div[2]/text()").get()
        if not raw_date:
            raw_date = (
                response.css(".info-data.st600::text").get()
                or response.css("[class*='info-data'][class*='st600']::text").get()
            )
        if not raw_date:
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
            # Remove the word "competition" from raw_date (case-insensitive)
            raw_date = re.sub(r"\bcompetition\b", "", raw_date, flags=re.IGNORECASE)
            raw_date = " ".join(raw_date.split()).strip()
        date = self.convert_date_format(raw_date) if raw_date else None

        # Location: primary from contentContainer XPath (div[7] text), then class "info-data", then fallbacks
        address = None
        location_text = response.xpath(
            '//*[@id="contentContainer"]/div/div[3]/div[4]/div[1]/div[5]/div[7]//text()'
        ).getall()
        if not location_text:
            location_text = response.xpath(
                "/html/body/div/div[3]/div/div[3]/div[4]/div[1]/div[5]/div[7]//text()"
            ).getall()
        if location_text:
            address = " ".join(t.strip() for t in location_text if t and t.strip()).strip()
        if not address:
            location_text = response.xpath(
                "//*[contains(translate(., 'location', 'LOCATION'), 'location') or contains(translate(., 'venue', 'VENUE'), 'venue')]"
                "/following-sibling::*[contains(@class,'info-data')]//text()"
            ).getall()
            if location_text:
                address = " ".join(t.strip() for t in location_text if t and t.strip()).strip()
        if not address:
            info_data_els = response.css(".info-data:not(.st600)::text").getall()
            if info_data_els:
                address = " ".join(t.strip() for t in info_data_els if t and t.strip()).strip()
        if not address:
            address = self.extract_address(response)

        # Detail page is JS-rendered: Scrapy often gets empty HTML. If date, location, title or description missing, load with Selenium.
        if (not raw_date or not address or not title.strip() or not desc_parts) and re.search(r"/competitions/\d+", response.url):
            html = self._fetch_detail_page_html_selenium(response.url)
            if html:
                sel = Selector(text=html)
                if not title.strip():
                    title_parts = sel.xpath('//*[@id="contentContainer"]/div/div[3]/div[4]/div[2]/h1//text()').getall()
                    if title_parts:
                        title = " ".join(t.strip() for t in title_parts if t and t.strip()).strip()
                        if "|" in title:
                            title = title.split("|", 1)[0].strip()
                if not desc_parts:
                    desc_parts = sel.xpath(
                        '//*[@id="contentContainer"]/div/div[3]/div[4]/div[2]/div[10]/div//text()'
                    ).getall()
                    if desc_parts:
                        joined = " ".join(p.strip() for p in desc_parts if p and p.strip())
                        short_description = joined[:500].rsplit(" ", 1)[0] + "..." if len(joined) > 500 else joined
                if not raw_date:
                    raw_date_parts = sel.xpath('//*[@id="contentContainer"]/div/div[3]/div[4]/div[1]/div[5]/div[2]/div[2]//text()').getall()
                    raw_date = " ".join(t.strip() for t in raw_date_parts if t and t.strip()).strip() if raw_date_parts else None
                    if not raw_date:
                        raw_date = sel.css(".info-data.st600::text").get()
                    if raw_date:
                        raw_date = raw_date.strip()
                        raw_date = re.sub(r"\bcompetition\b", "", raw_date, flags=re.IGNORECASE)
                        raw_date = " ".join(raw_date.split()).strip()
                        date = self.convert_date_format(raw_date)
                if not address:
                    location_text = sel.xpath(
                        '//*[@id="contentContainer"]/div/div[3]/div[4]/div[1]/div[5]/div[7]//text()'
                    ).getall()
                    if location_text:
                        address = " ".join(t.strip() for t in location_text if t and t.strip()).strip()
                    if not address:
                        info_data_els = sel.css(".info-data:not(.st600)::text").getall()
                        if info_data_els:
                            address = " ".join(t.strip() for t in info_data_els if t and t.strip()).strip()

        if address:
            address = self.remove_location_text(address)
        coords = self.extract_coordinates(response)
        event_data = {"name": title, "date": date, "url": response.url}
        if address:
            geocoded = self.geocode_address(address, event_data=event_data)
            if geocoded and not coords:
                coords = geocoded

        # Team Aretas: if event has "Hyrox" → Hyrox category, else → Crossfit category.
        event_text = f"{(title or '')} {(short_description or '')}".lower()
        if "hyrox" in event_text:
            event_category = "Hyrox"
            event_subcategory = "Hyrox"
            event_categories_list = [("Hyrox", "Hyrox")]
        else:
            event_category = "Crossfit"
            event_subcategory = "Crossfit"
            event_categories_list = [("Crossfit", "Crossfit")]

        item_key = f"{title}_{date}_{response.url}"
        if item_key in self.seen_events:
            return
        self.seen_events.add(item_key)

        if not self.should_process_event(title, date, response.url):
            return

        # Data validation: only yield events with valid UK coordinates
        is_valid, reason = validate_uk_coordinates(coords)
        if not is_valid:
            self.logger.warning("Skipping event with invalid/missing UK coordinates: %s - %s", title[:50], reason)
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
        item["categories"] = event_categories_list
        item["site"] = self.site_name
        item["raw"] = {
            "title": title,
            "raw_date": raw_date,
            "address": address,
            "coordinates": coords,
            "full_description": short_description or "",
        }
        _sanitize_event_item(item)
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
