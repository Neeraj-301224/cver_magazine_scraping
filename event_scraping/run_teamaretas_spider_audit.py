 """Audit runner and spider for Team Aretas.

This spider scrapes ALL events without dropping any items.
It tracks why an event would or wouldn't be kept by the production spider,
saving everything to `scraped_data/teamaretas_all_unfiltered_<date>.json`.
"""
import json
import re
from datetime import datetime
from pathlib import Path

from scrapy import Selector
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from event_scraping.spiders.fitness_training.teamaretas_spider import (
    TeamAretasSpider,
    _description_string_value,
    _truncate_blurb,
)
from event_scraping.utils.common import geocode_address, validate_uk_coordinates


class TeamAretasAuditSpider(TeamAretasSpider):
    name = "teamaretas_audit"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.audit_summary = {
            "total_scraped": 0,
            "kept_in_production": 0,
            "dropped_in_production": 0,
            "reasons_breakdown": {},
        }

    def parse_event(self, response):
        """Extract event fields and record audit information WITHOUT dropping any events."""
        self.logger.info("Audit parsing event: %s", response.url)

        # 1. Title extraction
        title_parts = response.xpath(
            '//*[@id="contentContainer"]/div/div[3]/div[4]/div[2]/h1//text()'
        ).getall()
        title = " ".join(t.strip() for t in title_parts if t and t.strip()).strip() if title_parts else ""
        if not title:
            title = (
                response.css("h1.text-left.no-margin.hidesmall::text").get()
                or response.css(".h1.text-left.no-margin.hidesmall::text").get()
                or response.css("h1.text-left.no-margin.hidesmall *::text").get()
                or response.css("h1::text").get()
                or response.css("[class*='title']::text").get()
                or response.css("h1 *::text").get()
                or response.css("title::text").get()
                or ""
            ).strip()

        if "|" in title:
            title = title.split("|", 1)[0].strip()

        # 2. Description
        joined = _description_string_value(response)
        short_description = _truncate_blurb(joined) if joined else None

        # 3. Date
        raw_date_parts = response.xpath(
            '//*[@id="contentContainer"]/div/div[3]/div[4]/div[1]/div[5]/div[2]/div[2]//text()'
        ).getall()
        raw_date = " ".join(t.strip() for t in raw_date_parts if t and t.strip()).strip() if raw_date_parts else None
        if not raw_date:
            raw_date = response.xpath(
                "/html/body/div/div[3]/div/div[3]/div[4]/div[1]/div[5]/div[2]/div[2]/text()"
            ).get()
        if not raw_date:
            raw_date = (
                response.css(".info-data.st600::text").get()
                or response.css("[class*='info-data'][class*='st600']::text").get()
                or response.css("time::attr(datetime)").get()
                or response.css("[class*='date']::text").get()
            )

        # 4. Location / Address
        address = None
        loc_parts = response.xpath(
            '//*[@id="contentContainer"]/div/div[3]/div[4]/div[1]/div[5]/div[7]//text()'
        ).getall()
        if not loc_parts:
            loc_parts = response.xpath(
                "/html/body/div/div[3]/div/div[3]/div[4]/div[1]/div[5]/div[7]//text()"
            ).getall()
        if loc_parts:
            address = " ".join(t.strip() for t in loc_parts if t and t.strip()).strip()
        if not address:
            info_data_els = response.css(".info-data:not(.st600)::text").getall()
            if info_data_els:
                address = " ".join(t.strip() for t in info_data_els if t and t.strip()).strip()

        # 5. Selenium fallback for JS-rendered DOM if fields are missing
        used_selenium = False
        if (not raw_date or not address or not title.strip() or not short_description) and re.search(r"/competitions/\d+", response.url):
            html = self._fetch_detail_page_html_selenium(response.url)
            if html:
                used_selenium = True
                sel = Selector(text=html)
                if not title.strip():
                    t_parts = sel.xpath('//*[@id="contentContainer"]/div/div[3]/div[4]/div[2]/h1//text()').getall()
                    if t_parts:
                        title = " ".join(t.strip() for t in t_parts if t and t.strip()).strip()
                        if "|" in title:
                            title = title.split("|", 1)[0].strip()
                if not short_description:
                    joined = _description_string_value(sel)
                    if joined:
                        short_description = _truncate_blurb(joined)
                if not raw_date:
                    r_parts = sel.xpath('//*[@id="contentContainer"]/div/div[3]/div[4]/div[1]/div[5]/div[2]/div[2]//text()').getall()
                    raw_date = " ".join(t.strip() for t in r_parts if t and t.strip()).strip() if r_parts else None
                    if not raw_date:
                        raw_date = sel.css(".info-data.st600::text").get()
                if not address:
                    l_parts = sel.xpath('//*[@id="contentContainer"]/div/div[3]/div[4]/div[1]/div[5]/div[7]//text()').getall()
                    if l_parts:
                        address = " ".join(t.strip() for t in l_parts if t and t.strip()).strip()
                    if not address:
                        info_els = sel.css(".info-data:not(.st600)::text").getall()
                        if info_els:
                            address = " ".join(t.strip() for t in info_els if t and t.strip()).strip()

        # Clean date
        date = None
        if raw_date:
            raw_date = raw_date.strip()
            raw_date = re.sub(r"\bcompetition\b", "", raw_date, flags=re.IGNORECASE)
            raw_date = " ".join(raw_date.split()).strip()
            date = self.convert_date_format(raw_date)

        # Clean address
        cleaned_address = self.remove_location_text(address) if address else None

        # Look for UK postcode in address or page
        uk_postcode = None
        postcode_match = re.search(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b", address or "", re.IGNORECASE)
        if postcode_match:
            uk_postcode = postcode_match.group(0).upper()

        # Coordinates
        coords = self.extract_coordinates(response)
        event_data = {"name": title, "date": date, "url": response.url}
        if not coords and cleaned_address:
            coords = self.geocode_address(cleaned_address, event_data=event_data)

        # Coordinate fallback using UK postcode if full address failed
        postcode_geocoded = False
        if not coords and uk_postcode:
            try:
                coords = geocode_address(uk_postcode, user_agent=f"{self.__class__.__name__}/1.0")
                if coords:
                    postcode_geocoded = True
            except Exception:
                pass

        # Validate coordinates
        is_uk_valid, coord_reason = validate_uk_coordinates(coords)

        # Category
        event_text = f"{(title or '')} {(short_description or '')}".lower()
        if "hyrox" in event_text:
            event_category = "Hyrox"
            event_subcategory = "Hyrox"
        else:
            event_category = "Crossfit"
            event_subcategory = "Crossfit"

        # --- Removal Condition Checks (Simulating Production Filtering) ---
        removal_reasons = []

        if not title or not title.strip():
            removal_reasons.append("Skipped: Missing event title")

        item_key = f"{title}_{date}_{response.url}"
        if item_key in self.seen_events:
            removal_reasons.append("Skipped: Duplicate within crawl session")
        else:
            self.seen_events.add(item_key)

        is_duplicate_in_db = False
        try:
            if not self.should_process_event(title, date, response.url):
                is_duplicate_in_db = True
                removal_reasons.append("Skipped: Already exists in Database (duplicate)")
        except Exception as e:
            self.logger.debug("DB check error: %s", e)

        if not is_uk_valid:
            removal_reasons.append(f"Skipped: UK coordinates missing or invalid ({coord_reason})")

        kept_in_production = (len(removal_reasons) == 0)

        # Update audit tally
        self.audit_summary["total_scraped"] += 1
        if kept_in_production:
            self.audit_summary["kept_in_production"] += 1
        else:
            self.audit_summary["dropped_in_production"] += 1
            for r in removal_reasons:
                base_r = r.split(":")[0] + ":" + r.split(":")[1] if ":" in r else r
                self.audit_summary["reasons_breakdown"][base_r] = (
                    self.audit_summary["reasons_breakdown"].get(base_r, 0) + 1
                )

        # YIELD FULL UNFILTERED DATA RECORD WITH DIAGNOSTICS
        yield {
            "name": self.clean_text(title) if title else None,
            "date": date,
            "raw_date": raw_date,
            "address": address,
            "cleaned_address": cleaned_address,
            "extracted_uk_postcode": uk_postcode,
            "coordinates": coords,
            "category": event_category,
            "subcategory": event_subcategory,
            "url": response.url,
            "short_description": self.clean_text(short_description) if short_description else None,
            "site": self.site_name,
            "audit_diagnostics": {
                "kept_in_production": kept_in_production,
                "removal_reasons": removal_reasons if removal_reasons else ["None - Event kept"],
                "coordinates_valid": is_uk_valid,
                "coordinates_reason": coord_reason,
                "postcode_geocoded": postcode_geocoded,
                "is_db_duplicate": is_duplicate_in_db,
                "used_selenium": used_selenium,
            },
        }

    def closed(self, reason):
        print("\n" + "=" * 60)
        print("TEAMARETAS AUDIT CRAWL SUMMARY")
        print("=" * 60)
        print(f"Total events scraped:    {self.audit_summary['total_scraped']}")
        print(f"Would be kept in prod:   {self.audit_summary['kept_in_production']}")
        print(f"Would be dropped in prod:{self.audit_summary['dropped_in_production']}")
        print("\nBreakdown of removal conditions:")
        for r, cnt in self.audit_summary["reasons_breakdown"].items():
            print(f"  - {r}: {cnt} event(s)")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    scraped_data_dir = Path(__file__).parent / "scraped_data"
    scraped_data_dir.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_file = str(scraped_data_dir / f"teamaretas_all_unfiltered_{date_str}.json")

    print(f"Running TeamAretasAuditSpider")
    print(f"Output file: {output_file}")

    crawl_settings = {
        **get_project_settings(),
        "FEEDS": {
            output_file: {
                "format": "json",
                "encoding": "utf8",
                "overwrite": True,
                "indent": 2,
            }
        },
        "ITEM_PIPELINES": {},  # Bypass default pipeline so all audit fields remain unaltered
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
        "RANDOMIZE_DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "LOG_LEVEL": "INFO",
    }

    process = CrawlerProcess(crawl_settings)
    process.crawl(TeamAretasAuditSpider)
    process.start()

