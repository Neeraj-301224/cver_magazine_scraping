import re

from ...utils.common import get_all_matching_categories
from .timeoutdoors_spider import TimeOutdoorsSpider


class TimeOutdoorsCyclingSwimSpider(TimeOutdoorsSpider):
    """TimeOutdoors listings for cycling and swimming only.

    Cycling sources (per product request):
    - https://www.timeoutdoors.com/events/mountain-biking
    - https://www.timeoutdoors.com/charity/bike-rides
    - https://www.timeoutdoors.com/events/gravel-rides
    - https://www.timeoutdoors.com/events/sportives

    Swimming sources (confirmed listing pages on timeoutdoors.com):
    - https://www.timeoutdoors.com/events/open-water-swimming
    - https://www.timeoutdoors.com/events/sea-swims
    - https://www.timeoutdoors.com/events/lake-swims
    - https://www.timeoutdoors.com/events/river-swims
    - https://www.timeoutdoors.com/charity/swims
    - https://www.timeoutdoors.com/events/swimming
    """

    name = "timeoutdoors_cycling_swim"
    category = "fitness_training"
    site_name = "timeoutdoors"
    charity_listing_path_substrings = ("/charity/bike-rides", "/charity/swims")
    url_event_indicators = [
        "run",
        "race",
        "marathon",
        "10k",
        "5k",
        "half",
        "ultra",
        "trail",
        "triathlon",
        "duathlon",
        "swim",
        "bike",
        "cycle",
        "cycling",
        "gravel",
        "sportive",
        "sportives",
        "mtb",
        "sportif",
        "aquathlon",
        "open-water",
        "openwater",
        "pool",
        "channel",
        "swimrun",
        "paddle",
        "dinghy",
        "riding",
        "ride",
    ]

    CATEGORY_KEYWORDS = {
        "Cycling": {
            "Sportives": [
                "sportive",
                "sportif",
                "cycling sportive",
                "bike sportive",
                "gravel",
            ],
            "Time Trials": ["time trial", "cycling time trial", "bike time trial"],
            "Road Races": ["road race", "cycling race", "bike race", "road cycling"],
            "Cyclocross": ["cyclocross", "cx", "cyclo-cross"],
            "Mountain Biking": [
                "mountain bike",
                "mtb",
                "mountain biking",
                "off road cycling",
                "trail centre",
                "trail center",
            ],
            "Track Cycling": ["track cycling", "velodrome", "track race", "track bike"],
            "Charity & Challenge Rides": [
                "charity ride",
                "challenge ride",
                "charity cycling",
                "fundraising ride",
                "bike ride",
            ],
        },
        "Swimming": {
            "Open Water Swims": [
                "open water",
                "open water swim",
                "sea swim",
                "lake swim",
                "river swim",
                "skins swim",
                "skins swimming",
                "skins",
            ],
            "Pool Meets": ["pool meet", "pool swimming", "pool race", "swimming meet"],
            "Swim Runs": ["swim run", "swimrun", "swim-run", "aquathlon"],
            "Channel/Distance Swims": [
                "channel swim",
                "distance swim",
                "long distance swim",
                "marathon swim",
            ],
        },
    }

    ALL_KEYWORDS = []
    for _grp, subs in CATEGORY_KEYWORDS.items():
        for _words in subs.values():
            ALL_KEYWORDS.extend(_words)

    start_urls = [
        "https://www.timeoutdoors.com/events/mountain-biking",
        "https://www.timeoutdoors.com/charity/bike-rides",
        "https://www.timeoutdoors.com/events/gravel-rides",
        "https://www.timeoutdoors.com/events/sportives",
        "https://www.timeoutdoors.com/events/open-water-swimming",
        "https://www.timeoutdoors.com/events/sea-swims",
        "https://www.timeoutdoors.com/events/lake-swims",
        "https://www.timeoutdoors.com/events/river-swims",
        "https://www.timeoutdoors.com/charity/swims",
        "https://www.timeoutdoors.com/events/swimming",
    ]

    swim_rx = re.compile(
        r"\b(swimming|swimmers?|swimrun|swim-run|aquathlon|swim)\b|"
        r"\bopen\s+water\b|\bsea\s+swim\b|\bpool\s+swim\b|\briver\s+swim\b|\blake\s+swim\b",
        re.I,
    )
    cycle_rx = re.compile(
        r"\b(bikes?|biking|bicycles?|bicycle|cycling|cyclists?|sportives?|sportif|"
        r"velodrome|mtb|cyclocross|cyclo-?cross|gravel|sportive|rides?|riding)\b",
        re.I,
    )

    def get_event_category(self, title, description_parts):
        """Map to Cycling or Swimming only (same top-level labels as insert_event expects)."""
        if not title:
            return None, None
        matches = get_all_matching_categories(
            title, description_parts or [], self.CATEGORY_KEYWORDS
        )
        for prefer in ("Swimming", "Cycling"):
            for cat, sub in matches:
                if cat == prefer:
                    return cat, sub
        text = title + " "
        if description_parts:
            text += " ".join(str(p) for p in description_parts)
        tl = text.lower()
        if self.swim_rx.search(tl):
            return "Swimming", "Swimming"
        if self.cycle_rx.search(tl):
            return "Cycling", "Cycling"
        return None, None

    @staticmethod
    def _strip_event_overview_heading_text(text):
        """Remove section label 'Event overview' (any casing) from scraped copy."""
        if not text or not str(text).strip():
            return None
        t = str(text)
        t = re.sub(r"\bEvent\s+overview\b\.?:?\s*", " ", t, flags=re.IGNORECASE)
        t = re.sub(r"\s+", " ", t).strip()
        return t or None

    def parse_event(self, response):
        for item in super().parse_event(response):
            if item.get("category") not in ("Cycling", "Swimming"):
                self.logger.debug(
                    "Skipping event (not cycling/swimming category): %s",
                    item.get("name"),
                )
                continue
            stripped = self._strip_event_overview_heading_text(item.get("short_description"))
            item["short_description"] = self.clean_text(stripped) if stripped else None
            raw = item.get("raw") or {}
            for key in ("desc_preview", "full_description"):
                if raw.get(key):
                    s = self._strip_event_overview_heading_text(raw[key])
                    raw[key] = self.clean_text(s) if s else None
            item["raw"] = raw
            yield item
