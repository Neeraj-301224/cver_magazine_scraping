# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
from itemadapter import ItemAdapter


# Chars to remove: dash-like, braces, apostrophe/single quote (ASCII and Unicode right single quote)
CHARS_TO_REMOVE = ("-", "\u2013", "\u2014", "{", "}", "'", "\u2019")  # -, –, —, {, }, ', '

# Regex to extract address from JSON-like blob (e.g. \"address\":\"Rivington, Bolton BL6 7SA, UK\")
_RE_JSON_ADDRESS = re.compile(r'\\"address\\":\\"([^"]*)\\"')
_RE_JSON_NAME = re.compile(r'\\"name\\":\\"([^"]*)\\"')


def _clean_json_like_address(s):
    """If string looks like escaped JSON with \"address\":\"...\" or \"name\":\"...\", return the extracted value."""
    if not isinstance(s, str) or not s.strip():
        return s
    # Prefer \"address\":\"...\" value
    m = _RE_JSON_ADDRESS.search(s)
    if m:
        return m.group(1).strip()
    # Fallback to \"name\":\"...\" (e.g. venue name)
    m = _RE_JSON_NAME.search(s)
    if m:
        return m.group(1).strip()
    return s


def _sanitize_text_remove_chars(s):
    """Remove -, –, —, {, }, ', newlines, tabs from string. Returns cleaned string or original if not str."""
    if not isinstance(s, str):
        return s
    s = s.replace("\n", " ").replace("\t", " ")
    for c in CHARS_TO_REMOVE:
        s = s.replace(c, "")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


def _sanitize_date_dash_to_to(s):
    """In date strings, replace - / – / — with ' to '. Also remove {, }, ', newlines, tabs."""
    if not isinstance(s, str):
        return s
    s = s.replace("\n", " ").replace("\t", " ")
    s = s.replace("-", " to ").replace("\u2013", " to ").replace("\u2014", " to ")
    s = s.replace("{", "").replace("}", "").replace("'", "").replace("\u2019", "")
    while "  " in s:
        s = s.replace("  ", " ")
    while " to to " in s:
        s = s.replace(" to to ", " to ")
    return s.strip()


# Keys that are date-like: replace - with " to "; all other string fields: remove -, {, }
DATE_KEYS = ("date", "raw_date")


def _sanitize_dict(d):
    """Recursively sanitize every string value in a dict. Date-like keys: - → ' to '; others: remove -, {, }."""
    if not isinstance(d, dict):
        return
    for key in list(d.keys()):
        val = d[key]
        if val is None:
            continue
        if isinstance(val, str):
            if key == "address":
                val = _clean_json_like_address(val)
            if key in DATE_KEYS:
                d[key] = _sanitize_date_dash_to_to(val)
            else:
                d[key] = _sanitize_text_remove_chars(val)
        elif isinstance(val, dict):
            _sanitize_dict(val)
        elif isinstance(val, list):
            for i, elem in enumerate(val):
                if isinstance(elem, str):
                    val[i] = _sanitize_text_remove_chars(elem) if key not in DATE_KEYS else _sanitize_date_dash_to_to(elem)
                elif isinstance(elem, dict):
                    _sanitize_dict(elem)


class EventScrapingPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        # Sanitize entire item (all top-level and nested dicts); every string gets - removed (or ' to ' for date keys)
        d = dict(adapter)
        _sanitize_dict(d)
        for key in d:
            adapter[key] = d[key]
        return item
