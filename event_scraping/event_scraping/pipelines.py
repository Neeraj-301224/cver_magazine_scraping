# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import html
from itemadapter import ItemAdapter


# Chars to remove: dash-like, braces, apostrophe/single quote (ASCII and Unicode right single quote)
CHARS_TO_REMOVE = ("-", "\u2013", "\u2014", "{", "}", "'", "\u2019")  # -, –, —, {, }, ', '

# Regex to extract address from JSON-like blob (e.g. \"address\":\"Rivington, Bolton BL6 7SA, UK\")
_RE_JSON_ADDRESS = re.compile(r'\\"address\\":\\"([^"]*)\\"')
_RE_JSON_NAME = re.compile(r'\\"name\\":\\"([^"]*)\\"')

# Description-like keys: strip emojis, control chars, HTML entities, smart quotes
DESCRIPTION_KEYS = ("short_description",)

# Remove ASCII control characters (0x00-0x1F, 0x7F)
_RE_CONTROL = re.compile(r"[\x00-\x1f\x7f]+")
# Remove emojis and symbols (broad ranges: misc technical e.g. ⏰, arrows, dingbats, emoji blocks)
_RE_EMOJI = re.compile(
    r"[\u200b-\u200d\u2060\ufe0f\u203c\u2049\u20e3\u2122\u2139"
    r"\u2194-\u2199\u21a9-\u21aa\u231a-\u231b\u2328\u23cf\u23e9-\u23f3\u23f8-\u23fa"
    r"\u2300-\u23ff\u2600-\u26ff\u2700-\u27bf\u2934-\u2935\u2b05-\u2b07\u2b1b-\u2b1c\u2b50\u2b55\u3030\u303d\u3297\u3299"
    r"\U0001f300-\U0001f9ff]+",
    re.UNICODE,
)
# Smart/curly quotes and similar
_SMART_QUOTES = (
    ("\u201c", '"'), ("\u201d", '"'), ("\u2018", "'"), ("\u2019", "'"),
    ("\u2013", " "), ("\u2014", " "), ("\u2026", "..."),
)


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


def _strip_emoji_chars(s):
    """Remove any remaining chars in emoji/symbol code point ranges (fallback for grapheme clusters)."""
    result = []
    for c in s:
        o = ord(c)
        if o <= 0x1F or o == 0x7F:
            continue
        if 0x2300 <= o <= 0x23FF:
            continue
        if 0x2600 <= o <= 0x26FF:
            continue
        if 0x2700 <= o <= 0x27BF:
            continue
        if 0x2934 <= o <= 0x2935 or 0x2B05 <= o <= 0x2B07 or o in (0x2B1B, 0x2B1C, 0x2B50, 0x2B55):
            continue
        if 0x3030 <= o <= 0x303D or o in (0x3297, 0x3299):
            continue
        if 0x1F300 <= o <= 0x1F9FF:
            continue
        if o in (0x200B, 0x200C, 0x200D, 0x2060, 0xFE0F):
            continue
        result.append(c)
    return "".join(result)


def _sanitize_description(s):
    """Clean description text: remove control chars, emojis, HTML entities, smart quotes; normalize whitespace."""
    if not isinstance(s, str):
        return s
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = _RE_CONTROL.sub("", s)
    s = _RE_EMOJI.sub("", s)
    s = _strip_emoji_chars(s)
    for old, new in _SMART_QUOTES:
        s = s.replace(old, new)
    s = s.replace("&nbsp;", " ").replace("\u00a0", " ")
    try:
        s = html.unescape(s)
    except Exception:
        pass
    for c in CHARS_TO_REMOVE:
        s = s.replace(c, "")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


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
            if key == "url":
                continue  # Do not modify URLs; leave as-is
            if key == "address":
                val = _clean_json_like_address(val)
            if key in DATE_KEYS:
                d[key] = _sanitize_date_dash_to_to(val)
            elif key in DESCRIPTION_KEYS:
                d[key] = _sanitize_description(val)
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
