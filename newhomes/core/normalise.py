"""Canonicalisation utilities — names, domains, Facebook URLs, ABNs.

These are the spine of entity resolution. Tests in tests/test_normalise.py.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse, urlunparse

import tldextract

# Words to strip from developer/builder names before fuzzy matching.
# We keep the raw name in `name`; this produces `normalised_name`.
_NAME_NOISE = re.compile(
    r"\b("
    r"pty\s*ltd|pty|ltd|limited|inc|incorporated|corporation|corp|"
    r"holdings|group|properties|property|developments?|developers?|"
    r"builders?|homes?|construction|constructions?|"
    r"the|and|&"
    r")\b\.?",
    flags=re.IGNORECASE,
)

_NON_WORD = re.compile(r"[^a-z0-9 ]+")
_MULTISPACE = re.compile(r"\s+")


def normalise_name(name: str) -> str:
    """Lowercase, strip noise words, collapse whitespace.

    >>> normalise_name("Stockland Pty Ltd")
    'stockland'
    >>> normalise_name("Mirvac Group Properties")
    'mirvac'
    >>> normalise_name("Metricon Homes Pty. Ltd.")
    'metricon'
    """
    s = name.lower().strip()
    s = _NAME_NOISE.sub(" ", s)
    s = _NON_WORD.sub(" ", s)
    s = _MULTISPACE.sub(" ", s).strip()
    return s


def canonical_domain(url_or_domain: str) -> str | None:
    """Return registrable domain in lowercase, no www, no path.

    >>> canonical_domain("https://www.Stockland.com.au/communities")
    'stockland.com.au'
    >>> canonical_domain("stockland.com.au")
    'stockland.com.au'
    >>> canonical_domain("not a url")
    """
    if not url_or_domain:
        return None
    s = url_or_domain.strip().lower()
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    try:
        ext = tldextract.extract(s)
    except Exception:
        return None
    if not ext.domain or not ext.suffix:
        return None
    return f"{ext.domain}.{ext.suffix}"


def canonical_fb_url(url: str) -> str | None:
    """Resolve to https://www.facebook.com/<page> form. Strips query/path tail.

    Accepts m.facebook.com, fb.com, /pages/<name>/<id>, etc. Returns None if it
    doesn't look like a Facebook page URL at all.

    >>> canonical_fb_url("https://www.facebook.com/Stockland/")
    'https://www.facebook.com/Stockland'
    >>> canonical_fb_url("http://m.facebook.com/Mirvac?ref=foo")
    'https://www.facebook.com/Mirvac'
    >>> canonical_fb_url("https://facebook.com/pages/Some-Estate/1234567890")
    'https://www.facebook.com/pages/Some-Estate/1234567890'
    """
    if not url:
        return None
    try:
        p = urlparse(url.strip())
    except Exception:
        return None
    host = (p.hostname or "").lower()
    if not host or "facebook.com" not in host and host != "fb.com":
        return None
    path = p.path.rstrip("/")
    if not path or path == "/":
        return None
    # /pages/<slug>/<id> is canonical and must be preserved
    return urlunparse(("https", "www.facebook.com", path, "", "", ""))


_ABN_DIGITS = re.compile(r"\d")


def canonical_abn(s: str | None) -> str | None:
    """Strip non-digits; return 11-digit ABN or None."""
    if not s:
        return None
    digits = "".join(_ABN_DIGITS.findall(s))
    return digits if len(digits) == 11 else None


_AU_STATES = {
    "nsw": "NSW", "new south wales": "NSW",
    "vic": "VIC", "victoria": "VIC",
    "qld": "QLD", "queensland": "QLD",
    "wa": "WA",  "western australia": "WA",
    "sa": "SA",  "south australia": "SA",
    "tas": "TAS", "tasmania": "TAS",
    "act": "ACT", "australian capital territory": "ACT",
    "nt": "NT",  "northern territory": "NT",
}


def canonical_state(s: str | None) -> str | None:
    if not s:
        return None
    return _AU_STATES.get(s.strip().lower())
