"""Tests for the canonicalisation utilities — these underpin entity resolution."""
from newhomes.core.normalise import (
    canonical_abn,
    canonical_domain,
    canonical_fb_url,
    canonical_state,
    normalise_name,
)


def test_normalise_name_strips_suffixes():
    assert normalise_name("Stockland Pty Ltd") == "stockland"
    assert normalise_name("Mirvac Group Properties") == "mirvac"
    assert normalise_name("Metricon Homes Pty. Ltd.") == "metricon"
    assert normalise_name("Burbank Australia") == "burbank australia"
    assert normalise_name("AVJennings Properties Limited") == "avjennings"


def test_normalise_name_empty():
    assert normalise_name("") == ""
    assert normalise_name("   ") == ""


def test_canonical_domain():
    assert canonical_domain("https://www.Stockland.com.au/communities/aura") == "stockland.com.au"
    assert canonical_domain("stockland.com.au") == "stockland.com.au"
    assert canonical_domain("STOCKLAND.COM.AU") == "stockland.com.au"
    assert canonical_domain("not a url") is None
    assert canonical_domain("") is None


def test_canonical_fb_url_variants():
    assert canonical_fb_url("https://www.facebook.com/Stockland/") == "https://www.facebook.com/Stockland"
    assert canonical_fb_url("http://m.facebook.com/Mirvac?ref=foo") == "https://www.facebook.com/Mirvac"
    assert canonical_fb_url("https://facebook.com/pages/Some-Estate/1234567890") == \
        "https://www.facebook.com/pages/Some-Estate/1234567890"
    assert canonical_fb_url("https://example.com/foo") is None
    assert canonical_fb_url("") is None


def test_canonical_abn():
    assert canonical_abn("11 222 333 444") == "11222333444"
    assert canonical_abn("ABN: 11222333444") == "11222333444"
    assert canonical_abn("123") is None  # too short
    assert canonical_abn(None) is None


def test_canonical_state():
    assert canonical_state("NSW") == "NSW"
    assert canonical_state("new south wales") == "NSW"
    assert canonical_state("Vic") == "VIC"
    assert canonical_state("queensland") == "QLD"
    assert canonical_state("not a state") is None
    assert canonical_state(None) is None
