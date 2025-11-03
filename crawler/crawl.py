#!/usr/bin/env python3
import os
import requests
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from bs4 import BeautifulSoup

DATA_DIR = "data"
SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
USER_AGENT = os.getenv("USER_AGENT", "Crawl4AI/1.0 (+https://github.com/<yourname>/Crawl4AI)")
DELAY = float(os.getenv("CRAWL_DELAY", 0.5))  # seconds between requests


def load_sitemaps():
    """Read sitemap URLs from sitemaps.txt"""
    if not os.path.exists(SITEMAP_FILE):
        raise FileNotFoundError(f"Missing sitemap list: {SITEMAP_FILE}")
    with open(SITEMAP_FILE) as f:
        sitemaps = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    print(f"🗺️ Loaded {len(sitemaps)} sitemap(s)")
    return sitemaps


def parse_sitemap(url):
    """Return a list of URLs from a sitemap.xml"""
    print(f"Fetching sitemap: {url}")
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc.text]
    print(f"✅ Found {len(urls)} URLs in {url}")
    return urls


def fetch_page(url):
    """Download page and extract readable text"""
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove script/style tags
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()

        title = soup.title.string.strip() if soup.title and soup.title.string else url
        text = " ".join(soup.get_text().split())
        return title, text

    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return None, None


def url_to_filename(url):
    """Turn a URL into a filesystem-safe slug"""
    parsed = urlparse(url)
    slug = parsed.netloc + parsed.path
    if slug.endswith("/"):
        slug = slug[:-1]
    slug = slug.replace("/", "_")
    if not slug:
        slug = "index"
    return f"{slug}.md"


def save_markdown(url, title, text):
    """Save page as Markdown"""
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = url_to_filename(url)
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"**URL:** {url}\n\n")
        f.write(text)
    print(f"✅ Saved: {path}")


def crawl():
    sitemaps = load_sitemaps()
    all_urls = []
    for sm in sitemaps:
        all_urls.extend(parse_sitemap(sm))

    print(f"🌐 Total URLs found: {len(all_urls)}")

    for url in all_urls:
        # Skip media and obvious non-page content
        if url.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".mp4")):
            continue
        title, text = fetch_page(url)
        if title and text:
            save_markdown(url, title, text)
        time.sleep(DELAY)


if __name__ == "__main__":
    crawl()
