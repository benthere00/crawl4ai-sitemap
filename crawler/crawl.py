#!/usr/bin/env python3
import os
import time
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from crawl4ai import Crawl4AI

DATA_DIR = "data"
SITEMAP_FILE = "sitemaps.txt"
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"
CRAWL_DELAY = float(os.getenv("CRAWL_DELAY", 0.5))
USER_AGENT = os.getenv("USER_AGENT", "Crawl4AI-GitHubAction/1.0")


def clean_data_folder():
    if CLEAN_DATA and os.path.exists(DATA_DIR):
        print(f"🧹 Cleaning old data in {DATA_DIR}/ ...")
        for f in os.listdir(DATA_DIR):
            os.remove(os.path.join(DATA_DIR, f))
    os.makedirs(DATA_DIR, exist_ok=True)


def parse_sitemap(url):
    print(f"🗺️ Fetching sitemap: {url}")
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = [loc.text.strip() for loc in root.findall(".//sm:loc", ns)]
    print(f"✅ Found {len(urls)} URLs")
    return urls


def url_to_filename(url):
    parsed = urlparse(url)
    slug = parsed.netloc + parsed.path
    if slug.endswith("/"):
        slug = slug[:-1]
    slug = slug.replace("/", "_")
    return f"{slug}.md"


def crawl_url(crawler, url):
    print(f"🌐 Crawling: {url}")
    try:
        result = crawler.crawl(url, to_markdown=True)
        if not result or not result.markdown:
            print(f"⚠️ Empty result for {url}")
            return

        path = os.path.join(DATA_DIR, url_to_filename(url))
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"# {result.title or url}\n\n")
            f.write(f"**URL:** {url}\n\n")
            f.write(result.markdown)

        print(f"✅ Saved: {path}")
    except Exception as e:
        print(f"⚠️ Error crawling {url}: {e}")


def main():
    clean_data_folder()

    if not os.path.exists(SITEMAP_FILE):
        print(f"❌ Missing sitemap file: {SITEMAP_FILE}")
        return

    with open(SITEMAP_FILE) as f:
        sitemap_urls = [line.strip() for line in f if line.strip()]

    all_urls = []
    for sm in sitemap_urls:
        all_urls.extend(parse_sitemap(sm))

    if not all_urls:
        print("⚠️ No URLs to crawl. Exiting.")
        return

    crawler = Crawl4AI()
    start = time.time()

    for url in all_urls:
        crawl_url(crawler, url)
        time.sleep(CRAWL_DELAY)

    print(f"\n✅ Crawl finished in {time.time() - start:.1f}s, results in '{DATA_DIR}/'.")


if __name__ == "__main__":
    main()
