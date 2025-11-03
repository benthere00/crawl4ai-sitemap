#!/usr/bin/env python3
import os
import time
import asyncio
import aiofiles
import urllib.parse
import xml.etree.ElementTree as ET
import requests
from crawl4ai import AsyncWebCrawler

# === Configuration ===
DATA_DIR = "data/v2"
SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"
MAX_URLS = int(os.getenv("MAX_URLS", "500"))
CRAWL_DELAY = float(os.getenv("CRAWL_DELAY", "0.5"))
USER_AGENT = os.getenv("USER_AGENT", "Crawl4AI-GitHubAction/1.0 (+https://github.com/)")
CSS_SELECTORS = [
    s.strip()
    for s in os.getenv("CSS_SELECTOR", "").split(",")
    if s.strip()
]

# === Helper Functions ===
def domain_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return parsed.netloc.replace("www.", "")

def safe_filename(url: str) -> str:
    path = urllib.parse.urlparse(url).path.strip("/")
    if not path:
        path = "index"
    filename = (
        path.replace("/", "_")
        .replace("?", "_")
        .replace("&", "_")
        .replace("=", "_")
        .replace("%", "_")
    )
    return filename + ".md"

def parse_sitemap(url: str) -> list:
    """Parse a sitemap.xml and return a list of URLs."""
    print(f"🧭 Expanding sitemap: {url}")
    try:
        headers = {"User-Agent": USER_AGENT}
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        urls = []
        root = ET.fromstring(res.content)
        for loc in root.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
            if loc.text:
                urls.append(loc.text.strip())
        print(f"   Found {len(urls)} URLs in sitemap.")
        return urls
    except Exception as e:
        print(f"⚠️ Failed to parse sitemap {url}: {e}")
        return []

async def crawl_url(crawler, url, save_dir):
    """Crawl a single URL and save its markdown output."""
    try:
        md_content = None
        last_error = None

        # Try multiple CSS selectors until one yields usable markdown
        selectors = CSS_SELECTORS or [None]
        for sel in selectors:
            try:
                result = await crawler.arun(
                    url=url,
                    headers={"User-Agent": USER_AGENT},
                    css_selector=sel,
                )
                if result and result.markdown:
                    md_content = result.markdown
                    if sel:
                        print(f"   ✅ Extracted using selector: {sel}")
                    break
            except Exception as e:
                last_error = e
                continue

        if not md_content:
            print(f"⚠️ No content extracted for {url}. Last error: {last_error}")
            return

        filename = safe_filename(url)
        filepath = os.path.join(save_dir, filename)
        async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
            await f.write(md_content)

        print(f"✅ Saved {filepath}")

    except Exception as e:
        print(f"❌ Error crawling {url}: {e}")

async def main():
    if not os.path.exists(SITEMAP_FILE):
        print(f"❌ Missing {SITEMAP_FILE}")
        return

    # Collect all URLs from sitemaps.txt
    with open(SITEMAP_FILE, "r", encoding="utf-8") as f:
        sitemap_urls = [line.strip() for line in f if line.strip()]

    all_urls = []
    for sitemap in sitemap_urls:
        if sitemap.endswith(".xml"):
            all_urls.extend(parse_sitemap(sitemap))
        else:
            all_urls.append(sitemap)

    if not all_urls:
        print("⚠️ No URLs found to crawl.")
        return

    # Trim to max allowed
    urls_to_crawl = all_urls[:MAX_URLS]
    first_domain = domain_from_url(urls_to_crawl[0])
    domain_dir = os.path.join(DATA_DIR, first_domain)

    # Ensure domain folder exists
    os.makedirs(domain_dir, exist_ok=True)

    # Clean only that domain folder if requested
    if CLEAN_DATA:
        import shutil
        print(f"🧹 Cleaning old data in {domain_dir} ...")
        shutil.rmtree(domain_dir, ignore_errors=True)
        os.makedirs(domain_dir, exist_ok=True)

    print(f"🚀 Starting crawl for {len(urls_to_crawl)} URLs under {first_domain} ...")
    print(f"   Using User-Agent: {USER_AGENT}")
    print(f"   Crawl delay: {CRAWL_DELAY}s between requests")
    if CSS_SELECTORS:
        print(f"   CSS Selectors: {', '.join(CSS_SELECTORS)}")

    async with AsyncWebCrawler() as crawler:
        for i, url in enumerate(urls_to_crawl, start=1):
            print(f"\n[{i}/{len(urls_to_crawl)}] Crawling: {url}")
            await crawl_url(crawler, url, domain_dir)
            if i < len(urls_to_crawl):
                await asyncio.sleep(CRAWL_DELAY)

    print(f"\n✅ Crawl completed. Saved files under {domain_dir}")

if __name__ == "__main__":
    asyncio.run(main())
