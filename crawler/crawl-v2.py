#!/usr/bin/env python3
import os
import time
import asyncio
import aiofiles
import urllib.parse
import xml.etree.ElementTree as ET
import requests
import shutil
from crawl4ai import AsyncWebCrawler

# === Configuration ===
BASE_DATA_DIR = os.path.join("data", "v2")
SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"
MAX_URLS = int(os.getenv("MAX_URLS", "500"))
CRAWL_DELAY = float(os.getenv("CRAWL_DELAY", "0.5"))
USER_AGENT = os.getenv("USER_AGENT", "Crawl4AI-GitHubAction/2.0 (+https://github.com/)")

# === Helper Functions ===
def domain_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return parsed.netloc.replace("www.", "")

def get_domain_dir(domain: str) -> str:
    """Ensure data/v2/<domain> exists; optionally clean."""
    folder = os.path.join(BASE_DATA_DIR, domain)
    os.makedirs(folder, exist_ok=True)
    if CLEAN_DATA and os.path.exists(folder):
        print(f"🧹 Cleaning data folder: {folder}")
        for root, dirs, files in os.walk(folder):
            for f in files:
                try:
                    os.remove(os.path.join(root, f))
                except Exception:
                    pass
            for d in dirs:
                try:
                    shutil.rmtree(os.path.join(root, d))
                except Exception:
                    pass
    return folder

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
    """Expand a sitemap XML and return all <loc> entries."""
    print(f"🧭 Expanding sitemap: {url}")
    try:
        headers = {"User-Agent": USER_AGENT}
        res = requests.get(url, headers=headers, timeout=20)
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
    """Crawl a single URL and save its Markdown output."""
    try:
        result = await crawler.arun(
            url=url,
            headers={"User-Agent": USER_AGENT},
        )
        md_content = result.markdown or ""
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

    with open(SITEMAP_FILE, "r", encoding="utf-8") as f:
        sitemap_urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]

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

    # Prepare target folder
    first_domain = domain_from_url(urls_to_crawl[0])
    domain_dir = get_domain_dir(first_domain)

    print(f"\n🚀 Starting crawl for {len(urls_to_crawl)} URLs under {first_domain}")
    print(f"   User-Agent: {USER_AGENT}")
    print(f"   Crawl Delay: {CRAWL_DELAY}s\n")

    async with AsyncWebCrawler() as crawler:
        for i, url in enumerate(urls_to_crawl, start=1):
            print(f"[{i}/{len(urls_to_crawl)}] {url}")
            await crawl_url(crawler, url, domain_dir)
            if i < len(urls_to_crawl):
                await asyncio.sleep(CRAWL_DELAY)

    print(f"\n✅ Crawl completed. Files saved under '{domain_dir}/'")

if __name__ == "__main__":
    asyncio.run(main())
