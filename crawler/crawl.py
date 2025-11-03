#!/usr/bin/env python3
import os
import time
import random
import asyncio
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler

# === Load environment ===
load_dotenv()

SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
OUTPUT_DIR = "data"
MAX_URLS = int(os.getenv("MAX_URLS", "500"))
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"


def expand_sitemap(url):
    """Expand a sitemap.xml into a list of URLs."""
    urls = []
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()

        root = ET.fromstring(res.text)
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        # Handle <urlset>
        for loc in root.findall(".//ns:loc", ns):
            if loc.text:
                urls.append(loc.text.strip())

        # Handle nested <sitemapindex>
        for sm in root.findall(".//ns:sitemap/ns:loc", ns):
            nested = expand_sitemap(sm.text.strip())
            urls.extend(nested)

    except Exception as e:
        print(f"⚠️ Failed to expand sitemap {url}: {e}")
    return urls


def load_sitemap_list():
    """Load sitemap or URLs from text file."""
    all_urls = []
    if not os.path.exists(SITEMAP_FILE):
        print(f"⚠️ Sitemap file {SITEMAP_FILE} not found!")
        return []

    with open(SITEMAP_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.endswith(".xml"):
                print(f"📜 Expanding sitemap XML: {line}")
                urls = expand_sitemap(line)
                print(f"  → Found {len(urls)} URLs")
                all_urls.extend(urls)
            else:
                all_urls.append(line)

    unique = list(dict.fromkeys(all_urls))
    print(f"✅ Total URLs loaded (deduped): {len(unique)}")
    return unique[:MAX_URLS]


async def crawl_url(crawler, url, index, total):
    """Crawl a single URL asynchronously."""
    print(f"[{index}/{total}] Crawling: {url}")
    try:
        result = await crawler.arun(url=url)
        if not result or not result.markdown:
            print("⚠️ No markdown content found.")
            return
        safe_name = url.replace("https://", "").replace("http://", "").replace("/", "_")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(os.path.join(OUTPUT_DIR, f"{safe_name}.md"), "w", encoding="utf-8") as f:
            f.write(result.markdown)
        print(f"✅ Saved {safe_name}.md")
    except Exception as e:
        print(f"❌ Error crawling {url}: {e}")
    await asyncio.sleep(random.uniform(0.3, 1.0))


async def main():
    urls = load_sitemap_list()
    if not urls:
        print("⚠️ No URLs to crawl. Exiting.")
        return

    if CLEAN_DATA and os.path.exists(OUTPUT_DIR):
        for root, _, files in os.walk(OUTPUT_DIR):
            for f in files:
                os.remove(os.path.join(root, f))
        print(f"🧹 Cleaned old data from {OUTPUT_DIR}")

    async with AsyncWebCrawler() as crawler:
        tasks = []
        for i, url in enumerate(urls, start=1):
            tasks.append(crawl_url(crawler, url, i, len(urls)))
        await asyncio.gather(*tasks)

    print("🎉 Crawl complete! Markdown files in /data")


if __name__ == "__main__":
    asyncio.run(main())
