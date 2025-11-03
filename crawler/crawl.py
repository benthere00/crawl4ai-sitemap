import os
import time
from urllib.parse import urlparse
from crawl4ai import WebCrawler

DATA_DIR = "data"
SITEMAP_FILE = os.getenv("SITEMAP_FILE", "sitemaps.txt")
CLEAN_DATA = os.getenv("CLEAN_DATA", "true").lower() == "true"

def load_urls():
    urls = []
    if os.path.exists(SITEMAP_FILE):
        with open(SITEMAP_FILE, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u:
                    urls.append(u)
    return urls

def safe_filename(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.netloc + parsed.path
    if path.endswith("/"):
        path = path[:-1]
    path = path.replace("/", "_").replace("?", "_").replace("&", "_")
    return f"{path}.md"

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    if CLEAN_DATA:
        for f in os.listdir(DATA_DIR):
            os.remove(os.path.join(DATA_DIR, f))

    urls = load_urls()
    if not urls:
        print("⚠️ No URLs found in sitemaps.txt")
        return

    crawler = WebCrawler()

    for url in urls:
        print(f"🌐 Crawling: {url}")
        try:
            content = crawler.crawl(url)
            filename = safe_filename(url)
            filepath = os.path.join(DATA_DIR, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content.markdown)
            print(f"✅ Saved: {filepath}")
        except Exception as e:
            print(f"❌ Error crawling {url}: {e}")
        time.sleep(1)  # small delay between requests

if __name__ == "__main__":
    main()
