import os
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

SITEMAP_URL = os.getenv("SITEMAP_URL", "https://steam-n-dry.co.nz/page-sitemap.xml")
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_sitemap(url):
    print(f"Fetching sitemap: {url}")
    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "xml")
    return [loc.text.strip() for loc in soup.find_all("loc")]

def fetch_page(url):
    print(f"Fetching: {url}")
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.string.strip() if soup.title else ""
        content = md(str(soup.find("main") or soup.body))  # convert to markdown
        return title, content
    except Exception as e:
        print(f"❌ Error on {url}: {e}")
        return None, None

def slugify(url):
    clean = url.replace("https://", "").replace("http://", "").strip("/")
    clean = clean.replace("/", "_")
    return clean or "index"

def main():
    urls = fetch_sitemap(SITEMAP_URL)
    print(f"Found {len(urls)} URLs.")
    for url in urls:
        title, content = fetch_page(url)
        if content:
            slug = slugify(url)
            path = os.path.join(OUTPUT_DIR, f"{slug}.md")
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"---\nurl: {url}\ntitle: {title}\n---\n\n{content}")
            print(f"✅ Saved: {path}")

if __name__ == "__main__":
    main()
