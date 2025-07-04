import feedparser
import pandas as pd
import os
import uuid
from datetime import datetime
import time
import re

RSS_FEED_URL = "https://stackoverflow.com/jobs/feed"
OUTPUT_DIR = "../../../data/bronze/Jobs_feed/RSS"
POLL_INTERVAL = 300  # 5 minutes

TECH_KEYWORDS = ["python", "java", "aws", "docker", "kubernetes", "sql", "azure", "gcp", "typescript", "node", "react"]

os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract_technologies(text):
    text = text.lower()
    return [tech for tech in TECH_KEYWORDS if re.search(r"\b" + re.escape(tech) + r"\b", text)]

def fetch_and_parse_rss():
    feed = feedparser.parse(RSS_FEED_URL)
    jobs = []
    for entry in feed.entries:
        summary = entry.get("summary", "")
        jobs.append({
            "id": entry.get("id", str(uuid.uuid4())),
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "summary": summary,
            "author": entry.get("author", ""),
            "tech_keywords": extract_technologies(summary),
            "retrieved_at": datetime.utcnow().isoformat()
        })
    return pd.DataFrame(jobs)

def save_to_parquet(df: pd.DataFrame):
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    output_path = os.path.join(OUTPUT_DIR, f"jobs_feed_{timestamp}.parquet")
    df.to_parquet(output_path, index=False)
    print(f"[{timestamp}] ✅ Saved {len(df)} job entries to {output_path}")

def main():
    while True:
        print(f"[{datetime.utcnow().isoformat()}] 🔍 Fetching RSS feed...")
        try:
            df = fetch_and_parse_rss()
            if not df.empty:
                save_to_parquet(df)
            else:
                print("⚠️ No new jobs found.")
        except Exception as e:
            print(f"❌ Error fetching or saving feed: {e}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
