import requests
import json
import os
from datetime import datetime, UTC
import urllib3
from time import sleep

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URLS = {
    "2d_hmv": "https://mania_v1.cloud-dl.workers.dev/2d_hmv?page={}",
    "3d_hmv": "https://mania_v1.cloud-dl.workers.dev/3d_hmv?page={}"
}
DETAIL_URL = "https://mania_v1.cloud-dl.workers.dev/video/"
TOTAL_PAGES = 1
TIMEOUT = 10

def fetch_video_details(slug):
    try:
        res = requests.get(DETAIL_URL + slug.strip("/"), timeout=TIMEOUT, verify=False)
        return res.json()
    except Exception as e:
        print(f"  [!] Error fetching details for {slug}: {e}")
        return {}

def scrape(category):
    all_videos = []
    seen = set()
    print(f"Scraping {category}...")

    for page in range(1, TOTAL_PAGES + 1):
        print(f"  Page {page}/{TOTAL_PAGES}")
        try:
            res = requests.get(BASE_URLS[category].format(page), timeout=TIMEOUT, verify=False)
            videos = res.json().get("videos", [])
        except Exception as e:
            print(f"  [!] Failed to load page {page}: {e}")
            continue

        for vid in videos:
            slug = vid["name2"].strip("/")
            if slug in seen:
                continue
            details = fetch_video_details(slug)
            all_videos.append({
                "title": vid["name"],
                "slug": slug,
                "thumbnail": vid["thumbnail"],
                "views": vid["views"],
                "upload_date": details.get("upload_date", "Unknown"),
                "duration": details.get("duration", "Unknown"),
                "download_link": details.get("download_link", None)
            })
            seen.add(slug)

    with open(f"{category}.json", "w") as f:
        json.dump({
            "last_updated": datetime.now(UTC).isoformat(),
            "videos": all_videos
        }, f, indent=2)
    print(f"Saved {category}.json")

if __name__ == "__main__":
    scrape("2d_hmv")
    scrape("3d_hmv")
          
