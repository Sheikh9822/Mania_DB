import requests
import json
import os
import re
from datetime import datetime, timedelta, UTC
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import unquote
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuration ---
CATEGORIES = {
    "2d_hmv": "2d_hmv.json",
    "3d_hmv": "3d_hmv.json",
}

# SPEED SETTINGS
LATEST_PAGES_TO_SCRAPE = 3  # Will fetch Page 3, then 2, then 1
MAX_WORKERS = 25            # High concurrency
REQUEST_TIMEOUT = 10

# URLs
BASE_LIST_URL_TEMPLATE = "https://mania_v1.cloud-dl.workers.dev/{category}?page={page}"
BASE_DETAIL_URL = "https://mania_v1.cloud-dl.workers.dev/video/"
PROXY_PREFIX = "https://mania_v1.cloud-dl.workers.dev/proxy?url="

# --- Network Setup ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

session = requests.Session()
retries = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retries)
session.mount('https://', adapter)
session.verify = False

def parse_relative_date(date_str):
    """
    Parses '2 hours ago', 'Yesterday', '3 weeks ago' into 'Jan 18 2026'.
    """
    if not date_str or date_str == "Unknown":
        return datetime.now().strftime("%b %d %Y")

    anchor = datetime.now()
    s = date_str.lower().strip()

    try:
        if "just now" in s or "min" in s or "sec" in s or "hour" in s or "today" in s:
            return anchor.strftime("%b %d %Y")
        if "yesterday" in s:
            return (anchor - timedelta(days=1)).strftime("%b %d %Y")

        match = re.search(r'(\d+)\s+(day|week|month|year)', s)
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            
            delta = timedelta(days=0)
            if "day" in unit: delta = timedelta(days=num)
            elif "week" in unit: delta = timedelta(weeks=num)
            elif "month" in unit: delta = timedelta(days=num * 30)
            elif "year" in unit: delta = timedelta(days=num * 365)
            
            return (anchor - delta).strftime("%b %d %Y")
            
    except Exception:
        pass
    
    return anchor.strftime("%b %d %Y")

def load_data(filename):
    """Loads existing data to avoid duplicates."""
    if not os.path.exists(filename): return [], set()
    try:
        with open(filename, "r", encoding='utf-8') as f:
            content = f.read()
            if not content.strip(): return [], set()
            data = json.loads(content)
            videos = data.get("videos", [])
            slugs = set()
            for v in videos:
                if "slug" in v:
                    slugs.add(v["slug"])
            return videos, slugs
    except Exception:
        return [], set()

def save_data(filename, videos):
    """
    Saves and re-assigns Serial Numbers.
    Highest Serial = Newest Video (Top of list).
    """
    total = len(videos)
    # Assign Serial Numbers: Index 0 (Newest) = Total, Last = 1
    for i, vid in enumerate(videos):
        vid["serial_no"] = total - i

    with open(filename, "w", encoding='utf-8') as f:
        json.dump({
            "last_updated": datetime.now(UTC).isoformat(),
            "total_videos": total,
            "videos": videos
        }, f, indent=2, ensure_ascii=False)
    print(f"  -> Saved {total} videos to {filename}")

def fetch_page_summaries(category, page):
    """Fetches a single page of video summaries."""
    url = BASE_LIST_URL_TEMPLATE.format(category=category, page=page)
    try:
        res = session.get(url, timeout=REQUEST_TIMEOUT)
        if res.status_code == 200:
            return res.json().get("videos", [])
    except Exception:
        pass
    return []

def fetch_video_details_and_process(vid_summary):
    """
    Fetches details for a video and formats the final object.
    """
    original_slug = vid_summary.get("name2", "").strip("/")
    if not original_slug: return None

    # 1. Parse Date from Summary
    relative_date = vid_summary.get("upload_date", "Unknown")
    formatted_date = parse_relative_date(relative_date)

    # 2. Fetch Details
    detail_url = BASE_DETAIL_URL + original_slug
    duration = "Unknown"
    final_link = None

    try:
        res = session.get(detail_url, timeout=REQUEST_TIMEOUT)
        if res.status_code == 200:
            detail = res.json()
            duration = detail.get("duration", "Unknown")
            raw_link = detail.get("download_link")
            
            # 3. Clean Link
            if raw_link:
                final_link = raw_link.replace(PROXY_PREFIX, "")
    except Exception:
        pass

    try: decoded_slug = unquote(original_slug)
    except: decoded_slug = original_slug

    return {
        "serial_no": 0, # Will be set in save_data
        "title": vid_summary.get("name", "Unknown Title"),
        "slug": decoded_slug,
        "thumbnail": vid_summary.get("thumbnail", "Unknown Thumbnail"),
        "views": vid_summary.get("views", "Unknown Views"),
        "upload_date": formatted_date,
        "duration": str(duration),
        "download_link": final_link
    }

# --- Main High-Speed Processor ---
if __name__ == "__main__":
    start_time = datetime.now(UTC)
    print(f"--- High-Performance Scraper Started at {start_time.isoformat()} ---")

    for category, filename in CATEGORIES.items():
        print(f"\nProcessing Category: {category}...")
        
        # 1. Load Existing
        existing_videos, existing_slugs = load_data(filename)
        
        # 2. Fetch pages in REVERSE order (3 -> 2 -> 1)
        print(f"  -> Fetching {LATEST_PAGES_TO_SCRAPE} pages concurrently (Order: {LATEST_PAGES_TO_SCRAPE} -> 1)...")
        
        results_by_page = {} # Store results to maintain order later

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit in reverse order: 3, 2, 1
            future_to_page = {
                executor.submit(fetch_page_summaries, category, p): p 
                for p in range(LATEST_PAGES_TO_SCRAPE, 0, -1)
            }
            
            for future in as_completed(future_to_page):
                p = future_to_page[future]
                videos = future.result()
                results_by_page[p] = videos
                if videos:
                    print(f"     Fetched Page {p}: {len(videos)} videos")

        # 3. Reassemble List: Page 1 must be first, then Page 2, then Page 3
        # We fetch 3->2->1, but we stack 1->2->3 so Newest is at Index 0.
        all_new_summaries = []
        for p in range(1, LATEST_PAGES_TO_SCRAPE + 1):
            if p in results_by_page and results_by_page[p]:
                all_new_summaries.extend(results_by_page[p])

        # 4. Filter Unique Videos
        unique_summaries_to_fetch = []
        seen_original_slugs = set()

        for vid in all_new_summaries:
            raw_slug = vid.get("name2", "").strip("/")
            if not raw_slug: continue
            
            try: check_slug = unquote(raw_slug)
            except: check_slug = raw_slug
            
            if check_slug not in existing_slugs and raw_slug not in seen_original_slugs:
                unique_summaries_to_fetch.append(vid)
                seen_original_slugs.add(raw_slug)

        if not unique_summaries_to_fetch:
            print("  -> No new videos found.")
            continue

        print(f"  -> Found {len(unique_summaries_to_fetch)} new unique videos. Fetching details...")

        # 5. Fetch Details Concurrently
        newly_processed_videos_map = {} # Store by index to keep sorted order
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # We map future -> index so we can put them back in the exact list order
            future_to_index = {
                executor.submit(fetch_video_details_and_process, vid): i 
                for i, vid in enumerate(unique_summaries_to_fetch)
            }
            
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                result = future.result()
                if result:
                    newly_processed_videos_map[index] = result

        # Reconstruct list in order [0, 1, 2...] based on original summary order
        sorted_new_videos = []
        for i in range(len(unique_summaries_to_fetch)):
            if i in newly_processed_videos_map:
                sorted_new_videos.append(newly_processed_videos_map[i])

        # 6. Combine and Save
        # [Newest Page 1 Stuff] + [Old Stuff]
        final_list = sorted_new_videos + existing_videos
        save_data(filename, final_list)

    end_time = datetime.now(UTC)
    print(f"\n--- Finished in {end_time - start_time} ---")