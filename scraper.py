# --- START OF COMPLETE FILE bulk_categories_v3.py ---
import requests
import json
import os
from time import sleep
from datetime import datetime, UTC
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
CATEGORIES = {
    "2d_hmv": "2d_hmv.json",
    "3d_hmv": "3d_hmv.json",
}
LATEST_PAGES_TO_SCRAPE = 3  # How many of the newest pages to check for updates
BASE_LIST_URL_TEMPLATE = "https://mania_v1.cloud-dl.workers.dev/{category}?page={page}"
BASE_DETAIL_URL = "https://mania_v1.cloud-dl.workers.dev/video/"
MAX_WORKERS = 10
REQUEST_TIMEOUT = 15

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_data(filename):
    """
    Loads video data and existing slugs from a JSON file.
    Returns (list_of_videos, set_of_slugs) on success.
    Returns None if the file exists but cannot be read or parsed, to prevent data loss.
    Returns ([], set()) if the file does not exist (first run).
    """
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding='utf-8') as f: # Specify encoding
                # Handle empty file case
                content = f.read()
                if not content.strip():
                    print(f"Warning: File {filename} is empty. Starting fresh.")
                    return [], set()

                # Attempt to load JSON
                data = json.loads(content)
                videos = data.get("videos", [])
                # Validate data structure slightly
                if not isinstance(videos, list):
                     print(f"CRITICAL ERROR: 'videos' key in {filename} is not a list. Data format is invalid.")
                     return None # Indicate failure due to structure

                slugs = set()
                valid_videos = []
                for video in videos:
                    # Ensure basic structure needed for slug extraction
                    if isinstance(video, dict) and "slug" in video:
                        slugs.add(video["slug"])
                        valid_videos.append(video)
                    else:
                        print(f"Warning: Skipping invalid video entry in {filename}: {video}")

                print(f"Loaded {len(valid_videos)} existing videos (found {len(slugs)} unique slugs) from {filename}.")
                return valid_videos, slugs # Return only valid videos and their slugs

        except json.JSONDecodeError as e:
            print(f"CRITICAL ERROR: Failed to decode JSON from {filename}: {e}. Cannot proceed with this file to prevent data loss.")
            return None # Indicate failure
        except IOError as e:
            print(f"CRITICAL ERROR: Could not read file {filename}: {e}. Cannot proceed with this file.")
            return None # Indicate failure
        except Exception as e:
            # Catch other potential exceptions during loading
            print(f"CRITICAL ERROR: An unexpected error occurred while loading {filename}: {e}. Cannot proceed.")
            return None # Indicate failure
    else:
        # File doesn't exist, normal for first run
        print(f"File {filename} not found. Will create a new one.")
        return [], set() # Return empty lists, indicating no existing data

def save_data(filename, videos):
    """Saves video data to a JSON file, sorting by date (newest first)."""
    def sort_key(video):
        # Prioritize 'upload_date', fallback to a very old date if missing/invalid
        date_str = video.get("upload_date", "1970-01-01T00:00:00Z")
        try:
            if isinstance(date_str, str) and date_str != "Unknown" and date_str:
                # Handle timezone 'Z' (UTC)
                if date_str.endswith('Z'):
                    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                else:
                    # Attempt ISO format parse, assume UTC if no timezone info
                    dt = datetime.fromisoformat(date_str)
                    if dt.tzinfo is None:
                         return dt.replace(tzinfo=UTC)
                    return dt
            # Use epoch start for "Unknown" or invalid strings for sorting purpose
            return datetime(1970, 1, 1, tzinfo=UTC)
        except ValueError:
             # Fallback for unexpected date formats
             return datetime(1970, 1, 1, tzinfo=UTC)

    try:
        # Sort descending by date (newest first)
        videos.sort(key=sort_key, reverse=True)
        print(f"Sorted combined video list by date (newest first).")
    except Exception as e:
        print(f"Warning: Could not sort videos for {filename} due to an unexpected error: {e}. Saving in potentially unsorted (newly added first) order.")

    try:
        with open(filename, "w", encoding='utf-8') as f: # Specify encoding
            json.dump({
                "last_updated": datetime.now(UTC).isoformat(),
                "total_videos": len(videos),
                "videos": videos # Save the (potentially sorted) list
            }, f, indent=2, ensure_ascii=False) # ensure_ascii=False for broader char support
        print(f"Successfully saved {len(videos)} total videos to {filename}.")
    except IOError as e:
        print(f"CRITICAL ERROR: Could not write data to file {filename}: {e}")
    except Exception as e:
         print(f"CRITICAL ERROR: An unexpected error occurred while saving {filename}: {e}")


def get_page_data(category, page):
    """Fetches list data for a specific category and page."""
    url = BASE_LIST_URL_TEMPLATE.format(category=category, page=page)
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
        res.raise_for_status()
        if 'application/json' not in res.headers.get('Content-Type', ''):
             print(f"Error: Non-JSON response received from {url} (Page {page}, Category {category}). Status: {res.status_code}.") # Content removed for brevity
             return []
        return res.json().get("videos", [])
    except requests.exceptions.RequestException as e:
        print(f"Error loading page {page} for category {category} from {url}: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for page {page}, category {category} (URL: {url}): {e}")
    except Exception as e:
         print(f"An unexpected error occurred fetching page {page}, category {category} (URL: {url}): {e}")
    return []

def get_video_details(slug):
    """Fetches detailed data for a specific video slug."""
    url = BASE_DETAIL_URL + slug.strip("/")
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
        res.raise_for_status()
        if 'application/json' not in res.headers.get('Content-Type', ''):
             print(f"Error: Non-JSON response received from {url} (Detail for slug {slug}). Status: {res.status_code}.") # Content removed for brevity
             return None
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error loading details for video {slug} from {url}: {e}")
    except json.JSONDecodeError as e:
         print(f"Error decoding JSON for video details {slug} (URL: {url}): {e}")
    except Exception as e:
        print(f"An unexpected error occurred fetching details for {slug} (URL: {url}): {e}")
    return None

def fetch_and_process_video(vid_summary):
    """Fetches details and merges data for a single video summary."""
    slug = vid_summary.get("name2", "").strip("/")
    if not slug:
        print(f"Warning: Skipping video summary due to missing 'name2'/slug: {vid_summary.get('name', 'N/A')}")
        return None, None # Indicate no valid data and no failure to retry

    detail = get_video_details(slug)
    if not detail or not isinstance(detail, dict): # Ensure detail is a dict
        return None, slug # Return None data, but the slug that failed

    upload_date = detail.get("upload_date", "Unknown")
    duration = detail.get("duration", "Unknown")
    download_link = detail.get("download_link", None)

    if upload_date is None: upload_date = "Unknown"
    if duration is None: duration = "Unknown"

    return {
        "title": vid_summary.get("name", "Unknown Title"),
        "slug": slug,
        "thumbnail": vid_summary.get("thumbnail", "Unknown Thumbnail"),
        "views": vid_summary.get("views", "Unknown Views"),
        "upload_date": str(upload_date), # Ensure string type
        "duration": str(duration),     # Ensure string type
        "download_link": download_link
    }, None # Return data and None for failed slug


# --- Main Processing Logic ---
if __name__ == "__main__":
    start_time = datetime.now(UTC)
    print(f"Starting script at {start_time.isoformat()}")

    for category, filename in CATEGORIES.items():
        print(f"\n--- Processing Category: {category} ---")
        print(f"Output file: {filename}")

        # --- Step 1: Load existing data SAFELY ---
        load_result = load_data(filename)
        if load_result is None:
            print(f"Skipping category '{category}' for this run due to loading failure. The file '{filename}' will NOT be modified.")
            continue

        existing_videos, existing_slugs = load_result
        print(f"Successfully loaded data for '{category}'.")

        # --- Step 2: Fetch new data from latest pages ---
        print(f"Checking latest {LATEST_PAGES_TO_SCRAPE} pages for new videos...")
        newly_added_videos = []
        failed_slugs_category = []
        processed_slugs_this_run = set()

        for page in range(1, LATEST_PAGES_TO_SCRAPE + 1):
            print(f"Scraping page {page}/{LATEST_PAGES_TO_SCRAPE} for {category}...")
            page_videos_summaries = get_page_data(category, page)

            if not page_videos_summaries:
                print(f"No video summaries found or error on page {page} for {category}. Moving to next page.")
                sleep(0.5)
                continue

            slugs_to_fetch = []
            for vid_summary in page_videos_summaries:
                 if not isinstance(vid_summary, dict):
                     print(f"Warning: Skipping invalid video summary item on page {page}: {vid_summary}")
                     continue
                 slug = vid_summary.get("name2", "").strip("/")
                 if slug and slug not in existing_slugs and slug not in processed_slugs_this_run:
                     slugs_to_fetch.append(vid_summary)
                     processed_slugs_this_run.add(slug)

            if not slugs_to_fetch:
                print(f"No new video slugs found on page {page} for {category}.")
                continue

            print(f"Found {len(slugs_to_fetch)} potential new video(s) on page {page}. Fetching details...")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_vid = {executor.submit(fetch_and_process_video, vid): vid for vid in slugs_to_fetch}

                for future in as_completed(future_to_vid):
                    vid_summary = future_to_vid[future]
                    try:
                        video_data, failed_slug = future.result()
                        if video_data:
                            # Double-check slug isn't already somehow in existing_videos (shouldn't happen with checks above, but safe)
                            if video_data["slug"] not in existing_slugs:
                                newly_added_videos.append(video_data)
                                # Add slug to existing_slugs immediately to prevent duplicates if fetched again
                                existing_slugs.add(video_data["slug"])
                            else:
                                 print(f"Warning: Skipped adding video {video_data['slug']} as it was already in the existing set (unexpected).")
                        elif failed_slug:
                            if failed_slug not in failed_slugs_category: # Avoid duplicates in failed list
                                failed_slugs_category.append(failed_slug)
                    except Exception as exc:
                        slug_attempted = vid_summary.get("name2", "unknown_slug").strip("/")
                        print(f'Fetching details for slug {slug_attempted} generated an exception: {exc}')
                        if slug_attempted != "unknown_slug" and slug_attempted not in failed_slugs_category:
                            failed_slugs_category.append(slug_attempted)

            # Optional: Short sleep between pages to avoid overwhelming the server
            sleep(1)

        # --- Step 3: Retry failed detail fetches ---
        if failed_slugs_category:
            print(f"\nRetrying {len(failed_slugs_category)} failed video detail fetches for {category}...")
            retried_slugs = list(set(failed_slugs_category)) # De-duplicate
            failed_slugs_category.clear() # Clear original list

            with ThreadPoolExecutor(max_workers=MAX_WORKERS // 2) as executor: # Use fewer workers for retries
                 # Create dummy summaries just containing the slug for the function
                 future_to_slug = {executor.submit(fetch_and_process_video, {"name2": slug}): slug for slug in retried_slugs}

                 for future in as_completed(future_to_slug):
                     slug = future_to_slug[future]
                     try:
                         video_data, failed_slug_retry = future.result()
                         if video_data:
                             if video_data["slug"] not in existing_slugs: # Double check slug hasn't been added
                                 print(f"Successfully retried and added: {slug}")
                                 newly_added_videos.append(video_data)
                                 existing_slugs.add(video_data["slug"])
                             else:
                                 print(f"Retried {slug}, but it was already added (perhaps concurrently or previously). Skipping.")
                         elif failed_slug_retry:
                             print(f"Still failed after retry: {slug}")
                             # Optionally log these persistent failures to a separate file
                     except Exception as exc:
                         print(f'Retry for slug {slug} generated an exception: {exc}')
                 sleep(1) # Sleep after retries


        # --- Step 4: Combine and Save ---
        print(f"\nFinished checking pages for {category}.")
        if newly_added_videos:
            print(f"Found and processed {len(newly_added_videos)} new videos.")

            # --- MODIFICATION: Prepend new videos ---
            # Combine new videos first, then existing ones
            all_category_videos = newly_added_videos + existing_videos
            # -----------------------------------------

            print(f"Total videos for {category} will be {len(all_category_videos)} (new additions placed at the start before sorting).")
            print(f"Saving updated data for {category}...")
            # save_data will sort this combined list by date (newest first)
            save_data(filename, all_category_videos)
        else:
            print(f"No new videos were successfully added for {category} in this run.")
            # Optionally save even if no new videos, just to update the "last_updated" timestamp
            # print(f"Updating timestamp in {filename}...")
            # save_data(filename, existing_videos) # Pass the original list
            print(f"File '{filename}' remains unchanged as no new videos were added.")


    end_time = datetime.now(UTC)
    print(f"\n--- Script Finished ---")
    print(f"Ended at: {end_time.isoformat()}")
    print(f"Total duration: {end_time - start_time}")

# --- END OF COMPLETE FILE bulk_categories_v3.py ---
