import requests
import json
import os
from time import sleep
from datetime import datetime, UTC
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import unquote # Import unquote

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
                        # Decode the slug when loading existing data as well,
                        # to ensure consistency and prevent re-adding decoded versions
                        # if they were previously stored encoded.
                        try:
                            original_slug = video["slug"]
                            decoded_slug = unquote(original_slug)
                            slugs.add(decoded_slug) # Add the decoded slug to the set for checks
                            video["slug"] = decoded_slug # Update the video dict in memory
                        except Exception as e:
                            print(f"Warning: Could not decode existing slug '{video.get('slug')}' from {filename}. Keeping original. Error: {e}")
                            slugs.add(video["slug"]) # Add the original slug if decoding fails

                        valid_videos.append(video)
                    else:
                        print(f"Warning: Skipping invalid video entry in {filename}: {video}")

                print(f"Loaded {len(valid_videos)} existing videos (found {len(slugs)} unique slugs) from {filename}.")
                return valid_videos, slugs # Return valid videos and their slugs

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
             print(f"Warning: Could not parse date '{date_str}' for sorting video '{video.get('slug', 'N/A')}'. Using fallback date.")
             return datetime(1970, 1, 1, tzinfo=UTC)
        except Exception as e:
             print(f"Warning: Unexpected error parsing date '{date_str}' for video '{video.get('slug', 'N/A')}': {e}. Using fallback date.")
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
            }, f, indent=2, ensure_ascii=False) # ensure_ascii=False is CRUCIAL for emojis
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
        # Check Content-Type header for JSON
        content_type = res.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            # Log the unexpected content type and potentially a snippet of the response
            response_snippet = res.text[:100] + "..." if len(res.text) > 100 else res.text
            print(f"Error: Non-JSON response received from {url} (Page {page}, Category {category}). Status: {res.status_code}. Content-Type: '{content_type}'. Response starts with: '{response_snippet}'")
            return []
        return res.json().get("videos", [])
    except requests.exceptions.Timeout:
        print(f"Error: Timeout loading page {page} for category {category} from {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error loading page {page} for category {category} from {url}: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for page {page}, category {category} (URL: {url}): {e}")
    except Exception as e:
         print(f"An unexpected error occurred fetching page {page}, category {category} (URL: {url}): {e}")
    return []

def get_video_details(slug):
    """Fetches detailed data for a specific video slug."""
    # The detail API likely expects the original, potentially URL-encoded slug
    url = BASE_DETAIL_URL + slug.strip("/")
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
        res.raise_for_status()
        content_type = res.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
             response_snippet = res.text[:100] + "..." if len(res.text) > 100 else res.text
             print(f"Error: Non-JSON response received from {url} (Detail for slug {slug}). Status: {res.status_code}. Content-Type: '{content_type}'. Response starts with: '{response_snippet}'")
             return None
        return res.json()
    except requests.exceptions.Timeout:
        print(f"Error: Timeout loading details for video {slug} from {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error loading details for video {slug} from {url}: {e}")
    except json.JSONDecodeError as e:
         print(f"Error decoding JSON for video details {slug} (URL: {url}): {e}")
    except Exception as e:
        print(f"An unexpected error occurred fetching details for {slug} (URL: {url}): {e}")
    return None

def fetch_and_process_video(vid_summary):
    """
    Fetches details for a video summary, decodes the slug for storage,
    and merges data. Uses the original slug for the API call.
    """
    original_slug = vid_summary.get("name2", "").strip("/")
    if not original_slug:
        print(f"Warning: Skipping video summary due to missing 'name2'/slug: {vid_summary.get('name', 'N/A')}")
        return None, None # Indicate no valid data and no failure to retry

    # Use the original, potentially encoded slug to fetch details
    detail = get_video_details(original_slug)
    if not detail or not isinstance(detail, dict): # Ensure detail is a dict
        return None, original_slug # Return None data, but the original slug that failed

    # --- Decode the slug HERE, *after* using it for the API call ---
    try:
        decoded_slug = unquote(original_slug)
    except Exception as e:
        print(f"Warning: Could not decode slug '{original_slug}'. Using original for storage. Error: {e}")
        decoded_slug = original_slug # Fallback to the original if decoding fails
    # ---------------------------------------------------------------

    upload_date = detail.get("upload_date", "Unknown")
    duration = detail.get("duration", "Unknown")
    download_link = detail.get("download_link", None)

    if upload_date is None: upload_date = "Unknown"
    if duration is None: duration = "Unknown"

    return {
        "title": vid_summary.get("name", "Unknown Title"),
        "slug": decoded_slug, # Store the DECODED slug
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
        # load_data now decodes slugs internally for the existing_slugs set
        load_result = load_data(filename)
        if load_result is None:
            print(f"Skipping category '{category}' for this run due to loading failure. The file '{filename}' will NOT be modified.")
            continue

        existing_videos, existing_decoded_slugs = load_result # This set now contains decoded slugs
        print(f"Successfully loaded data for '{category}'. Using decoded slugs for checks.")

        # --- Step 2: Fetch new data from latest pages ---
        print(f"Checking latest {LATEST_PAGES_TO_SCRAPE} pages for new videos...")
        newly_added_videos = []
        failed_slugs_category = [] # Stores original slugs that failed detail fetch
        processed_original_slugs_this_run = set() # Track original slugs processed

        for page in range(1, LATEST_PAGES_TO_SCRAPE + 1):
            print(f"Scraping page {page}/{LATEST_PAGES_TO_SCRAPE} for {category}...")
            page_videos_summaries = get_page_data(category, page)

            if not page_videos_summaries:
                print(f"No video summaries found or error on page {page} for {category}. Moving to next page.")
                sleep(0.5)
                continue

            slugs_to_fetch_summaries = []
            for vid_summary in page_videos_summaries:
                 if not isinstance(vid_summary, dict):
                     print(f"Warning: Skipping invalid video summary item on page {page}: {vid_summary}")
                     continue

                 original_slug = vid_summary.get("name2", "").strip("/")
                 if not original_slug:
                     continue # Already warned in fetch_and_process_video if called

                 # Decode the slug from the summary *only for the check* against existing data
                 try:
                    check_slug = unquote(original_slug)
                 except Exception:
                     check_slug = original_slug # Use original if decode fails for check

                 # Check if the DECODED slug is new AND the ORIGINAL slug hasn't been processed yet
                 if check_slug not in existing_decoded_slugs and original_slug not in processed_original_slugs_this_run:
                     slugs_to_fetch_summaries.append(vid_summary)
                     processed_original_slugs_this_run.add(original_slug) # Add original slug to processed set

            if not slugs_to_fetch_summaries:
                print(f"No new video slugs found on page {page} for {category}.")
                continue

            print(f"Found {len(slugs_to_fetch_summaries)} potential new video(s) on page {page}. Fetching details...")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Pass the summary containing the potentially encoded slug
                future_to_vid = {executor.submit(fetch_and_process_video, vid): vid for vid in slugs_to_fetch_summaries}

                for future in as_completed(future_to_vid):
                    vid_summary = future_to_vid[future]
                    original_slug_attempted = vid_summary.get("name2", "unknown_slug").strip("/")
                    try:
                        video_data, failed_original_slug = future.result()
                        if video_data:
                            # video_data["slug"] is already decoded by fetch_and_process_video
                            decoded_slug_result = video_data["slug"]
                            # Double-check decoded slug isn't in the existing set
                            if decoded_slug_result not in existing_decoded_slugs:
                                newly_added_videos.append(video_data)
                                # Add the decoded slug to existing_decoded_slugs immediately
                                existing_decoded_slugs.add(decoded_slug_result)
                            else:
                                 print(f"Warning: Skipped adding video {decoded_slug_result} as its decoded slug was already in the existing set (unexpected).")
                        elif failed_original_slug:
                            # Store the original slug that failed
                            if failed_original_slug not in failed_slugs_category:
                                failed_slugs_category.append(failed_original_slug)
                    except Exception as exc:
                        print(f'Fetching details for original slug {original_slug_attempted} generated an exception: {exc}')
                        if original_slug_attempted != "unknown_slug" and original_slug_attempted not in failed_slugs_category:
                            failed_slugs_category.append(original_slug_attempted)

            # Optional: Short sleep between pages to avoid overwhelming the server
            sleep(1)

        # --- Step 3: Retry failed detail fetches ---
        if failed_slugs_category:
            print(f"\nRetrying {len(failed_slugs_category)} failed video detail fetches for {category}...")
            retried_original_slugs = list(set(failed_slugs_category)) # De-duplicate original slugs
            failed_slugs_category.clear() # Clear original list

            with ThreadPoolExecutor(max_workers=MAX_WORKERS // 2) as executor: # Use fewer workers for retries
                 # Create dummy summaries containing the original (potentially encoded) slug
                 future_to_original_slug = {executor.submit(fetch_and_process_video, {"name2": slug}): slug for slug in retried_original_slugs}

                 for future in as_completed(future_to_original_slug):
                     original_slug = future_to_original_slug[future]
                     try:
                         video_data, failed_slug_retry = future.result()
                         if video_data:
                             decoded_slug_result = video_data["slug"] # Already decoded
                             if decoded_slug_result not in existing_decoded_slugs: # Check decoded slug again
                                 print(f"Successfully retried and added: {decoded_slug_result} (Original: {original_slug})")
                                 newly_added_videos.append(video_data)
                                 existing_decoded_slugs.add(decoded_slug_result) # Add decoded slug
                             else:
                                 print(f"Retried original slug {original_slug}, but its decoded form '{decoded_slug_result}' was already added. Skipping.")
                         elif failed_slug_retry:
                             # failed_slug_retry is the original slug
                             print(f"Still failed after retry: {failed_slug_retry}")
                             # Optionally log these persistent failures to a separate file
                     except Exception as exc:
                         print(f'Retry for original slug {original_slug} generated an exception: {exc}')
                 sleep(1) # Sleep after retries


        # --- Step 4: Combine and Save ---
        print(f"\nFinished checking pages for {category}.")
        if newly_added_videos:
            print(f"Found and processed {len(newly_added_videos)} new videos.")

            # Combine new videos first, then existing ones
            all_category_videos = newly_added_videos + existing_videos
            # Note: existing_videos still contains potentially outdated slugs if load_data couldn't decode some.
            # However, the save_data sorting and saving process uses the slugs as they are in the combined list.
            # The primary check using `existing_decoded_slugs` prevents adding duplicates based on the decoded form.

            print(f"Total videos for {category} will be {len(all_category_videos)} (new additions placed at the start before sorting).")
            print(f"Saving updated data for {category}...")
            # save_data will sort this combined list by date (newest first) and save with decoded slugs (where possible)
            save_data(filename, all_category_videos)
        else:
            print(f"No new videos were successfully added for {category} in this run.")
            print(f"File '{filename}' remains unchanged as no new videos were added.")


    end_time = datetime.now(UTC)
    print(f"\n--- Script Finished ---")
    print(f"Ended at: {end_time.isoformat()}")
    print(f"Total duration: {end_time - start_time}")
