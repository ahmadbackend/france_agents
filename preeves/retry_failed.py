import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
from bs4 import BeautifulSoup

FAILED_FILE = "preeves_failed.json"
FINAL_FILE  = "preeves_final.json"
PROXY       = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

WORKERS     = 10
MAX_RETRIES = 5

lock          = threading.Lock()
still_failed  = []


def parse_listings(html):
    soup = BeautifulSoup(html, "html.parser")
    h3 = soup.find("h3", class_="trade-count")
    if h3:
        match = re.search(r"\d+", h3.get_text(strip=True))
        return int(match.group()) if match else 0
    return 0


def get_widget_url(html):
    soup = BeautifulSoup(html, "html.parser")
    div = soup.find("div", class_="imdw-widget")
    if not div:
        return None
    cid   = div.get("data-cid", "")
    hash_ = div.get("data-hash", "")
    ctype = div.get("data-ctype", "company")
    if not cid or not hash_:
        return None
    return (
        f"https://widget3.immodvisor.com/page"
        f"?cid={cid}&ctype={ctype}&hash={hash_}&fp=&wording=plural&number=10"
    )


def parse_reviews(html):
    soup = BeautifulSoup(html, "html.parser")
    rating = None
    spans = soup.find_all("span", class_="imdw-page-rating-number")
    if spans:
        text = spans[0].get_text(strip=True)
        m = re.match(r"([\d.]+)\s*/", text)
        rating = float(m.group(1)) if m else (
            float(re.search(r"[\d.]+", text).group())
            if re.search(r"[\d.]+", text) else None
        )
    reviews = None
    nbr_div = soup.find("div", class_="imdw-page-nbr-reviews")
    if nbr_div:
        span = nbr_div.find("span")
        if span:
            m = re.search(r"\d+", span.get_text(strip=True))
            reviews = int(m.group()) if m else None
        if reviews is None:
            m = re.search(r"\d+", nbr_div.get_text(strip=True))
            reviews = int(m.group()) if m else None
    return reviews, rating


def fetch_one(failed_entry):
    url = failed_entry["profile_url"]
    session = requests.Session(impersonate="chrome", proxy=PROXY)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                listings = parse_listings(resp.text)
                reviews, rating = None, None
                widget_url = get_widget_url(resp.text)
                if widget_url:
                    w = session.get(widget_url, timeout=15)
                    if w.status_code == 200:
                        reviews, rating = parse_reviews(w.text)
                print(f"  OK: listings={listings} reviews={reviews} rating={rating} | {url}")
                return url, listings, reviews, rating
            else:
                print(f"  HTTP {resp.status_code} attempt {attempt}: {url}")
        except Exception as e:
            print(f"  ERROR attempt {attempt}: {e} | {url}")
        time.sleep(2 ** attempt)

    with lock:
        still_failed.append(failed_entry)
    print(f"  STILL FAILED: {url}")
    return url, None, None, None


# Load data
with open(FAILED_FILE, "r", encoding="utf-8") as f:
    failed_list = json.load(f)

with open(FINAL_FILE, "r", encoding="utf-8") as f:
    final_data = json.load(f)

# Build lookup: profile_url → index in final_data
url_to_idx = {row["profile_url"]: i for i, row in enumerate(final_data)}

print(f"Retrying {len(failed_list)} failed agents with {WORKERS} workers...\n")

# Process in parallel
with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = {executor.submit(fetch_one, entry): entry for entry in failed_list}
    for future in as_completed(futures):
        url, listings, reviews, rating = future.result()
        if listings is None and reviews is None and rating is None:
            continue  # still failed, already logged
        idx = url_to_idx.get(url)
        if idx is not None:
            with lock:
                row = final_data[idx]
                if row.get("number_of_listings") == "":
                    row["number_of_listings"] = listings if listings is not None else ""
                if row.get("number_of_reviews") == "":
                    row["number_of_reviews"] = reviews if reviews is not None else ""
                if row.get("average_rating") == "":
                    row["average_rating"] = rating if rating is not None else ""
        else:
            print(f"  WARNING: {url} not found in {FINAL_FILE}")

# Save updated final
with open(FINAL_FILE, "w", encoding="utf-8") as f:
    json.dump(final_data, f, ensure_ascii=False, indent=4)

# Overwrite failed file with what's still unresolved
with open(FAILED_FILE, "w", encoding="utf-8") as f:
    json.dump(still_failed, f, ensure_ascii=False, indent=4)

print(f"\nDone. Resolved: {len(failed_list) - len(still_failed)} | Still failed: {len(still_failed)}")
print(f"Remaining failures saved to {FAILED_FILE}")
