import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
from bs4 import BeautifulSoup

INPUT_FILE  = "preeves_final.json"
FAILED_FILE = "preeves_failed.json"
WORKERS     = 20
MAX_RETRIES = 3
SAVE_EVERY  = 100
PROXY       = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

lock          = threading.Lock()
save_counter  = 0
updated_count = 0
error_count   = 0
failed_urls   = []

def save():
    with open(INPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def needs_scraping(agent):
    """Re-scrape if any of the three fields is still missing."""
    return (
        agent.get("number_of_listings") == "" or
        agent.get("number_of_reviews")  == "" or
        agent.get("average_rating")     == ""
    )

def parse_listings(html):
    soup = BeautifulSoup(html, "html.parser")
    h3 = soup.find("h3", class_="trade-count")
    if h3:
        match = re.search(r"\d+", h3.get_text(strip=True))
        return int(match.group()) if match else 0
    return 0

def get_immodvisor_widget_url(html):
    """
    Extract cid/ctype/hash from the imdw-widget div and build the
    /page endpoint URL (always use page route — it contains both
    rating and review count regardless of data-type on the div).
    Required params: fp=, wording=plural, number=10.
    """
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
    """Parse average_rating and number_of_reviews from immodvisor widget HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Rating — two formats:
    # Format A: <span>4.8</span> / <span>5</span>  (separate spans)
    # Format B: <span>5/5</span>                    (single span, e.g. "5/5")
    rating = None
    rating_spans = soup.find_all("span", class_="imdw-page-rating-number")
    if rating_spans:
        text = rating_spans[0].get_text(strip=True)
        slash_match = re.match(r"([\d.]+)\s*/", text)
        if slash_match:
            rating = float(slash_match.group(1))
        else:
            match = re.search(r"[\d.]+", text)
            rating = float(match.group()) if match else None

    # Reviews — number is inside a <span> inside imdw-page-nbr-reviews
    reviews = None
    nbr_div = soup.find("div", class_="imdw-page-nbr-reviews")
    if nbr_div:
        span = nbr_div.find("span")
        if span:
            match = re.search(r"\d+", span.get_text(strip=True))
            reviews = int(match.group()) if match else None
        if reviews is None:
            match = re.search(r"\d+", nbr_div.get_text(strip=True))
            reviews = int(match.group()) if match else None

    return reviews, rating

def fetch(index, agent):
    global save_counter, updated_count, error_count

    url = agent.get("profile_url", "")
    if not url:
        return

    session = requests.Session(impersonate="chrome", proxy=PROXY)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                listings = parse_listings(resp.text)

                reviews, rating = None, None
                widget_url = get_immodvisor_widget_url(resp.text)
                if widget_url:
                    widget_resp = session.get(widget_url, timeout=15)
                    if widget_resp.status_code == 200:
                        reviews, rating = parse_reviews(widget_resp.text)
                    else:
                        print(f"[{index}] Widget HTTP {widget_resp.status_code}: {widget_url}")
                else:
                    print(f"[{index}] No immodvisor widget found on page: {url}")

                with lock:
                    # Only overwrite fields that are still empty
                    if agent.get("number_of_listings") == "":
                        agent["number_of_listings"] = listings
                    if agent.get("number_of_reviews") == "":
                        agent["number_of_reviews"] = reviews if reviews is not None else ""
                    if agent.get("average_rating") == "":
                        agent["average_rating"] = rating if rating is not None else ""

                    updated_count += 1
                    save_counter  += 1
                    print(f"[{index}] OK | listings={listings} reviews={reviews} rating={rating} | {url}")
                    if save_counter % SAVE_EVERY == 0:
                        save()
                        print(f"  -> Progress saved ({save_counter} done)")
                return
            else:
                print(f"[{index}] HTTP {resp.status_code} attempt {attempt}: {url}")
        except Exception as e:
            print(f"[{index}] ERROR attempt {attempt}: {e} — {url}")

        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)  # exponential backoff: 2s, 4s

    # All retries exhausted
    with lock:
        error_count += 1
        failed_urls.append({
            "index":      index,
            "profile_url": url,
            "first_name": agent.get("first_name", ""),
            "last_name":  agent.get("last_name", "")
        })
        print(f"[{index}] FAILED after {MAX_RETRIES} retries: {url}")

total   = len(data)
pending = [(i, agent) for i, agent in enumerate(data) if needs_scraping(agent)]

print(f"Total: {total} | To scrape: {len(pending)} | Workers: {WORKERS}")
start = time.time()

with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = {executor.submit(fetch, i, agent): (i, agent) for i, agent in pending}
    for future in as_completed(futures):
        future.result()

save()

with open(FAILED_FILE, "w", encoding="utf-8") as f:
    json.dump(failed_urls, f, ensure_ascii=False, indent=4)

elapsed = time.time() - start
print(f"\nDone in {elapsed:.1f}s | Updated: {updated_count} | Failed: {error_count}")
print(f"Failed URLs saved to: {FAILED_FILE}")
