"""
Re-check reviews for agents with zero or empty number_of_reviews.
- Uses headless Patchright (sync) so JS-rendered content is available
- Extracts review count and rating from consultant-card or footer
- Updates number_of_reviews and average_rating if found
- Checkpoints every SAVE_EVERY completions
- Records failed URLs to recheck_reviews_failed.txt
"""

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from patchright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ── config ────────────────────────────────────────────────────────────────────

FINAL_FILE  = "efficity_final.json"
FAILED_FILE = "recheck_reviews_failed.txt"

PROXY_SERVER = "https://pr.oxylabs.io:7777"
PROXY_USER   = "customer-AHMAD_NSPT0-cc-US"
PROXY_PASS   = "Thvqz3aTY=0xiuH"

WORKERS    = 5
RETRIES    = 3
SAVE_EVERY = 50

# ── shared state ──────────────────────────────────────────────────────────────

lock         = threading.Lock()
completed    = 0
failed_count = 0

# ── helpers ───────────────────────────────────────────────────────────────────

def extract_reviews(html):
    """
    Tries consultant-card first, then falls back to footer.

    consultant-card:
      <em class="ml-2">4.9</em> inside p.rating
      <a class="card-immodvisor-link">out of 29282 recommendations</a>

    footer:
      <div class="number-stars">4.9/5</div>
      <em>based on 29282 reviews on Immodvisor</em>
    """
    soup   = BeautifulSoup(html, "html.parser")
    rating = None
    count  = None

    # consultant-card
    card = soup.find("div", class_="consultant-card")
    if card:
        p_rating = card.find("p", class_="rating")
        if p_rating:
            em = p_rating.find("em")
            if em:
                try:
                    rating = float(em.get_text(strip=True).replace(",", "."))
                except ValueError:
                    pass

        link = card.find("a", class_="card-immodvisor-link")
        if link:
            text = link.get_text(" ", strip=True)
            m = re.search(r"(\d[\d\s,]*)\s+recommendation", text, re.IGNORECASE)
            if m:
                count = int(re.sub(r"[\s,]", "", m.group(1)))

    # footer fallback
    if rating is None:
        stars_div = soup.find("div", class_="number-stars")
        if stars_div:
            m = re.search(r"([\d,.]+)\s*/\s*5", stars_div.get_text(strip=True))
            if m:
                try:
                    rating = float(m.group(1).replace(",", "."))
                except ValueError:
                    pass

    if count is None:
        for em in soup.find_all("em"):
            text = em.get_text(" ", strip=True)
            m = re.search(r"(\d[\d\s,]*)\s+review", text, re.IGNORECASE)
            if m:
                count = int(re.sub(r"[\s,]", "", m.group(1)))
                break

    return count, rating


def _save():
    with open(FINAL_FILE, "w", encoding="utf-8") as f:
        json.dump(agents, f, ensure_ascii=False, indent=4)
    print(f"  -> checkpoint saved ({len(agents)} agents)")


# ── worker (one browser context per thread) ───────────────────────────────────

def process(idx, agent):
    global completed, failed_count

    profile_url = agent.get("profile_url", "")
    html        = None

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            proxy={
                "server":   PROXY_SERVER,
                "username": PROXY_USER,
                "password": PROXY_PASS,
            },
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        )

        for attempt in range(RETRIES):
            page = context.new_page()
            try:
                page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_selector(
                        "div.consultant-card, div.number-stars",
                        timeout=8000,
                    )
                except Exception:
                    pass
                html = page.content()
                page.close()
                break
            except Exception as e:
                page.close()
                print(f"  [ERR attempt {attempt+1}] {profile_url}: {e}")

        browser.close()

    with lock:
        if html:
            count, rating = extract_reviews(html)
            if count is not None:
                agents[idx]["number_of_reviews"] = count
            if rating is not None:
                agents[idx]["average_rating"] = rating
            completed += 1
            print(f"  OK [{completed}] {profile_url} -> reviews: {count} | rating: {rating}")
            if completed % SAVE_EVERY == 0:
                _save()
        else:
            failed_count += 1
            with open(FAILED_FILE, "a", encoding="utf-8") as fh:
                fh.write(profile_url + "\n")
            print(f"  FAIL [{failed_count}] {profile_url}")


# ── main ──────────────────────────────────────────────────────────────────────

with open(FINAL_FILE, encoding="utf-8") as f:
    agents = json.load(f)

todo = [
    (i, a) for i, a in enumerate(agents)
    if not a.get("number_of_reviews")
]

print(f"Total agents: {len(agents)} | Zero/empty reviews: {len(todo)}")

with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = {pool.submit(process, i, a): i for i, a in todo}
    for fut in as_completed(futures):
        fut.result()

_save()
print(f"\nDone. Processed: {completed} | Failed: {failed_count}")
