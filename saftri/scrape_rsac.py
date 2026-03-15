import json
import re
import time
from curl_cffi import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

INPUT_FILE  = "saftri_final.json"
OUTPUT_FILE = "saftri_final.json"
FAILED_FILE = "rsac_failed.txt"

PROXY = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"
MAX_THREADS = 20
RETRIES = 3

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "referer": "https://www.safti.fr/",
}

write_lock = Lock()

with open(INPUT_FILE, encoding="utf-8") as f:
    agents = json.load(f)

# index by profile_url for fast lookup
index = {a["profile_url"]: i for i, a in enumerate(agents)}

# only process entries without rsac_number
todo = [a for a in agents if not a.get("rsac_number")]
print(f"Total agents: {len(agents)} | To process: {len(todo)}")

completed = 0
failed    = 0


def extract_rsac(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find(attrs={"data-testid": "minisite-agent-rsac"})
    if not tag:
        return None
    text = tag.get_text(" ", strip=True)
    # match sequence of digits separated by spaces, e.g. "987 518 925"
    m = re.search(r"\b(\d[\d ]{5,}\d)\b", text)
    return m.group(1).strip() if m else None


def fetch_rsac(agent: dict) -> tuple[str, str | None]:
    url = agent["profile_url"]
    for attempt in range(RETRIES):
        try:
            r = requests.get(
                url,
                headers=headers,
                impersonate="chrome",
                proxy=PROXY,
                timeout=30,
            )
            if r.status_code == 200:
                rsac = extract_rsac(r.text)
                return url, rsac
            print(f"  [{r.status_code}] {url}")
        except Exception as e:
            print(f"  [ERR attempt {attempt+1}] {url}: {e}")
        time.sleep(1.5)
    return url, None


def process(agent: dict):
    global completed, failed
    url, rsac = fetch_rsac(agent)
    idx = index[url]

    with write_lock:
        if rsac:
            agents[idx]["rsac_number"] = rsac
            completed += 1
            print(f"  OK [{completed}/{len(todo)}] {url} → {rsac}")
        else:
            failed += 1
            with open(FAILED_FILE, "a", encoding="utf-8") as fh:
                fh.write(url + "\n")
            print(f"  FAIL [{failed}] {url}")


with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
    futures = {pool.submit(process, a): a for a in todo}
    done = 0
    for fut in as_completed(futures):
        done += 1
        fut.result()  # surface any unexpected exception
        # save progress every 100 completions
        if done % 100 == 0:
            with write_lock:
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    json.dump(agents, f, ensure_ascii=False, indent=4)
            print(f"  [checkpoint] saved at {done}/{len(todo)}")

# final save
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(agents, f, ensure_ascii=False, indent=4)

print(f"\nDone. Completed: {completed} | Failed: {failed}")
print(f"Results saved to {OUTPUT_FILE}")
print(f"Failed URLs logged to {FAILED_FILE}")
