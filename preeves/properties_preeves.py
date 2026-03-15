# agent properties https://www.proprietes-privees.com/api/trades/mandatary/10752
# per page agents https://www.proprietes-privees.com/api/mandataries?page=2
#profile url style https://www.proprietes-privees.com/negociateur/william.fitoussi firstname.lastname

import json
import time
from curl_cffi import requests

BASE_URL = "https://www.proprietes-privees.com/api/mandataries"
PROXY = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"

PARAMS = {
}

headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,ar;q=0.8",
    "cache-control": "no-cache",
    "referer": "https://www.proprietes-privees.com/negociateur",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

TOTAL_PAGES = 157

session = requests.Session(impersonate="chrome")

agents = []
errors = []

def fetch(page):

    params = PARAMS.copy()
    params["page"] = page

    try:

        r = session.get(
            BASE_URL,
            params=params,
            headers=headers,
            timeout=30,
            proxy=PROXY
        )

        if r.status_code == 403:
            print(f"403 page {page}")
            errors.append({"page": page, "status": 403})
            return None

        if r.status_code != 200:
            print(f"Error {r.status_code} page {page}")
            errors.append({"page": page, "status": r.status_code})
            return None
        return r.json()

    except Exception as e:

        print(f"Exception page {page}: {e}")
        errors.append({"page": page, "error": str(e)})
        return None


for page in range(1, TOTAL_PAGES + 1):

    print(f"Page {page}/{TOTAL_PAGES}")

    data = fetch(page)

    if data and "data" in data and "data" in data["data"]:
        agents.extend(data["data"]["data"])

    time.sleep(0.5)


with open("preeves_agents.json", "w", encoding="utf-8") as f:
    json.dump(agents, f, ensure_ascii=False, indent=2)


with open("bsk_error_pages.txt", "w") as f:

    for e in errors:
        f.write(json.dumps(e) + "\n")


print("Finished")
print("Agents:", len(agents))
print("Errors:", len(errors))