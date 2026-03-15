"""
Debug: test the correct immodvisor /page endpoint with proper params.
"""
from curl_cffi import requests

PROXY = "https://customer-AHMAD_NSPT0-cc-US:Thvqz3aTY=0xiuH@pr.oxylabs.io:7777"
CID   = "74071"
HASH  = "UDZNU-MVGUQB-HSKLG-Y459-15FJ"
CTYPE = "company"

session = requests.Session(impersonate="chrome", proxy=PROXY)

# The JS builds: widgetsUrl + wType.route + '?' + queries
# For 'page' type: route='page', params include cid, ctype, hash, fp, wording, number, noStats
candidates = [
    f"https://widget3.immodvisor.com/page?cid={CID}&ctype={CTYPE}&hash={HASH}&fp=&wording=plural&number=10&noStats=false",
    f"https://widget3.immodvisor.com/page?cid={CID}&ctype={CTYPE}&hash={HASH}&fp=&wording=plural&number=10",
    f"https://widget3.immodvisor.com/page?cid={CID}&ctype={CTYPE}&hash={HASH}&number=10",
    f"https://widget3.immodvisor.com/page?cid={CID}&hash={HASH}&number=10",
    f"https://widget3.immodvisor.com/page?cid={CID}&ctype={CTYPE}&hash={HASH}",
    f"https://widget3.immodvisor.com/rating?cid={CID}&ctype={CTYPE}&hash={HASH}&fp=&wording=plural&noStats=false",
    f"https://widget3.immodvisor.com/rating?cid={CID}&ctype={CTYPE}&hash={HASH}",
]

for url in candidates:
    r = session.get(url, timeout=10)
    has_reviews = "imdw-page-nbr-reviews" in r.text
    has_rating  = "imdw-page-rating-number" in r.text
    snippet = r.text[:200].replace("\n", " ").strip()
    print(f"[{r.status_code}] reviews={has_reviews} rating={has_rating}")
    print(f"  URL    : {url}")
    print(f"  Snippet: {snippet}\n")
