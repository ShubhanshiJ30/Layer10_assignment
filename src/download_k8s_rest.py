import os, json, time
import requests
from tqdm import tqdm

OWNER = "kubernetes"
REPO = "kubernetes"
BASE = "https://api.github.com"

TOKEN = os.environ.get("GITHUB_TOKEN")
if not TOKEN:
    raise SystemExit("Missing GITHUB_TOKEN env var")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

def ensure_dirs():
    os.makedirs("data/raw", exist_ok=True)

def request_json(url, params=None, accept=None, max_retries=8):
    headers = dict(HEADERS)
    if accept:
        headers["Accept"] = accept

    backoff = 1.5
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params, timeout=60)

        # transient
        if r.status_code in (502, 503, 504):
            sleep_s = backoff ** attempt
            print(f"[retry] {r.status_code} {url} sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        # rate limit
        if r.status_code == 403 and "rate limit" in r.text.lower():
            reset = r.headers.get("X-RateLimit-Reset")
            if reset:
                wait = max(30, int(reset) - int(time.time()) + 5)
            else:
                wait = 60
            print(f"[retry] rate limit sleeping {wait}s")
            time.sleep(wait)
            continue

        r.raise_for_status()
        return r.json(), r.headers

    raise SystemExit(f"Failed after retries: {url}")

def paginate(url, params=None, accept=None, max_pages=None):
    page = 1
    while True:
        p = dict(params or {})
        p["per_page"] = 100
        p["page"] = page
        data, headers = request_json(url, p, accept=accept)
        if not isinstance(data, list):
            raise SystemExit(f"Expected list from {url}, got {type(data)}")
        if not data:
            break
        yield from data
        page += 1
        if max_pages and page > max_pages:
            break

def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def main(limit_items=200):
    ensure_dirs()

    issues_url = f"{BASE}/repos/{OWNER}/{REPO}/issues"
    pulls_url  = f"{BASE}/repos/{OWNER}/{REPO}/pulls"

    # 1) Issues (REST /issues includes PRs; filter them out)
    issues = []
    print("Downloading issues...")
    for item in paginate(issues_url, params={"state":"all","sort":"updated","direction":"desc"}):
        if "pull_request" in item:
            continue
        issues.append(item)
        if len(issues) >= limit_items:
            break
    write_jsonl("data/raw/issues.jsonl", issues)
    print(f"Wrote {len(issues)} issues")

    # 2) PRs
    prs = []
    print("Downloading PRs...")
    for item in paginate(pulls_url, params={"state":"all","sort":"updated","direction":"desc"}):
        prs.append(item)
        if len(prs) >= limit_items:
            break
    write_jsonl("data/raw/prs.jsonl", prs)
    print(f"Wrote {len(prs)} PRs")

    # 3) Comments per issue/pr
    # We'll store comments per number (one jsonl file each) for easy incremental runs
    nums = [i["number"] for i in issues] + [p["number"] for p in prs]
    nums = list(dict.fromkeys(nums))  # de-dupe keep order

    os.makedirs("data/raw/comments", exist_ok=True)
    os.makedirs("data/raw/timeline", exist_ok=True)

    print("Downloading comments + timeline for each number...")
    for n in tqdm(nums, desc="items"):
        # Comments endpoint (same for issues & PRs)
        c_url = f"{BASE}/repos/{OWNER}/{REPO}/issues/{n}/comments"
        comments = list(paginate(c_url))
        with open(f"data/raw/comments/{n}.json", "w", encoding="utf-8") as f:
            json.dump(comments, f, ensure_ascii=False)

        # Timeline endpoint requires a special Accept header
        t_url = f"{BASE}/repos/{OWNER}/{REPO}/issues/{n}/timeline"
        timeline = list(paginate(t_url, accept="application/vnd.github+json"))
        with open(f"data/raw/timeline/{n}.json", "w", encoding="utf-8") as f:
            json.dump(timeline, f, ensure_ascii=False)

        time.sleep(0.2)  # gentle pacing

    print("Done.")
    print("Raw outputs:")
    print(os.path.abspath("data/raw/issues.jsonl"))
    print(os.path.abspath("data/raw/prs.jsonl"))
    print(os.path.abspath("data/raw/comments"))
    print(os.path.abspath("data/raw/timeline"))

if __name__ == "__main__":
    main(limit_items=200)