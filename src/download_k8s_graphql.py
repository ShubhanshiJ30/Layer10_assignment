import os, json, time
import requests
from tqdm import tqdm

OWNER = "kubernetes"
REPO = "kubernetes"
API = "https://api.github.com/graphql"

TOKEN = os.environ.get("GITHUB_TOKEN")
if not TOKEN:
    raise SystemExit("Missing GITHUB_TOKEN env var")

HEADERS = {"Authorization": f"Bearer {TOKEN}"}

QUERY = """
query($owner:String!, $repo:String!, $cursor:String) {
  repository(owner:$owner, name:$repo) {
    issues(first: 50, after: $cursor, states: [OPEN, CLOSED], orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        __typename
        id
        number
        title
        body
        url
        createdAt
        updatedAt
        closedAt
        author { login }
        labels(first: 50) { nodes { name } }
        milestone { title }
        assignees(first: 20) { nodes { login } }

        comments(first: 50) {
          nodes {
            id
            author { login }
            createdAt
            updatedAt
            body
            url
          }
        }

        timelineItems(first: 100) {
          nodes {
            __typename
            ... on ClosedEvent { createdAt actor { login } }
            ... on ReopenedEvent { createdAt actor { login } }
            ... on LabeledEvent { createdAt actor { login } label { name } }
            ... on UnlabeledEvent { createdAt actor { login } label { name } }
            ... on AssignedEvent { createdAt actor { login } assignee { ... on User { login } } }
            ... on UnassignedEvent { createdAt actor { login } assignee { ... on User { login } } }
            ... on MilestonedEvent { createdAt actor { login } milestoneTitle }
            ... on DemilestonedEvent { createdAt actor { login } milestoneTitle }
          }
        }
      }
    }

    pullRequests(first: 50, after: $cursor, states: [OPEN, CLOSED, MERGED], orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        __typename
        id
        number
        title
        body
        url
        createdAt
        updatedAt
        closedAt
        mergedAt
        author { login }
        labels(first: 50) { nodes { name } }
        milestone { title }
        assignees(first: 20) { nodes { login } }

        comments(first: 50) {
          nodes {
            id
            author { login }
            createdAt
            updatedAt
            body
            url
          }
        }

        timelineItems(first: 100) {
          nodes {
            __typename
            ... on ClosedEvent { createdAt actor { login } }
            ... on ReopenedEvent { createdAt actor { login } }
            ... on MergedEvent { createdAt actor { login } }
            ... on LabeledEvent { createdAt actor { login } label { name } }
            ... on UnlabeledEvent { createdAt actor { login } label { name } }
            ... on AssignedEvent { createdAt actor { login } assignee { ... on User { login } } }
            ... on UnassignedEvent { createdAt actor { login } assignee { ... on User { login } } }
            ... on MilestonedEvent { createdAt actor { login } milestoneTitle }
            ... on DemilestonedEvent { createdAt actor { login } milestoneTitle }
          }
        }
      }
    }
  }
}
"""

def gql(payload, max_retries=8):
    backoff = 1.5
    for attempt in range(max_retries):
        r = requests.post(API, headers=HEADERS, json=payload, timeout=60)

        # Retry on transient server errors
        if r.status_code in (502, 503, 504):
            sleep_s = backoff ** attempt
            print(f"[retry] HTTP {r.status_code} from GitHub GraphQL. sleeping {sleep_s:.1f}s (attempt {attempt+1}/{max_retries})")
            time.sleep(sleep_s)
            continue

        # Handle rate limit (rare on GraphQL, but possible)
        if r.status_code == 403 and "rate limit" in r.text.lower():
            sleep_s = max(60, backoff ** attempt)
            print(f"[retry] Rate limit. sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        # For other errors, raise
        r.raise_for_status()

        j = r.json()
        if "errors" in j:
            # Sometimes GitHub returns errors for abuse/rate limiting
            msg = str(j["errors"])
            if "something went wrong" in msg.lower() or "timeout" in msg.lower():
                sleep_s = backoff ** attempt
                print(f"[retry] GraphQL errors look transient. sleeping {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise SystemExit(f"GraphQL errors: {j['errors']}")

        return j["data"]

    raise SystemExit("GraphQL failed after retries.")
def dump_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

def main(max_items=300):
    os.makedirs("data/raw", exist_ok=True)
    print("CWD:", os.getcwd())
    print("Writing to:", os.path.abspath("data/raw"))

    issues = []
    prs = []

    cursor = None
    pbar = tqdm(total=max_items, desc="issues")
    while len(issues) < max_items:
        data = gql({"query": QUERY, "variables": {"owner": OWNER, "repo": REPO, "cursor": cursor}})
        block = data["repository"]["issues"]
        nodes = block["nodes"]
        if not nodes:
            break
        for n in nodes:
            issues.append(n)
            pbar.update(1)
            if len(issues) >= max_items:
                break
        if not block["pageInfo"]["hasNextPage"]:
            break
        cursor = block["pageInfo"]["endCursor"]
        time.sleep(0.2)
    pbar.close()

    cursor = None
    pbar = tqdm(total=max_items, desc="prs")
    while len(prs) < max_items:
        data = gql({"query": QUERY, "variables": {"owner": OWNER, "repo": REPO, "cursor": cursor}})
        block = data["repository"]["pullRequests"]
        nodes = block["nodes"]
        if not nodes:
            break
        for n in nodes:
            prs.append(n)
            pbar.update(1)
            if len(prs) >= max_items:
                break
        if not block["pageInfo"]["hasNextPage"]:
            break
        cursor = block["pageInfo"]["endCursor"]
        time.sleep(0.2)
    pbar.close()

    dump_jsonl("data/raw/issues.jsonl", issues)
    dump_jsonl("data/raw/prs.jsonl", prs)

    print("WROTE:", os.path.abspath("data/raw/issues.jsonl"))
    print("WROTE:", os.path.abspath("data/raw/prs.jsonl"))

if __name__ == "__main__":
    main()
