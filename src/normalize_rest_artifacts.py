import os, json, hashlib

REPO = "kubernetes/kubernetes"

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def canonical_text(title, author, created_at, body):
    title = title or ""
    body = body or ""
    return f"TITLE: {title}\nAUTHOR: {author or ''}\nCREATED_AT: {created_at or ''}\n\n{body}"

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def main():
    artifacts = []
    comments = []
    events = []

    # -------- Issues --------
    for item in load_jsonl("data/raw/issues.jsonl"):
        number = item["number"]
        node_id = item["id"]
        author = (item.get("user") or {}).get("login")
        created_at = item.get("created_at")
        updated_at = item.get("updated_at")
        title = item.get("title")
        body = item.get("body")
        url = item.get("html_url")

        artifact_id = f"gh:{REPO}:issue:{node_id}"
        text = canonical_text(title, author, created_at, body)

        artifacts.append({
            "artifact_id": artifact_id,
            "artifact_kind": "issue",
            "repo": REPO,
            "number": number,
            "author_login": author,
            "created_at": created_at,
            "updated_at": updated_at,
            "url": url,
            "title": title,
            "body": body,
            "text": text,
            "text_sha256": sha256(text),
            "raw": item
        })

    # -------- PRs --------
    for item in load_jsonl("data/raw/prs.jsonl"):
        number = item["number"]
        node_id = item["id"]
        author = (item.get("user") or {}).get("login")
        created_at = item.get("created_at")
        updated_at = item.get("updated_at")
        title = item.get("title")
        body = item.get("body")
        url = item.get("html_url")

        artifact_id = f"gh:{REPO}:pr:{node_id}"
        text = canonical_text(title, author, created_at, body)

        artifacts.append({
            "artifact_id": artifact_id,
            "artifact_kind": "pr",
            "repo": REPO,
            "number": number,
            "author_login": author,
            "created_at": created_at,
            "updated_at": updated_at,
            "url": url,
            "title": title,
            "body": body,
            "text": text,
            "text_sha256": sha256(text),
            "raw": item
        })

    # -------- Comments --------
    comments_dir = "data/raw/comments"
    for fname in os.listdir(comments_dir):
        path = os.path.join(comments_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            items = json.load(f)

        number = int(fname.replace(".json", ""))

        for c in items:
            comment_id = c["id"]
            author = (c.get("user") or {}).get("login")
            created_at = c.get("created_at")
            updated_at = c.get("updated_at")
            body = c.get("body")
            url = c.get("html_url")

            artifact_id = f"gh:{REPO}:comment:{comment_id}"
            text = canonical_text(None, author, created_at, body)

            comments.append({
                "artifact_id": artifact_id,
                "artifact_kind": "comment",
                "repo": REPO,
                "number": number,
                "author_login": author,
                "created_at": created_at,
                "updated_at": updated_at,
                "url": url,
                "body": body,
                "text": text,
                "text_sha256": sha256(text),
                "raw": c
            })

    # -------- Timeline Events --------
    timeline_dir = "data/raw/timeline"
    for fname in os.listdir(timeline_dir):
        path = os.path.join(timeline_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            items = json.load(f)

        number = int(fname.replace(".json", ""))

        for ev in items:
            event_id = sha256(json.dumps(ev, sort_keys=True))[:24]
            artifact_id = f"gh:{REPO}:event:{event_id}"

            events.append({
                "artifact_id": artifact_id,
                "artifact_kind": "event",
                "repo": REPO,
                "number": number,
                "event_type": ev.get("event"),
                "created_at": ev.get("created_at"),
                "raw": ev
            })

    write_jsonl("data/processed/artifacts.jsonl", artifacts)
    write_jsonl("data/processed/comments.jsonl", comments)
    write_jsonl("data/processed/events.jsonl", events)

    print("Wrote:")
    print("  artifacts.jsonl:", len(artifacts))
    print("  comments.jsonl:", len(comments))
    print("  events.jsonl:", len(events))

if __name__ == "__main__":
    main()