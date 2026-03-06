import json, os, re, hashlib
from config import EXTRACTION_VERSION, SCHEMA_VERSION, SYSTEM_NAME

REPO = "kubernetes/kubernetes"

MENTION_RE = re.compile(r"@([A-Za-z0-9-]+)")
ISSUE_REF_RE = re.compile(r"#(\d+)")
FIX_RE = re.compile(r"\b(fixes|fix|closes|close|resolves|resolve)\s+#(\d+)", re.IGNORECASE)

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def person_entity(login):
    return {
        "type": "Person",
        "id": f"gh:user:{login}",
        "name": login
    }

def issue_entity(kind, number):
    t = "Issue" if kind == "issue" else "PullRequest"
    return {
        "type": t,
        "id": f"gh:{REPO}:{kind}:{number}",
        "name": f"{REPO}#{number}"
    }

def make_claim(claim_type, subject, obj, art_id, ts, text, start, end):
    quote = text[start:end]

    signature = json.dumps({
        "t": claim_type,
        "s": subject["id"],
        "o": obj["id"],
        "a": art_id,
        "st": start,
        "en": end
    }, sort_keys=True)

    claim_id = sha256(signature)[:24]

    return {
        "claim_id": claim_id,
        "claim_type": claim_type,
        "subject": subject,
        "object": obj,
        "polarity": "affirmed",
        "event_time": None,
        "asserted_at": ts,
        "confidence": 1.0,
        "evidence": {
            "artifact_id": art_id,
            "quote": quote,
            "start_offset": start,
            "end_offset": end,
            "timestamp": ts
        },
        "extraction_version": EXTRACTION_VERSION,
        "schema_version": SCHEMA_VERSION,
        "system": SYSTEM_NAME,
    }

def main(run_id=None):
    claims = []

    # Combine issues/PRs + comments
    artifacts = list(load_jsonl("data/processed/artifacts.jsonl")) + \
                list(load_jsonl("data/processed/comments.jsonl"))

    for art in artifacts:
        kind = art["artifact_kind"]
        number = art["number"]
        text = art.get("text") or ""
        ts = art.get("created_at")
        art_id = art["artifact_id"]

        subject = issue_entity(kind if kind in ("issue","pr") else art.get("parent_kind","issue"), number)

        # @mentions
        for m in MENTION_RE.finditer(text):
            login = m.group(1)
            claims.append(make_claim(
                "mentions",
                subject,
                person_entity(login),
                art_id,
                ts,
                text,
                m.start(),
                m.end()
            ))

        # Strong fix/close references
        for m in FIX_RE.finditer(text):
            num = int(m.group(2))
            claims.append(make_claim(
                "references",
                subject,
                issue_entity("issue", num),
                art_id,
                ts,
                text,
                m.start(),
                m.end()
            ))

        # Generic #123 references
        for m in ISSUE_REF_RE.finditer(text):
            num = int(m.group(1))
            claims.append(make_claim(
                "references",
                subject,
                issue_entity("issue", num),
                art_id,
                ts,
                text,
                m.start(),
                m.end()
            ))

    write_jsonl("data/processed/claims_text.jsonl", claims)
    print("Text-derived claims:", len(claims))

if __name__ == "__main__":
    main()