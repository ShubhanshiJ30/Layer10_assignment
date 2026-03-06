import json, os, hashlib
from config import EXTRACTION_VERSION, SCHEMA_VERSION, SYSTEM_NAME

REPO = "kubernetes/kubernetes"

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

def issue_entity(kind, number):
    t = "Issue" if kind == "issue" else "PullRequest"
    return {
        "type": t,
        "id": f"gh:{REPO}:{kind}:{number}",
        "name": f"{REPO}#{number}"
    }

def person_entity(login):
    return {
        "type": "Person",
        "id": f"gh:user:{login}",
        "name": login
    }

def label_entity(name):
    return {
        "type": "Label",
        "id": f"gh:{REPO}:label:{name}",
        "name": name
    }

def milestone_entity(title):
    return {
        "type": "Milestone",
        "id": f"gh:{REPO}:milestone:{title}",
        "name": title
    }
def obj_key(obj):
    # Entity object (has id)
    if isinstance(obj, dict) and "id" in obj:
        return obj["id"]
    # Value object (Status/value etc.) or plain types
    if isinstance(obj, dict):
        return json.dumps(obj, sort_keys=True)
    return str(obj)

def make_claim(claim_type, subject, obj, timestamp, evidence, polarity="affirmed"):
    signature = json.dumps({
        "t": claim_type,
        "s": subject["id"],
        "o": obj_key(obj),
        "p": polarity,
        "ts": timestamp
    }, sort_keys=True)

    claim_id = sha256(signature)[:24]

    return {
        "claim_id": claim_id,
        "claim_type": claim_type,
        "subject": subject,
        "object": obj,
        "polarity": polarity,
        "event_time": timestamp,
        "asserted_at": timestamp,
        "confidence": 1.0,
        "evidence": evidence,
        "extraction_version": EXTRACTION_VERSION,
        "schema_version": SCHEMA_VERSION,
        "system": SYSTEM_NAME,
        
    }

def main(run_id=None):
    claims = []

    # Build lookup of issue/pr kind by number
    kind_lookup = {}
    for art in load_jsonl("data/processed/artifacts.jsonl"):
        kind_lookup[art["number"]] = art["artifact_kind"]

    for ev in load_jsonl("data/processed/events.jsonl"):
        number = ev["number"]
        kind = kind_lookup.get(number)
        if not kind:
            continue

        subject = issue_entity(kind, number)
        timestamp = ev["created_at"]
        event_type = ev.get("event_type")
        raw = ev["raw"]
        artifact_id = ev["artifact_id"]

        if not event_type:
            continue

        # STATUS
        if event_type == "closed":
            obj = {"type": "Status", "value": "closed"}
            claims.append(make_claim(
                "has_status",
                subject,
                obj,
                timestamp,
                {
                    "artifact_id": artifact_id,
                    "json_path": "$.event",
                    "value": "closed",
                    "timestamp": timestamp
                }
            ))

        elif event_type == "reopened":
            obj = {"type": "Status", "value": "open"}
            claims.append(make_claim(
                "has_status",
                subject,
                obj,
                timestamp,
                {
                    "artifact_id": artifact_id,
                    "json_path": "$.event",
                    "value": "reopened",
                    "timestamp": timestamp
                }
            ))

        # LABELS
        elif event_type == "labeled":
            label = raw.get("label", {}).get("name")
            if label:
                claims.append(make_claim(
                    "has_label",
                    subject,
                    label_entity(label),
                    timestamp,
                    {
                        "artifact_id": artifact_id,
                        "json_path": "$.label.name",
                        "value": label,
                        "timestamp": timestamp
                    }
                ))

        elif event_type == "unlabeled":
            label = raw.get("label", {}).get("name")
            if label:
                claims.append(make_claim(
                    "has_label",
                    subject,
                    label_entity(label),
                    timestamp,
                    {
                        "artifact_id": artifact_id,
                        "json_path": "$.label.name",
                        "value": label,
                        "timestamp": timestamp
                    },
                    polarity="denied"
                ))

        # ASSIGNEES
        elif event_type == "assigned":
            login = raw.get("assignee", {}).get("login")
            if login:
                claims.append(make_claim(
                    "assigned_to",
                    subject,
                    person_entity(login),
                    timestamp,
                    {
                        "artifact_id": artifact_id,
                        "json_path": "$.assignee.login",
                        "value": login,
                        "timestamp": timestamp
                    }
                ))

        elif event_type == "unassigned":
            login = raw.get("assignee", {}).get("login")
            if login:
                claims.append(make_claim(
                    "assigned_to",
                    subject,
                    person_entity(login),
                    timestamp,
                    {
                        "artifact_id": artifact_id,
                        "json_path": "$.assignee.login",
                        "value": login,
                        "timestamp": timestamp
                    },
                    polarity="denied"
                ))

        # MILESTONE
        elif event_type == "milestoned":
            title = raw.get("milestone", {}).get("title")
            if title:
                claims.append(make_claim(
                    "in_milestone",
                    subject,
                    milestone_entity(title),
                    timestamp,
                    {
                        "artifact_id": artifact_id,
                        "json_path": "$.milestone.title",
                        "value": title,
                        "timestamp": timestamp
                    }
                ))

        elif event_type == "demilestoned":
            title = raw.get("milestone", {}).get("title")
            if title:
                claims.append(make_claim(
                    "in_milestone",
                    subject,
                    milestone_entity(title),
                    timestamp,
                    {
                        "artifact_id": artifact_id,
                        "json_path": "$.milestone.title",
                        "value": title,
                        "timestamp": timestamp
                    },
                    polarity="denied"
                ))

    write_jsonl("data/processed/claims_events.jsonl", claims)
    print("Event-derived claims:", len(claims))

if __name__ == "__main__":
    main()