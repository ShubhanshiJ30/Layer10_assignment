import json, os
from collections import defaultdict
from datetime import datetime
from config import SYSTEM_NAME, SCHEMA_VERSION, EXTRACTION_VERSION

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def parse_time(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z","+00:00"))

def main():
    claims = list(load_jsonl("data/processed/claims_merged.jsonl"))

    # Group by subject
    by_subject = defaultdict(list)
    for c in claims:
        by_subject[c["subject"]["id"]].append(c)

    current_states = []

    for subject_id, subject_claims in by_subject.items():

        # sort chronologically
        subject_claims.sort(key=lambda c: parse_time(c["asserted_at"]) or datetime.min)

        state = {
            "subject_id": subject_id,
            "subject_type": subject_claims[0]["subject"]["type"],
            "status": None,
            "labels": set(),
            "assignees": set(),
            "milestone": None,
            "system": SYSTEM_NAME,
            "schema_version": SCHEMA_VERSION,
            "extraction_version": EXTRACTION_VERSION,
        }

        for c in subject_claims:
            typ = c["claim_type"]
            polarity = c["polarity"]
            obj = c["object"]

            if typ == "has_status":
                if polarity == "affirmed":
                    state["status"] = obj["value"]

            elif typ == "has_label":
                label = obj["name"]
                if polarity == "affirmed":
                    state["labels"].add(label)
                else:
                    state["labels"].discard(label)

            elif typ == "assigned_to":
                user = obj["name"]
                if polarity == "affirmed":
                    state["assignees"].add(user)
                else:
                    state["assignees"].discard(user)

            elif typ == "in_milestone":
                if polarity == "affirmed":
                    state["milestone"] = obj["name"]
                else:
                    state["milestone"] = None

        # convert sets to lists
        state["labels"] = sorted(list(state["labels"]))
        state["assignees"] = sorted(list(state["assignees"]))

        current_states.append(state)

    write_jsonl("data/processed/current_state.jsonl", current_states)

    print("Subjects:", len(current_states))

if __name__ == "__main__":
    main()