import json, os, hashlib
from collections import defaultdict
from datetime import datetime
import uuid

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

def claim_key(c):
    obj = c["object"]
    if isinstance(obj, dict) and "id" in obj:
        obj_key = obj["id"]
    else:
        obj_key = json.dumps(obj, sort_keys=True)

    return (
        c["claim_type"],
        c["subject"]["id"],
        obj_key,
        c["polarity"]
    )

def main(run_id=None):
    merge_run_id = run_id or f"merge_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"

    all_claims = list(load_jsonl("data/processed/claims_events.jsonl")) + \
                 list(load_jsonl("data/processed/claims_text.jsonl"))

    merged = {}
    evidence_map = defaultdict(list)

    for c in all_claims:
        key = claim_key(c)
        if key not in merged:
            merged[key] = c.copy()
            merged[key]["evidence"] = []
            merged[key]["confidence"] = c.get("confidence", 1.0)

        evidence_map[key].append(c["evidence"])

    final_claims = []
    for key, claim in merged.items():
        claim["evidence"] = evidence_map[key]
        claim["evidence_count"] = len(evidence_map[key])

        # record that this file is the output of a merge step
        claim["merge_run_id"] = merge_run_id

        # keep original extraction fields (from first claim), but also stamp system/schema
        claim["system"] = claim.get("system", SYSTEM_NAME)
        claim["schema_version"] = claim.get("schema_version", SCHEMA_VERSION)
        claim["extraction_version"] = claim.get("extraction_version", EXTRACTION_VERSION)

        final_claims.append(claim)

    write_jsonl("data/processed/claims_merged.jsonl", final_claims)
    print("Original claims:", len(all_claims))
    print("Merged claims:", len(final_claims))
    print("merge_run_id:", merge_run_id)

if __name__ == "__main__":
    main()