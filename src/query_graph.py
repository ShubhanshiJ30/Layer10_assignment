import json
import argparse

def load_jsonl(path):
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def query_by_subject(subject_id):
    results = []
    for c in load_jsonl("data/processed/claims_merged.jsonl"):
        if c["subject"]["id"] == subject_id:
            results.append(c)
    return results

def query_by_issue_number(number):
    subject_id = f"gh:kubernetes/kubernetes:issue:{number}"
    return query_by_subject(subject_id)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--issue", type=int)
    args = parser.parse_args()

    if args.issue:
        results = query_by_issue_number(args.issue)

        print(f"Found {len(results)} claims\n")

        for c in results:
            print("Claim Type:", c["claim_type"])
            print("Object:", c["object"])
            print("Polarity:", c["polarity"])
            print("Evidence Count:", c.get("evidence_count",1))
            print("---")
    else:
        print("Provide --issue <number>")

if __name__ == "__main__":
    main()