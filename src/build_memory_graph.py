import json, os
from collections import defaultdict

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def node_from_entity(ent):
    # ent is like {"type":"Issue","id":"...","name":"..."}
    return {
        "node_id": ent["id"],
        "node_type": ent.get("type"),
        "name": ent.get("name")
    }

def node_from_value(val):
    # value nodes for things like Status/value
    # use deterministic id so it behaves like a node
    node_id = f"value:{json.dumps(val, sort_keys=True)}"
    return {
        "node_id": node_id,
        "node_type": val.get("type", "Value"),
        "name": val.get("value")
    }

def object_node_id(obj):
    if isinstance(obj, dict) and "id" in obj:
        return obj["id"]
    if isinstance(obj, dict):
        return f"value:{json.dumps(obj, sort_keys=True)}"
    return f"value:{str(obj)}"

def main():
    nodes = {}
    edges = []

    for c in load_jsonl("data/processed/claims_merged.jsonl"):
        subj = c["subject"]
        obj = c["object"]

        # subject node
        sn = node_from_entity(subj)
        nodes[sn["node_id"]] = sn

        # object node (entity or value)
        if isinstance(obj, dict) and "id" in obj:
            on = node_from_entity(obj)
        elif isinstance(obj, dict):
            on = node_from_value(obj)
        else:
            on = {"node_id": f"value:{str(obj)}", "node_type": "Value", "name": str(obj)}
        nodes[on["node_id"]] = on

        # edge (property graph edge)
        edges.append({
            "edge_id": c["claim_id"],
            "edge_type": c["claim_type"],
            "source": subj["id"],
            "target": object_node_id(obj),
            "polarity": c["polarity"],
            "asserted_at": c["asserted_at"],
            "confidence": c.get("confidence", 1.0),
            "evidence": c["evidence"],
            "evidence_count": c.get("evidence_count", 1),

            # versioning
            "schema_version": c.get("schema_version"),
            "extraction_version": c.get("extraction_version"),
            "extraction_run_id": c.get("extraction_run_id"),
            "merge_run_id": c.get("merge_run_id"),
        })

    write_jsonl("data/processed/graph_nodes.jsonl", list(nodes.values()))
    write_jsonl("data/processed/graph_edges.jsonl", edges)

    print("Nodes:", len(nodes))
    print("Edges:", len(edges))

if __name__ == "__main__":
    main()