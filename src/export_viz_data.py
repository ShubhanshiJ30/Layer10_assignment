import json, os
from datetime import datetime

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def iso_to_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def main():
    os.makedirs("viz", exist_ok=True)

    # Use explicit node/edge tables if present; otherwise derive from claims_merged.
    nodes_path = "data/processed/graph_nodes.jsonl"
    edges_path = "data/processed/graph_edges.jsonl"

    if os.path.exists(nodes_path) and os.path.exists(edges_path):
        nodes = list(load_jsonl(nodes_path))
        edges = list(load_jsonl(edges_path))
        # Normalize to vis-network format
        vis_nodes = []
        for n in nodes:
            vis_nodes.append({
                "id": n["node_id"],
                "label": n.get("name") or n["node_id"],
                "group": n.get("node_type") or "Unknown",
                "title": f'{n.get("node_type","")}<br>{n["node_id"]}'
            })

        vis_edges = []
        for e in edges:
            vis_edges.append({
                "id": e["edge_id"],
                "from": e["source"],
                "to": e["target"],
                "label": e.get("edge_type",""),
                "arrows": "to",
                "claim_type": e.get("edge_type",""),
                "polarity": e.get("polarity","affirmed"),
                "asserted_at": e.get("asserted_at"),
                "confidence": e.get("confidence", 1.0),
                "evidence": e.get("evidence", []),
                "evidence_count": e.get("evidence_count", 1),
                "schema_version": e.get("schema_version"),
                "extraction_version": e.get("extraction_version"),
                "extraction_run_id": e.get("extraction_run_id"),
                "merge_run_id": e.get("merge_run_id"),
            })

    else:
        # Fallback: build from claims_merged.jsonl
        vis_nodes = {}
        vis_edges = []
        for c in load_jsonl("data/processed/claims_merged.jsonl"):
            s = c["subject"]
            o = c["object"]

            vis_nodes[s["id"]] = {
                "id": s["id"],
                "label": s.get("name") or s["id"],
                "group": s.get("type") or "Unknown",
                "title": f'{s.get("type","")}<br>{s["id"]}'
            }

            if isinstance(o, dict) and "id" in o:
                oid = o["id"]
                vis_nodes[oid] = {
                    "id": oid,
                    "label": o.get("name") or oid,
                    "group": o.get("type") or "Unknown",
                    "title": f'{o.get("type","")}<br>{oid}'
                }
            else:
                oid = "value:" + json.dumps(o, sort_keys=True)
                vis_nodes[oid] = {
                    "id": oid,
                    "label": (o.get("value") if isinstance(o, dict) else str(o))[:40],
                    "group": (o.get("type") if isinstance(o, dict) else "Value"),
                    "title": f'Value<br>{oid}'
                }

            vis_edges.append({
                "id": c["claim_id"],
                "from": s["id"],
                "to": oid,
                "label": c["claim_type"],
                "arrows": "to",
                "claim_type": c["claim_type"],
                "polarity": c["polarity"],
                "asserted_at": c.get("asserted_at"),
                "confidence": c.get("confidence", 1.0),
                "evidence": c.get("evidence", []),
                "evidence_count": c.get("evidence_count", 1),
                "schema_version": c.get("schema_version"),
                "extraction_version": c.get("extraction_version"),
                "extraction_run_id": c.get("extraction_run_id"),
                "merge_run_id": c.get("merge_run_id"),
            })

        vis_nodes = list(vis_nodes.values())

    # Compute min/max time for UI defaults
    times = []
    for e in vis_edges:
        dt = iso_to_dt(e.get("asserted_at"))
        if dt:
            times.append(dt)
    min_time = min(times).isoformat() if times else None
    max_time = max(times).isoformat() if times else None

    out = {
        "meta": {
            "min_time": min_time,
            "max_time": max_time,
            "nodes": len(vis_nodes),
            "edges": len(vis_edges),
        },
        "nodes": vis_nodes,
        "edges": vis_edges,
    }

    with open("viz/graph_data.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    print("Wrote viz/graph_data.json")
    print("Nodes:", out["meta"]["nodes"], "Edges:", out["meta"]["edges"])
    print("Time range:", out["meta"]["min_time"], "→", out["meta"]["max_time"])

if __name__ == "__main__":
    main()