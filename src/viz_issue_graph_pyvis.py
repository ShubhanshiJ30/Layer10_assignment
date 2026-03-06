import json
import os
import argparse
from collections import defaultdict, deque
from pyvis.network import Network

def load_graph(path="viz/graph_data.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_index(edges):
    out_adj = defaultdict(list)
    in_adj = defaultdict(list)
    by_id = {}
    for e in edges:
        by_id[e["id"]] = e
        out_adj[e["from"]].append(e)
        in_adj[e["to"]].append(e)
    return out_adj, in_adj, by_id

def neighborhood_nodes(start_id, out_adj, in_adj, hops=1, treat_as_undirected=True, max_nodes=400):
    seen = set([start_id])
    q = deque([(start_id, 0)])

    while q:
        n, d = q.popleft()
        if d == hops:
            continue

        # outgoing
        for e in out_adj.get(n, []):
            for nxt in (e["to"],):
                if nxt not in seen:
                    seen.add(nxt)
                    if len(seen) >= max_nodes:
                        return seen
                    q.append((nxt, d + 1))

        if treat_as_undirected:
            # incoming treated as neighbors too
            for e in in_adj.get(n, []):
                for nxt in (e["from"],):
                    if nxt not in seen:
                        seen.add(nxt)
                        if len(seen) >= max_nodes:
                            return seen
                        q.append((nxt, d + 1))

    return seen

def filter_edges(edges, keep_nodes, max_edges=1200):
    kept = []
    for e in edges:
        if e["from"] in keep_nodes and e["to"] in keep_nodes:
            kept.append(e)
            if len(kept) >= max_edges:
                break
    return kept

def node_label(n):
    # short labels for readability
    lbl = n.get("label") or n.get("name") or n["id"]
    if len(lbl) > 42:
        lbl = lbl[:39] + "..."
    return lbl

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--issue", type=int, help="Issue number (kubernetes/kubernetes)")
    ap.add_argument("--pr", type=int, help="PR number (kubernetes/kubernetes)")
    ap.add_argument("--hops", type=int, default=1, help="Neighborhood hops (1 or 2 recommended)")
    ap.add_argument("--max-nodes", type=int, default=250)
    ap.add_argument("--max-edges", type=int, default=800)
    ap.add_argument("--edge-labels", action="store_true", help="Show edge labels (slower). Default off.")
    ap.add_argument("--undirected", action="store_true", help="Treat graph as undirected for neighborhood expansion.")
    args = ap.parse_args()

    if not args.issue and not args.pr:
        raise SystemExit("Provide either --issue N or --pr N")

    g = load_graph("viz/graph_data.json")
    nodes = g["nodes"]
    edges = g["edges"]

    out_adj, in_adj, _ = build_index(edges)
    nodes_by_id = {n["id"]: n for n in nodes}

    if args.issue:
        start_id = f"gh:kubernetes/kubernetes:issue:{args.issue}"
        tag = f"issue_{args.issue}"
    else:
        start_id = f"gh:kubernetes/kubernetes:pr:{args.pr}"
        tag = f"pr_{args.pr}"

    if start_id not in nodes_by_id:
        raise SystemExit(f"Start node not found: {start_id}. Check your IDs in graph_data.json.")

    keep = neighborhood_nodes(
        start_id,
        out_adj,
        in_adj,
        hops=args.hops,
        treat_as_undirected=args.undirected,
        max_nodes=args.max_nodes
    )
    kept_edges = filter_edges(edges, keep, max_edges=args.max_edges)

    # Ensure endpoints for kept edges included (in case of caps)
    for e in kept_edges:
        keep.add(e["from"])
        keep.add(e["to"])

    kept_nodes = [nodes_by_id[nid] for nid in keep if nid in nodes_by_id]

    os.makedirs("viz", exist_ok=True)
    out_html = f"viz/{tag}_h{args.hops}.html"

    # --- Build spacious pyvis graph ---
    net = Network(height="100vh", width="100%", directed=True, notebook=False, bgcolor="#ffffff")

    # Spacious physics:
    # - longer springs
    # - less gravity
    # - moderate damping
    net.barnes_hut(
        gravity=-1200,
        central_gravity=0.18,
        spring_length=240,        # ↑ makes it more spacious
        spring_strength=0.02,
        damping=0.12,
        overlap=0.2
    )

    # Add nodes
    for n in kept_nodes:
        net.add_node(
            n["id"],
            label=node_label(n),
            title=n.get("title") or n["id"],
            group=n.get("group") or "Unknown",
        )

    # Add edges (labels optional for speed + readability)
    for e in kept_edges:
        title = f'{e.get("claim_type","")} ({e.get("polarity","")})\n{e.get("asserted_at","")}\nevidence_count={e.get("evidence_count",1)}'
        if args.edge_labels:
            net.add_edge(e["from"], e["to"], label=e.get("claim_type") or "", title=title)
        else:
            net.add_edge(e["from"], e["to"], title=title)

    net.write_html(out_html, notebook=False)

    print("Wrote:", out_html)
    print(f"Subgraph: nodes={len(kept_nodes)} edges={len(kept_edges)} start={start_id}")
    print(f"Open via: http://localhost:8000/{os.path.basename(out_html)}")

if __name__ == "__main__":
    main()