import sys
from pathlib import Path

# Ensure repo root is on path
repo_root = Path(r"c:/Users/Dell/Documents/stage/mzcode").resolve()
sys.path.insert(0, str(repo_root))

from metazcode.sdk.ingestion.plsql import PlsqlLoader


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/run_plsql_loader.py <folder_with_plsql>")
        # Prefer the repo at workspace root if present, else fallback to temp
        candidate = repo_root / 'Water-Quality-DW-on-Oracle-Database'
        if not candidate.exists():
            candidate = repo_root / 'temp' / 'Water-Quality-DW-on-Oracle-Database'
        print(f"Defaulting to: {candidate}")
        target = candidate
    else:
        target = Path(sys.argv[1]).resolve()

    loader = PlsqlLoader(root_path=str(target))
    total_nodes = 0
    total_edges = 0
    for nodes, edges in loader.ingest():
        print(f"--- Batch ---  nodes={len(nodes)} edges={len(edges)}")
        for n in nodes:
            print(f"NODE [{n.node_type}] {n.node_id} :: {n.name}")
        for e in edges:
            print(f"EDGE [{e.relation}] {e.source_id} -> {e.target_id}")
        total_nodes += len(nodes)
        total_edges += len(edges)
    print('Total nodes:', total_nodes)
    print('Total edges:', total_edges)


if __name__ == '__main__':
    main()
