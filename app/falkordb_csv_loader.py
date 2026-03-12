#!/usr/bin/env python3
"""
FalkorDB folder CSV loader (nodes first, then edges)

What it does
- Scans a folder for *.csv files
- Detects "node CSVs" vs "edge CSVs" by their headers
- Loads ALL nodes first using MERGE (upsert), then loads ALL edges using MERGE
- Uses batching + UNWIND for speed, while keeping code readable

Expected CSV shapes (flexible)
Node CSV:
  - must contain: id
  - optional: labels (or label)
  - any other columns become node properties

Edge CSV:
  - must contain: source, target, type
  - optional: source_label, target_label (or source_labels/target_labels)
  - any other columns become relationship properties

Notes
- Graph name is provided via CLI: -g/--graph (required).
- Labels with characters FalkorDB doesn't like (e.g., "Software:Application") are sanitized to "Software_Application".
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

# FalkorDB client is imported lazily so `--help` works even if deps aren't installed.


# ---------------------------
# Helpers
# ---------------------------

_LABEL_SAFE_RE = re.compile(r"[^0-9A-Za-z_]")


def sanitize_label(label: str) -> str:
    """Make a label safe for Cypher. Example: 'Software:Application' -> 'Software_Application'."""
    if label is None:
        return ""
    label = label.strip()
    label = label.replace(":", "_")
    label = _LABEL_SAFE_RE.sub("_", label)
    label = re.sub(r"_+", "_", label).strip("_")
    return label or "Unknown"


def split_labels(raw: Optional[str]) -> List[str]:
    """Split a labels field into a list of sanitized labels."""
    if not raw:
        return []
    # Common separators seen in exports
    parts = re.split(r"[;|,]", str(raw))
    labels = [sanitize_label(p) for p in parts if str(p).strip()]
    # de-dup while preserving order
    seen = set()
    out = []
    for l in labels:
        if l and l not in seen:
            out.append(l)
            seen.add(l)
    return out


def chunked(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def coerce_value(v: Any) -> Any:
    """
    Best-effort conversion for CSV strings:
    - "" -> None
    - integer / float -> numeric
    - "true"/"false" -> bool
    Otherwise keep as string
    """
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    s = str(v).strip()
    if s == "":
        return None

    low = s.lower()
    if low in ("true", "false"):
        return low == "true"

    # int?
    if re.fullmatch(r"[-+]?\d+", s):
        try:
            return int(s)
        except Exception:
            return s

    # float?
    if re.fullmatch(r"[-+]?\d*\.\d+", s):
        try:
            return float(s)
        except Exception:
            return s

    return s


def read_csv_rows(path: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows: List[Dict[str, Any]] = []
        for row in reader:
            # Normalize keys + coerce values
            cleaned = {k.strip(): coerce_value(v) for k, v in row.items() if k is not None}
            rows.append(cleaned)
        return headers, rows


def is_edge_csv(headers: List[str]) -> bool:
    h = {c.lower().strip() for c in headers}
    return {"source", "target", "type"}.issubset(h)


def is_node_csv(headers: List[str]) -> bool:
    h = {c.lower().strip() for c in headers}
    return "id" in h


@dataclass(frozen=True)
class NodeGroupKey:
    labels: Tuple[str, ...]  # sanitized labels


@dataclass(frozen=True)
class EdgeGroupKey:
    source_labels: Tuple[str, ...]
    target_labels: Tuple[str, ...]
    rel_type: str  # sanitized relationship type


def sanitize_rel_type(rel_type: str) -> str:
    """Relationship types are usually uppercase; sanitize similarly to labels."""
    rel_type = (rel_type or "").strip()
    rel_type = rel_type.replace(":", "_")
    rel_type = _LABEL_SAFE_RE.sub("_", rel_type)
    rel_type = re.sub(r"_+", "_", rel_type).strip("_")
    return rel_type or "RELATED_TO"


# ---------------------------
# Loader
# ---------------------------

class FolderCSVLoader:
    def __init__(
        self,
        graph_name: str,
        host: str,
        port: int,
        username: Optional[str],
        password: Optional[str],
        csv_folder: str,
        batch_size: int,
    ) -> None:
        self.csv_folder = csv_folder
        self.batch_size = batch_size
        self.graph_name = graph_name

        try:
            try:
                from falkordb import FalkorDB as _FalkorDB
            except ImportError as e:
                raise RuntimeError(
                    "Missing dependency: 'falkordb'. Install it with: pip install falkordb"
                ) from e

            self.db = _FalkorDB(host=host, port=port, username=username, password=password)
            self.graph = self.db.select_graph(self.graph_name)
        except Exception as e:
            raise RuntimeError(f"Failed to connect to FalkorDB at {host}:{port}: {e}") from e

    def find_csv_files(self) -> List[str]:
        if not os.path.isdir(self.csv_folder):
            raise FileNotFoundError(f"CSV folder not found: {self.csv_folder}")

        files = []
        for name in sorted(os.listdir(self.csv_folder)):
            if name.lower().endswith(".csv"):
                files.append(os.path.join(self.csv_folder, name))
        return files

    def load_all(self) -> None:
        csv_files = self.find_csv_files()
        if not csv_files:
            print(f"⚠️ No CSV files found in: {self.csv_folder}")
            return

        node_files: List[str] = []
        edge_files: List[str] = []

        # Classify by headers
        for path in csv_files:
            headers, _ = read_csv_rows(path)
            if is_edge_csv(headers):
                edge_files.append(path)
            elif is_node_csv(headers):
                node_files.append(path)
            else:
                print(f"⚠️ Skipping (unrecognized CSV shape): {os.path.basename(path)}")

        print(f"Found {len(node_files)} node CSV(s) and {len(edge_files)} edge CSV(s).")

        # 1) nodes first
        for path in node_files:
            self.load_nodes_from_csv(path)

        # 2) then edges
        for path in edge_files:
            self.load_edges_from_csv(path)

        print("✅ Done.")

    def load_nodes_from_csv(self, path: str) -> None:
        name = os.path.basename(path)
        headers, rows = read_csv_rows(path)
        if not rows:
            print(f"  - {name}: empty, skipping.")
            return

        # Determine which column contains labels (if any)
        lowered = {h.lower(): h for h in headers}
        labels_col = lowered.get("labels") or lowered.get("label")

        # Group by label-set so we can keep labels static in the Cypher query
        groups: Dict[NodeGroupKey, List[Dict[str, Any]]] = {}

        for row in rows:
            node_id = row.get("id")
            if node_id is None:
                continue

            raw_labels = row.get(labels_col) if labels_col else None
            labels = split_labels(raw_labels)
            if not labels:
                # fallback: derive label from filename: nodes_<Label>.csv style, otherwise "Node"
                base = os.path.splitext(name)[0]
                derived = base.replace("nodes_", "")
                labels = split_labels(derived) or ["Node"]

            props = {k: v for k, v in row.items() if k not in ("id", labels_col) and v is not None}
            key = NodeGroupKey(labels=tuple(labels))
            groups.setdefault(key, []).append({"id": node_id, "props": props})

        print(f"🧩 Loading nodes from {name}: {len(rows)} row(s), {len(groups)} label-group(s).")

        for key, items in groups.items():
            label_str = ":" + ":".join(key.labels)
            cypher = f"""
            UNWIND $rows AS row
            MERGE (n{label_str} {{id: row.id}})
            SET n += row.props
            """
            self._run_batched(cypher, items, kind=f"nodes {label_str} ({name})")

    def load_edges_from_csv(self, path: str) -> None:
        name = os.path.basename(path)
        headers, rows = read_csv_rows(path)
        if not rows:
            print(f"  - {name}: empty, skipping.")
            return

        lowered = {h.lower(): h for h in headers}
        src_label_col = lowered.get("source_label") or lowered.get("source_labels")
        tgt_label_col = lowered.get("target_label") or lowered.get("target_labels")

        groups: Dict[EdgeGroupKey, List[Dict[str, Any]]] = {}

        for row in rows:
            src = row.get("source")
            tgt = row.get("target")
            rel_type = sanitize_rel_type(str(row.get("type") or "RELATED_TO"))

            if src is None or tgt is None:
                continue

            src_labels = split_labels(row.get(src_label_col)) if src_label_col else []
            tgt_labels = split_labels(row.get(tgt_label_col)) if tgt_label_col else []

            # If labels missing, match without label constraints (works, just less selective)
            src_labels = src_labels or []
            tgt_labels = tgt_labels or []

            props = {
                k: v
                for k, v in row.items()
                if k not in ("source", "target", "type", src_label_col, tgt_label_col) and v is not None
            }

            key = EdgeGroupKey(source_labels=tuple(src_labels), target_labels=tuple(tgt_labels), rel_type=rel_type)
            groups.setdefault(key, []).append({"source": src, "target": tgt, "props": props})

        print(f"🔗 Loading edges from {name}: {len(rows)} row(s), {len(groups)} type/label-group(s).")

        for key, items in groups.items():
            s_labels = ":" + ":".join(key.source_labels) if key.source_labels else ""
            t_labels = ":" + ":".join(key.target_labels) if key.target_labels else ""

            cypher = f"""
            UNWIND $rows AS row
            MATCH (s{s_labels} {{id: row.source}})
            MATCH (t{t_labels} {{id: row.target}})
            MERGE (s)-[r:{key.rel_type}]->(t)
            SET r += row.props
            """
            self._run_batched(cypher, items, kind=f"edges :{key.rel_type} ({name})")

    def _run_batched(self, cypher: str, rows: List[Dict[str, Any]], kind: str) -> None:
        total = len(rows)
        if total == 0:
            print(f"  ✅ Loaded 0 {kind}.")
            return

        # Use a Redis pipeline to reduce round-trips.
        # We flush the pipeline periodically to avoid accumulating very large requests.
        # Heuristic: aim to keep roughly ~10k rows in-flight.
        pipeline_size = max(1, 10_000 // max(1, self.batch_size))

        pipe = self.db.connection.pipeline(transaction=False)
        queued = 0
        chunk_start_batch = 1

        def flush(chunk_end_batch: int) -> None:
            nonlocal pipe, queued, chunk_start_batch
            if queued == 0:
                return
            try:
                pipe.execute()
            except Exception as e:
                raise RuntimeError(
                    f"Failed while loading {kind} (batches {chunk_start_batch}-{chunk_end_batch}, "
                    f"batch_size={self.batch_size}, total={total}): {e}"
                ) from e
            pipe = self.db.connection.pipeline(transaction=False)
            queued = 0
            chunk_start_batch = chunk_end_batch + 1

        for batch_num, batch in enumerate(chunked(rows, self.batch_size), start=1):
            try:
                query = self.graph._build_params_header({"rows": batch}) + cypher
                pipe.execute_command("GRAPH.QUERY", self.graph.name, query, "--compact")
                queued += 1
            except Exception as e:
                raise RuntimeError(
                    f"Failed while preparing {kind} (batch {batch_num}, batch_size={len(batch)}, total={total}): {e}"
                ) from e

            if queued >= pipeline_size:
                flush(batch_num)

        flush(batch_num)
        print(f"  ✅ Loaded {total} {kind}.")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    # Note: we intentionally reserve `-h` for host, so help is `--help` (no short flag).
    p = argparse.ArgumentParser(
        description="Load all node + edge CSVs in a folder into FalkorDB (MERGE-only).",
        add_help=False,
    )
    p.add_argument("--help", action="help", help="show this help message and exit")
    p.add_argument("csv_folder", help="Folder containing CSV files to load (required).")

    p.add_argument("-g", "--graph", dest="graph_name", required=True, help="FalkorDB graph name (required).")
    p.add_argument("-h", "--host", default="localhost", help="FalkorDB host (default: localhost)")
    p.add_argument("-p", "--port", type=int, default=6379, help="FalkorDB port (default: 6379)")
    p.add_argument("-u", "--username", default=None, help="FalkorDB username (optional)")
    p.add_argument("-a", "--password", dest="password", default=None, help="FalkorDB password (optional)")
    # Backward-compatible alias (hidden from --help)
    p.add_argument("-P", dest="password", help=argparse.SUPPRESS)
    p.add_argument("-b", "--batch-size", type=int, default=1000, help="Rows per batch (default: 1000)")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    loader = FolderCSVLoader(
        graph_name=args.graph_name,
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        csv_folder=args.csv_folder,
        batch_size=args.batch_size,
    )
    loader.load_all()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
