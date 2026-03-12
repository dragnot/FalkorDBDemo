#!/usr/bin/env python3
"""
Load the family-tree CSV into FalkorDB as Person nodes.

Reads the CSV locally and pushes rows via UNWIND+CREATE so the script
works correctly against a remote FalkorDB instance (file:// URIs are
only accessible to the server process, not the client).

Source file: data/development-evoca-customer-2_family_tree.csv

Usage:
    python load_family_tree.py [--host HOST] [--port PORT] [--graph GRAPH]

Environment variables (override defaults):
    FALKOR_HOST   FalkorDB host  (default: localhost)
    FALKOR_PORT   FalkorDB port  (default: 6379)
    GRAPH_NAME    Graph name     (default: family_tree)
    FALKOR_USER   FalkorDB username
    FALKOR_PASS   FalkorDB password
"""

import argparse
import csv
import os
import pathlib
from typing import List, Dict

from falkordb import FalkorDB

BATCH_SIZE = 500  # rows per UNWIND query

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_HOST = os.getenv("FALKOR_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("FALKOR_PORT", "6379"))
DEFAULT_GRAPH = os.getenv("GRAPH_NAME", "family_tree")
DEFAULT_USER = os.getenv("FALKOR_USER", None)
DEFAULT_PASS = os.getenv("FALKOR_PASS", None)

# Absolute path to the CSV file relative to this script's location
_HERE = pathlib.Path(__file__).parent
CSV_PATH = (_HERE.parent / "data" / "development-evoca-customer-2_family_tree.csv").resolve()

# Derive user_id from the CSV filename (strip the _family_tree.csv suffix)
user_id = CSV_PATH.stem.replace("_family_tree", "")   # → "development-evoca-customer-2"


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def _read_csv(path: pathlib.Path) -> List[Dict[str, str]]:
    """Read all rows from the CSV and return them as a list of dicts."""
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _batch_insert(graph, rows: List[Dict[str, str]]) -> int:
    """Insert rows in batches using UNWIND+CREATE. Returns total nodes created."""
    total_created = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        query = """
        UNWIND $rows AS row
        CREATE (n:Person { id: row.id })
        SET n = row
        """
        result = graph.query(query, {"rows": batch})
        total_created += result.nodes_created
    return total_created


def load(host: str, port: int, graph_name: str, username: str = None, password: str = None) -> None:
    # Step 1: Read CSV locally
    print(f"📂 Reading CSV from: {CSV_PATH}")
    try:
        rows = _read_csv(CSV_PATH)
        print(f"📋 {len(rows)} rows read from CSV")
    except Exception as e:
        print(f"❌ Failed to read CSV: {type(e).__name__} - {e}")
        return

    # Step 2: Connect to FalkorDB
    print(f"🔌 Connecting to FalkorDB at {host}:{port} …")
    db = FalkorDB(host=host, port=port, username=username, password=password)
    graph = db.select_graph(graph_name)
    print(f"✅ Connected – using graph '{graph_name}'")

    # Step 3: Push rows via UNWIND+CREATE
    try:
        print(f"🚀 Loading {len(rows)} rows into graph '{graph_name}' (batch size: {BATCH_SIZE}) …")
        nodes_created = _batch_insert(graph, rows)
        print(f"✅ Loaded {nodes_created} PERSON nodes from CSV for customer {user_id}")
    except Exception as e:
        print(f"❌ Unexpected Python Error: {type(e).__name__} - {e}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load family-tree CSV into FalkorDB.")
    p.add_argument("--host", default=DEFAULT_HOST, help=f"FalkorDB host (default: {DEFAULT_HOST})")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"FalkorDB port (default: {DEFAULT_PORT})")
    p.add_argument("--graph", default=DEFAULT_GRAPH, help=f"Graph name (default: {DEFAULT_GRAPH})")
    p.add_argument("--username", default=DEFAULT_USER, help="FalkorDB username (or set FALKOR_USER)")
    p.add_argument("--password", default=DEFAULT_PASS, help="FalkorDB password (or set FALKOR_PASS)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    load(host=args.host, port=args.port, graph_name=args.graph, username=args.username, password=args.password)
