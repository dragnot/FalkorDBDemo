#!/usr/bin/env python3
"""
Scan all Redis keys and print each key with its memory usage.

Usage:
    REDIS_PASSWORD=secret python redis_scan_sizes.py
"""

import os
import redis

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HOST     = "r-6jissuruar.instance-69n0tflcd.hc-2uaqqpjgg.us-east-2.aws.f2e0a955bb84.cloud"
PORT     = 59333
USERNAME = "falkordb"
PASSWORD = os.environ.get("REDIS_PASSWORD", "")
SSL      = False

SCAN_COUNT = 1000
SCAN_MATCH = "*"
TOP_N      = 200

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
r = redis.Redis(
    host=HOST,
    port=PORT,
    username=USERNAME,
    password=PASSWORD,
    ssl=SSL,
    decode_responses=True,
)

# ---------------------------------------------------------------------------
# Scan & collect sizes
# ---------------------------------------------------------------------------
print("Scanning keys...")
results = []
graph_keys = []

cursor = 0
while True:
    cursor, keys = r.scan(cursor=cursor, match=SCAN_MATCH, count=SCAN_COUNT)
    for key in keys:
        size = r.memory_usage(key) or 0
        key_type = r.type(key)
        results.append((key, size, key_type))
        if key_type == "graphdata":
            graph_keys.append(key)
    if cursor == 0:
        break

# Sort by size descending and take top N
results.sort(key=lambda x: x[1], reverse=True)
results = results[:TOP_N]

# ---------------------------------------------------------------------------
# Output (TSV)
# ---------------------------------------------------------------------------
print(f"\n{'KEY'}\t{'SIZE (bytes)'}\t{'TYPE'}")
print("-" * 60)
for key, size, key_type in results:
    print(f"{key}\t{size}\t{key_type}")

print(f"\nTotal keys found: {len(results)} (capped at top {TOP_N} by size)")

# ---------------------------------------------------------------------------
# Graph memory details
# ---------------------------------------------------------------------------
if graph_keys:
    print(f"\n{'='*60}")
    print(f"GRAPH MEMORY DETAILS ({len(graph_keys)} graph key(s))")
    print(f"{'='*60}")
    for key in graph_keys:
        print(f"\nGraph: {key}")
        try:
            node_res  = r.execute_command("GRAPH.QUERY", key, "MATCH (n) RETURN count(n) AS nodes", "--compact")
            edge_res  = r.execute_command("GRAPH.QUERY", key, "MATCH ()-[e]->() RETURN count(e) AS edges", "--compact")
            redis_mem = r.memory_usage(key) or 0
            # compact format: [header, [[type, value], ...], stats] — value is at [1][0][0][1]
            node_count = node_res[1][0][0][1]
            edge_count = edge_res[1][0][0][1]
            print(f"  Redis key memory : {redis_mem} bytes")
            print(f"  Nodes            : {node_count}")
            print(f"  Edges            : {edge_count}")
        except Exception as e:
            print(f"  Error querying graph: {type(e).__name__} - {e}")
