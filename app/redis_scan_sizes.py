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

cursor = 0
while True:
    cursor, keys = r.scan(cursor=cursor, match=SCAN_MATCH, count=SCAN_COUNT)
    for key in keys:
        key_type = r.type(key)
        if key_type == "graphdata":
            try:
                raw = r.execute_command("GRAPH.MEMORY", "USAGE", key)
                # response: ["total_graph_sz_mb", <int>]
                size_mb = int(raw[1])
                size_kb = size_mb * 1024
            except Exception:
                size_kb = 0
        else:
            size_bytes = r.memory_usage(key) or 0
            size_kb = size_bytes / 1024

        results.append((key, size_kb, key_type))
    if cursor == 0:
        break

# Sort by size descending and take top N
results.sort(key=lambda x: x[1], reverse=True)
results = results[:TOP_N]

# ---------------------------------------------------------------------------
# Output (TSV)
# ---------------------------------------------------------------------------
print(f"\n{'KEY'}\t{'SIZE (KB)'}\t{'TYPE'}")
print("-" * 60)
for key, size_kb, key_type in results:
    print(f"{key}\t{size_kb:.2f} KB\t{key_type}")

print(f"\nTotal keys found: {len(results)} (capped at top {TOP_N} by size)")
