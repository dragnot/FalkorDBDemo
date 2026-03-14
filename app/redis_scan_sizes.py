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
        size = r.memory_usage(key) or 0
        results.append((key, size))
    if cursor == 0:
        break

# Sort by size descending and take top N
results.sort(key=lambda x: x[1], reverse=True)
results = results[:TOP_N]

# ---------------------------------------------------------------------------
# Output (TSV)
# ---------------------------------------------------------------------------
print(f"\n{'KEY'}\t{'SIZE (bytes)'}")
print("-" * 60)
for key, size in results:
    print(f"{key}\t{size}")

print(f"\nTotal keys found: {len(results)} (capped at top {TOP_N} by size)")
