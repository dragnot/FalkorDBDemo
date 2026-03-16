#!/usr/bin/env python3
"""
Scan all Redis keys and print each key with its memory usage.

Usage:
    python3 redis_scan_sizes.py [--host HOST:PORT] [--password PASSWORD]

Examples:
    python3 redis_scan_sizes.py --password n28fyp9r
    python3 redis_scan_sizes.py --host myhost.com:59333 --password n28fyp9r
    REDIS_PASSWORD=secret python3 redis_scan_sizes.py
"""

import argparse
import os
import redis

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_HOST = "r-6jissuruar.instance-69n0tflcd.hc-2uaqqpjgg.us-east-2.aws.f2e0a955bb84.cloud"
DEFAULT_PORT = 59333

parser = argparse.ArgumentParser(description="Scan Redis keys and report sizes.")
parser.add_argument("--host", default=None, help="host:port (default: built-in host:59333)")
parser.add_argument("--password", default=os.environ.get("REDIS_PASSWORD", ""), help="Redis password")
parser.add_argument("--username", default=os.environ.get("REDIS_USERNAME", "falkordb"), help="Redis username")
args = parser.parse_args()

if args.host:
    _host, _, _port = args.host.partition(":")
    HOST = _host
    PORT = int(_port) if _port else 6379
else:
    HOST = DEFAULT_HOST
    PORT = DEFAULT_PORT

USERNAME = args.username
PASSWORD = args.password
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
total_kb = 0

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

        total_kb += size_kb
        results.append((key, size_kb, key_type, key_type == "graphdata"))
    if cursor == 0:
        break

# Sort by size descending and take top N
results.sort(key=lambda x: x[1], reverse=True)
results = results[:TOP_N]

# ---------------------------------------------------------------------------
# Output (TSV)
# ---------------------------------------------------------------------------
print(f"\n{'KEY'}\t{'SIZE'}\t{'TYPE'}")
print("-" * 60)
for key, size_kb, key_type, is_graph in results:
    if is_graph and size_kb >= 1024:
        size_display = f"{size_kb / 1024:.2f} MB"
    else:
        size_display = f"{size_kb:.2f} KB"
    print(f"{key}\t{size_display}\t{key_type}")

print(f"\nTotal keys shown: {len(results)} (capped at top {TOP_N} by size)")
print(f"Total size (all keys): {total_kb / 1024:.2f} MB")
