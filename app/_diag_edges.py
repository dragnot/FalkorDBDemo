#!/usr/bin/env python3
"""Diagnose why edges_connects.csv and edges_instance.csv don't load."""

import csv
import re
import sys

# ── same helpers as the real loader ──────────────────────────────────────────
_LABEL_SAFE_RE = re.compile(r"[^0-9A-Za-z_]")


def sanitize_label(label):
    if label is None:
        return ""
    label = str(label).strip().replace(":", "_")
    label = _LABEL_SAFE_RE.sub("_", label)
    label = re.sub(r"_+", "_", label).strip("_")
    return label or "Unknown"


def split_labels(raw):
    if not raw:
        return []
    parts = re.split(r"[;|,]", str(raw))
    seen, out = set(), []
    for p in parts:
        lg = sanitize_label(p)
        if lg and lg not in seen:
            out.append(lg)
            seen.add(lg)
    return out


def coerce_value(v):
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    s = str(v).strip()
    if s == "":
        return None
    if s.lower() in ("true", "false"):
        return s.lower() == "true"
    if re.fullmatch(r"[-+]?\d+", s):
        try:
            return int(s)
        except Exception:
            return s
    if re.fullmatch(r"[-+]?\d*\.\d+", s):
        try:
            return float(s)
        except Exception:
            return s
    return s


def read_csv_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = []
        for row in reader:
            cleaned = {k.strip(): coerce_value(v) for k, v in row.items() if k is not None}
            rows.append(cleaned)
        return headers, rows


# ── stringify (mimic FalkorDB helper) ────────────────────────────────────────
def stringify_param_value(value):
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    if value is None:
        return "null"
    if isinstance(value, (list, tuple)):
        return f'[{",".join(map(stringify_param_value, value))}]'
    if isinstance(value, dict):
        pairs = ",".join(f"{k}:{stringify_param_value(v)}" for k, v in value.items())
        return "{" + pairs + "}"
    return str(value)


# ── paths ─────────────────────────────────────────────────────────────────────
BASE = "/Users/guylubovitch/Documents/work/falkordemo/kg-orm/neo4j-sandbox-samples/network_and_IT/csv_output"
FILES = {
    "edges_connects": f"{BASE}/edges_connects.csv",
    "edges_instance": f"{BASE}/edges_instance.csv",
}
NODE_FILES = {
    "nodes_network":     f"{BASE}/nodes_network.csv",
    "nodes_interface":   f"{BASE}/nodes_interface.csv",
    "nodes_process":     f"{BASE}/nodes_process.csv",
    "nodes_application": f"{BASE}/nodes_application.csv",
    "nodes_os":          f"{BASE}/nodes_os.csv",
}

# ── build node index (labels_tuple, id) -> True ───────────────────────────────
node_index = {}  # (labels_tuple, id) -> filename

for fname, path in NODE_FILES.items():
    try:
        headers, rows = read_csv_rows(path)
    except FileNotFoundError:
        print(f"⚠  Node file missing: {fname}")
        continue
    lowered = {h.lower(): h for h in headers}
    id_col = lowered.get("id")
    labels_col = lowered.get("labels") or lowered.get("label")
    if not id_col:
        print(f"⚠  No 'id' column in {fname}")
        continue
    for row in rows:
        node_id = row.get(id_col)
        if node_id is None:
            continue
        raw_labels = row.get(labels_col) if labels_col else None
        labels = split_labels(raw_labels)
        if not labels:
            derived = fname.replace("nodes_", "")
            labels = split_labels(derived) or ["Node"]
        key = (tuple(labels), node_id)
        node_index[key] = fname
    print(f"✔  {fname}: {len(rows)} nodes loaded into index, first label example: {split_labels(rows[0].get(labels_col) if labels_col else None) if rows else []}")

print()
print("Node index size:", len(node_index))
print("Sample node keys (first 10):")
for k in list(node_index.keys())[:10]:
    print("  ", k)

# ── check the two edge files ──────────────────────────────────────────────────
for ename, epath in FILES.items():
    print(f"\n{'='*60}")
    print(f"Checking {ename}")
    headers, rows = read_csv_rows(epath)
    print(f"  Headers: {headers}")
    print(f"  Rows: {len(rows)}")

    lowered = {h.lower(): h for h in headers}
    src_label_col = lowered.get("source_label") or lowered.get("source_labels")
    tgt_label_col = lowered.get("target_label") or lowered.get("target_labels")
    src_col = lowered.get("source")
    tgt_col = lowered.get("target")

    print(f"  src_col={src_col!r}  src_label_col={src_label_col!r}")
    print(f"  tgt_col={tgt_col!r}  tgt_label_col={tgt_label_col!r}")

    if not src_col or not tgt_col:
        print("  ❌ Missing source/target column! Cannot load.")
        continue

    matched = missing_src = missing_tgt = missing_both = 0

    # sample mismatches
    src_mismatches = []
    tgt_mismatches = []

    for i, row in enumerate(rows):
        src = row.get(src_col)
        tgt = row.get(tgt_col)
        if src is None or tgt is None:
            continue

        src_labels = split_labels(row.get(src_label_col)) if src_label_col else []
        tgt_labels = split_labels(row.get(tgt_label_col)) if tgt_label_col else []

        has_src = (tuple(src_labels), src) in node_index if src_labels else False
        has_tgt = (tuple(tgt_labels), tgt) in node_index if tgt_labels else False

        if not has_src and not has_tgt:
            missing_both += 1
        elif not has_src:
            missing_src += 1
            if len(src_mismatches) < 5:
                src_mismatches.append((i, src, src_labels, row.get(src_label_col)))
        elif not has_tgt:
            missing_tgt += 1
            if len(tgt_mismatches) < 5:
                tgt_mismatches.append((i, tgt, tgt_labels, row.get(tgt_label_col)))
        else:
            matched += 1

    total = matched + missing_src + missing_tgt + missing_both
    print(f"\n  Results: {total} total rows")
    print(f"    ✅ Both endpoints found:   {matched}")
    print(f"    ❌ Source missing only:    {missing_src}")
    print(f"    ❌ Target missing only:    {missing_tgt}")
    print(f"    ❌ Both endpoints missing: {missing_both}")

    if src_mismatches:
        print(f"\n  Sample MISSING SOURCE entries (raw_label → sanitized_labels, id):")
        for (row_i, node_id, sanitized, raw) in src_mismatches:
            print(f"    row {row_i}: raw_label={raw!r} → sanitized={sanitized}  id={node_id!r}({type(node_id).__name__})")
            # Show what keys ARE in the index for this id
            matches_by_id = [(k, v) for k, v in node_index.items() if k[1] == node_id]
            if matches_by_id:
                print(f"      Node index HAS this id under: {[(k[0], v) for k,v in matches_by_id]}")
            else:
                print(f"      Node index has NO entry for id={node_id!r}")

    if tgt_mismatches:
        print(f"\n  Sample MISSING TARGET entries (raw_label → sanitized_labels, id):")
        for (row_i, node_id, sanitized, raw) in tgt_mismatches:
            print(f"    row {row_i}: raw_label={raw!r} → sanitized={sanitized}  id={node_id!r}({type(node_id).__name__})")
            matches_by_id = [(k, v) for k, v in node_index.items() if k[1] == node_id]
            if matches_by_id:
                print(f"      Node index HAS this id under: {[(k[0], v) for k,v in matches_by_id]}")
            else:
                print(f"      Node index has NO entry for id={node_id!r}")

    # Show the CYPHER that would be built for the first group
    print(f"\n  First 3 rows as edge items:")
    for row in rows[:3]:
        src = row.get(src_col)
        tgt = row.get(tgt_col)
        src_labels = split_labels(row.get(src_label_col)) if src_label_col else []
        tgt_labels = split_labels(row.get(tgt_label_col)) if tgt_label_col else []
        props = {k: v for k, v in row.items() if k not in (src_col, tgt_col, "type", src_label_col, tgt_label_col) and v is not None}
        item = {"source": src, "target": tgt, "props": props}
        print(f"    item={item}")
        print(f"    serialized={stringify_param_value(item)}")
        s_labels = ":" + ":".join(src_labels) if src_labels else ""
        t_labels = ":" + ":".join(tgt_labels) if tgt_labels else ""
        print(f"    MATCH (s{s_labels} {{id: row.source}}) MATCH (t{t_labels} {{id: row.target}})")

print("\nDone.")
