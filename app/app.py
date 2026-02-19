from flask import Flask, jsonify, Response
import os
import redis

app = Flask(__name__)

FALKOR_HOST = os.getenv("FALKOR_HOST", "falkordb")
FALKOR_PORT = int(os.getenv("FALKOR_PORT", "6379"))
GRAPH_NAME = os.getenv("GRAPH_NAME", "demo")

# “FalkorDB-ish” palette (dark + neon mint accent). Inspired by their site vibe.
BG = "#070B12"
PANEL = "#0C1220"
TEXT = "#E7EEF8"
MUTED = "#9DB0C7"
ACCENT = "#38F2B9"   # neon mint
ACCENT2 = "#2DD4FF"  # cyan glow


def rclient() -> redis.Redis:
    return redis.Redis(host=FALKOR_HOST, port=FALKOR_PORT, decode_responses=True)


def ensure_demo_graph():
    r = rclient()
    r.ping()
    q = """
    MERGE (a:Person {name:'Alice'})
    MERGE (b:Person {name:'Bob'})
    MERGE (a)-[:KNOWS]->(b)
    """
    r.execute_command("GRAPH.QUERY", GRAPH_NAME, q)


def fetch_graph_as_nodes_edges():
    r = rclient()

    # Pull nodes + edges for visualization. Keep it simple: names + relation type.
    q = """
    MATCH (a)-[r]->(b)
    RETURN a.name, type(r), b.name
    """
    raw = r.execute_command("GRAPH.QUERY", GRAPH_NAME, q)

    # GRAPH.QUERY response format is: [header, rows, stats]
    # header: list of columns, rows: list of row lists
    rows = raw[1] if len(raw) > 1 else []

    nodes_map = {}
    edges = []
    next_id = 1

    def node_id(name: str) -> int:
        nonlocal next_id
        if name not in nodes_map:
            nodes_map[name] = next_id
            next_id += 1
        return nodes_map[name]

    for row in rows:
        a_name = row[0]
        rel = row[1]
        b_name = row[2]
        a_id = node_id(a_name)
        b_id = node_id(b_name)
        edges.append({"from": a_id, "to": b_id, "label": rel})

    nodes = [{"id": nid, "label": name} for name, nid in nodes_map.items()]
    return nodes, edges


@app.get("/api/graph")
def api_graph():
    ensure_demo_graph()
    nodes, edges = fetch_graph_as_nodes_edges()
    return jsonify({"graph": GRAPH_NAME, "nodes": nodes, "edges": edges})


@app.post("/api/reseed")
def api_reseed():
    r = rclient()
    # Drop and recreate (fast, clean demo).
    r.execute_command("GRAPH.DELETE", GRAPH_NAME)
    ensure_demo_graph()
    return jsonify({"status": "ok", "graph": GRAPH_NAME})


@app.get("/")
def ui():
    # Serve a single-file HTML page with:
    # - Falkor-ish theme
    # - Inline “dragon” SVG
  # - Simple JSON output (no visualization)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>falkordemo • Network</title>

  <style>
    :root {{
      --bg: {BG};
      --panel: {PANEL};
      --text: {TEXT};
      --muted: {MUTED};
      --accent: {ACCENT};
      --accent2: {ACCENT2};
    }}

    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      background: radial-gradient(1200px 600px at 20% 10%, rgba(56,242,185,.14), transparent 60%),
                  radial-gradient(900px 500px at 85% 35%, rgba(45,212,255,.10), transparent 55%),
                  var(--bg);
      color: var(--text);
    }}

    .wrap {{
      max-width: 1050px;
      margin: 0 auto;
      padding: 28px 18px 40px;
    }}

    .header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 16px;
    }}

    .brand {{
      display: flex;
      align-items: center;
      gap: 14px;
    }}

    .title {{
      line-height: 1.1;
    }}
    .title h1 {{
      margin: 0;
      font-size: 20px;
      font-weight: 700;
      letter-spacing: 0.2px;
    }}
    .title p {{
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 13px;
    }}

    .card {{
      background: linear-gradient(180deg, rgba(12,18,32,.92), rgba(12,18,32,.75));
      border: 1px solid rgba(56,242,185,.18);
      border-radius: 18px;
      box-shadow: 0 20px 70px rgba(0,0,0,.35);
      overflow: hidden;
    }}

    .toolbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid rgba(255,255,255,.06);
    }}

    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 10px;
      border-radius: 999px;
      font-size: 12px;
      color: var(--muted);
      border: 1px solid rgba(255,255,255,.08);
      background: rgba(0,0,0,.18);
    }}
    .dot {{
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--accent);
      box-shadow: 0 0 14px rgba(56,242,185,.55);
    }}

    .btns {{
      display: flex;
      gap: 10px;
    }}

    button {{
      cursor: pointer;
      border: 1px solid rgba(56,242,185,.22);
      background: rgba(56,242,185,.10);
      color: var(--text);
      padding: 8px 10px;
      border-radius: 12px;
      font-size: 12px;
    }}
    button:hover {{
      border-color: rgba(56,242,185,.40);
      background: rgba(56,242,185,.14);
    }}

    pre {{
      margin: 0;
      padding: 16px;
      white-space: pre-wrap;
      word-break: break-word;
      color: var(--text);
      font-size: 13px;
      line-height: 1.45;
    }}

    .hint {{
      padding: 12px 16px;
      border-top: 1px solid rgba(255,255,255,.06);
      color: var(--muted);
      font-size: 12px;
    }}

    .footer {{
      margin-top: 14px;
      color: var(--muted);
      font-size: 12px;
    }}

    /* Inline dragon SVG sizing */
    .dragon {{
      width: 52px;
      height: 52px;
      filter: drop-shadow(0 0 18px rgba(56,242,185,.25));
    }}
  </style>
</head>

<body>
  <div class="wrap">
    <div class="header">
      <div class="brand">
        <!-- Simple “dragon” SVG (stylized) -->
        <svg class="dragon" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="dragon">
          <path d="M46 12c-7-7-19-6-26 1-8 8-7 22 2 29 7 6 17 6 24 0 3-2 5-5 6-8"
                stroke="{ACCENT}" stroke-width="3.2" stroke-linecap="round"/>
          <path d="M42 18c-6-5-14-4-19 1-6 6-5 16 1 21 5 4 12 4 17 0"
                stroke="{ACCENT2}" stroke-opacity="0.9" stroke-width="2.6" stroke-linecap="round"/>
          <path d="M49 13l6 2-5 4" stroke="{ACCENT}" stroke-width="3.2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M38 30c5-1 9 1 12 5" stroke="{ACCENT}" stroke-width="2.6" stroke-linecap="round"/>
          <circle cx="44.5" cy="24.5" r="1.7" fill="{TEXT}"/>
        </svg>

        <div class="title">
          <h1>falkordemo • Network</h1>
          <p>Flask + FalkorDB • live graph viz</p>
        </div>
      </div>

      <div class="btns">
        <button id="reloadBtn">Reload</button>
      </div>
    </div>

    <div class="card">
      <div class="toolbar">
        <div class="chip"><span class="dot"></span> Connected to <span id="connText">{FALKOR_HOST}:{FALKOR_PORT}</span></div>
        <div class="chip">Graph: <strong style="color: var(--text); font-weight: 700;">{GRAPH_NAME}</strong></div>
      </div>
      <pre id="out">Loading…</pre>
      <div class="hint">Shows the nodes returned by <code>/api/graph</code> as JSON.</div>
    </div>
  </div>

<script>
  const outEl = document.getElementById("out");

  async function loadNodes() {{
    outEl.textContent = "Loading…";
    const res = await fetch("/api/graph");
    const payload = await res.json();
    outEl.textContent = JSON.stringify(payload.nodes, null, 2);
  }}

  document.getElementById("reloadBtn").addEventListener("click", loadNodes);
  loadNodes();
</script>

</body>
</html>
"""
    return Response(html, mimetype="text/html")