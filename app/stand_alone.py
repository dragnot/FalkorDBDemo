import sys
import redis

GRAPH = "cinema"
HOST = "localhost"
PORT = 6378

CREATE_QUERY = """
MERGE (m:Movie {title:'Notting Hill'})
MERGE (j:Person {name:'Julia Roberts'})
  ON CREATE SET j.age = 58
  ON MATCH  SET j.age = coalesce(j.age, 58)
MERGE (h:Person {name:'Hugh Grant'})
  ON CREATE SET h.age = 65
  ON MATCH  SET h.age = coalesce(h.age, 65)
MERGE (j)-[:ACTED_IN]->(m)
MERGE (h)-[:ACTED_IN]->(m)
RETURN m.title AS movie, j.name AS lead1, j.age AS age1, h.name AS lead2, h.age AS age2
"""

VERIFY_QUERY = """
MATCH (p:Person)-[:ACTED_IN]->(m:Movie {title:'Notting Hill'})
RETURN p.name AS person, p.age AS age, m.title AS movie
ORDER BY person
"""

def run_graph_query(r: redis.Redis, graph: str, q: str):
    # FalkorDB uses RedisGraph-style commands
    return r.execute_command("GRAPH.QUERY", graph, q)

def main():
    r = redis.Redis(host=HOST, port=PORT, decode_responses=True)

    # Quick connectivity check
    try:
        r.ping()
    except Exception as e:
        print(f"Failed to connect to Redis/FalkorDB at {HOST}:{PORT}: {e}")
        sys.exit(1)

    # Create / upsert graph entities
    print("Creating Notting Hill graph...")
    create_res = run_graph_query(r, GRAPH, CREATE_QUERY)
    print("Create result:", create_res)

    # Verify existence
    print("\nVerifying nodes/edges...")
    verify_res = run_graph_query(r, GRAPH, VERIFY_QUERY)

    # The exact response shape can vary by FalkorDB/RedisGraph versions.
    # We’ll print it raw, and also try to pretty-print rows if present.
    print("Raw verify result:", verify_res)

    # Best-effort parse: look for a nested list of rows in the response.
    rows = None
    if isinstance(verify_res, list):
        # Common pattern: [header, rows, stats] OR [rows, stats]
        for item in verify_res:
            if isinstance(item, list) and item and isinstance(item[0], list):
                rows = item
                break

    if rows:
        print("\nPeople who acted in Notting Hill:")
        for row in rows:
            # row expected: [person, age, movie]
            person = row[0] if len(row) > 0 else None
            age = row[1] if len(row) > 1 else None
            movie = row[2] if len(row) > 2 else None
            print(f"- {person} (age {age}) -> {movie}")
    else:
        print("\nCould not auto-parse rows from response (format may differ). Raw output above is authoritative.")

if __name__ == "__main__":
    main()