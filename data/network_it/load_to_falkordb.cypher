// FalkorDB Load Script - Generated from Neo4j Export
// Run this script in FalkorDB to import the data

// Load Nodes

LOAD CSV WITH HEADERS FROM 'file:///nodes_datacenter.csv' AS row
MERGE (n:Datacenter {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_router.csv' AS row
MERGE (n:Router {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_egress.csv' AS row
MERGE (n:Egress {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_interface.csv' AS row
MERGE (n:Interface {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_network.csv' AS row
MERGE (n:Network {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_zone.csv' AS row
MERGE (n:Zone {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_rack.csv' AS row
MERGE (n:Rack {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_switch.csv' AS row
MERGE (n:Switch {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_type.csv' AS row
MERGE (n:Type {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_machine.csv' AS row
MERGE (n:Machine {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_software.csv' AS row
MERGE (n:Software {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_os.csv' AS row
MERGE (n:Os {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_service.csv' AS row
MERGE (n:Service {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_application.csv' AS row
MERGE (n:Application {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_version.csv' AS row
MERGE (n:Version {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_process.csv' AS row
MERGE (n:Process {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;


LOAD CSV WITH HEADERS FROM 'file:///nodes_port.csv' AS row
MERGE (n:Port {id: toInteger(row.id)})
SET n += row
REMOVE n.id, n.labels;

// Load Relationships

LOAD CSV WITH HEADERS FROM 'file:///edges_contains.csv' AS row
MATCH (a:DataCenter {id: toInteger(row.source)})
MATCH (b:Rack {id: toInteger(row.target)})
MERGE (a)-[r:CONTAINS]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_routes.csv' AS row
MATCH (a:Router {id: toInteger(row.source)})
MATCH (b:Interface {id: toInteger(row.target)})
MERGE (a)-[r:ROUTES]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_connects.csv' AS row
MATCH (a:Interface {id: toInteger(row.source)})
MATCH (b:Network {id: toInteger(row.target)})
MERGE (a)-[r:CONNECTS]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_holds.csv' AS row
MATCH (a:Rack {id: toInteger(row.source)})
MATCH (b:Machine {id: toInteger(row.target)})
MERGE (a)-[r:HOLDS]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_type.csv' AS row
MATCH (a:Machine {id: toInteger(row.source)})
MATCH (b:Type {id: toInteger(row.target)})
MERGE (a)-[r:TYPE]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_version.csv' AS row
MATCH (a:Software {id: toInteger(row.source)})
MATCH (b:Version {id: toInteger(row.target)})
MERGE (a)-[r:VERSION]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_previous.csv' AS row
MATCH (a:Version {id: toInteger(row.source)})
MATCH (b:Version {id: toInteger(row.target)})
MERGE (a)-[r:PREVIOUS]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_depends_on.csv' AS row
MATCH (a:Software {id: toInteger(row.source)})
MATCH (b:Version {id: toInteger(row.target)})
MERGE (a)-[r:DEPENDS_ON]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_runs.csv' AS row
MATCH (a:Machine {id: toInteger(row.source)})
MATCH (b:OS {id: toInteger(row.target)})
MERGE (a)-[r:RUNS]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_instance.csv' AS row
MATCH (a:OS {id: toInteger(row.source)})
MATCH (b:Version {id: toInteger(row.target)})
MERGE (a)-[r:INSTANCE]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_exposes.csv' AS row
MATCH (a:Interface {id: toInteger(row.source)})
MATCH (b:Port {id: toInteger(row.target)})
MERGE (a)-[r:EXPOSES]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;


LOAD CSV WITH HEADERS FROM 'file:///edges_listens.csv' AS row
MATCH (a:Application {id: toInteger(row.source)})
MATCH (b:Port {id: toInteger(row.target)})
MERGE (a)-[r:LISTENS]->(b)
SET r += row
REMOVE r.source, r.source_label, r.target, r.target_label, r.type;

