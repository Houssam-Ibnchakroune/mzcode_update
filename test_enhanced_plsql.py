from metazcode.sdk.ingestion.plsql.plsql_loader import PlsqlLoader
import json

# Test enhanced PL/SQL ingestion
loader = PlsqlLoader('examples/plsql_etl', 'examples/plsql_etl/create_schema.sql')

print('=== Enhanced PL/SQL Ingestion Test ===')
print()

nodes_list = []
edges_list = []

for nodes, edges in loader.ingest():
    nodes_list.extend(nodes)
    edges_list.extend(edges)

print(f'Total nodes: {len(nodes_list)}')
print(f'Total edges: {len(edges_list)}')
print()

# Show processing summary
summary = loader.get_processing_summary()
print('=== Processing Summary ===')
print(json.dumps(summary, indent=2))
print()

# Show enhanced metadata examples
print('=== Enhanced Metadata Examples ===')
for node in nodes_list:
    if 'type_mapping' in node.properties:
        print(f'Table: {node.name}')
        print('Type Mapping:', json.dumps(node.properties['type_mapping'], indent=2))
        print()
    if 'column_lineage' in node.properties:
        print(f'Operation: {node.name}')
        print('Column Lineage:', json.dumps(node.properties['column_lineage'], indent=2))
        print()
    if node.properties.get('operation_subtype'):
        print(f'Operation: {node.name} - Subtype: {node.properties.get("operation_subtype")}')
