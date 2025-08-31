import json
with open('enhanced_graph_plsql.json', 'r') as f:
    data = json.load(f)

print('=== Analysis of Generated JSON ===')
print()

# Count nodes by type
node_types = {}
tables_with_type_mapping = 0
tables_without_type_mapping = 0

for node in data['nodes']:
    node_type = node['node_type']
    node_types[node_type] = node_types.get(node_type, 0) + 1
    
    if node_type == 'table':
        print(f'Table: {node["name"]}')
        if 'type_mapping' in node['properties']:
            tables_with_type_mapping += 1
            print(f'  ✓ Has type mapping')
        else:
            tables_without_type_mapping += 1
            print(f'  ✗ No type mapping')
            print(f'  Properties: {list(node["properties"].keys())}')
        print()

print(f'Node type distribution: {node_types}')
print(f'Tables with type mapping: {tables_with_type_mapping}')
print(f'Tables without type mapping: {tables_without_type_mapping}')
print()

# Check for operation subtypes
operation_subtypes = []
operations_with_lineage = 0

for node in data['nodes']:
    if node['node_type'] == 'operation':
        subtype = node['properties'].get('operation_subtype')
        if subtype:
            operation_subtypes.append(subtype)
        
        if 'column_lineage' in node['properties']:
            operations_with_lineage += 1

print(f'Operation subtypes found: {set(operation_subtypes)}')
print(f'Operations with column lineage: {operations_with_lineage}')
