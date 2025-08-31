import json
with open('enhanced_graph_plsql.json', 'r') as f:
    data = json.load(f)

print('=== Tables with and without type mapping ===')
print()

for node in data['nodes']:
    if node['node_type'] == 'table':
        name = node['name']
        props = node['properties']
        
        print(f'Table: {name}')
        if 'type_mapping' in props:
            print('  ✓ Has type mapping')
            columns_count = len(props["type_mapping"].get("columns", []))
            confidence = props["type_mapping"].get("mapping_confidence", "N/A")
            print(f'    Columns: {columns_count}')
            print(f'    Confidence: {confidence}')
        elif 'create_statement' in props:
            print('  ⚠ Has CREATE statement but no type mapping')
            statement = props["create_statement"][:100] + "..."
            print(f'    Statement: {statement}')
        else:
            print('  ✗ No CREATE statement found')
            source = props.get("table_source", "unknown")
            print(f'    Source: {source}')
        print()
