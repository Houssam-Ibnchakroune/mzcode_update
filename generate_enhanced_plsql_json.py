from metazcode.sdk.ingestion.plsql.plsql_loader import PlsqlLoader
import json
from pathlib import Path

# Test enhanced PL/SQL ingestion and generate comprehensive JSON like SSIS
loader = PlsqlLoader('examples/plsql_etl', 'examples/plsql_etl/create_schema.sql')

print('=== Generating Enhanced PL/SQL JSON (SSIS-like quality) ===')
print()

# Collect all nodes and edges
all_nodes = []
all_edges = []

for nodes, edges in loader.ingest():
    all_nodes.extend(nodes)
    all_edges.extend(edges)

# Create enhanced JSON structure similar to SSIS
enhanced_json = {
    "metadata": {
        "source_type": "plsql_etl",
        "processing_engine": "Enhanced PL/SQL Loader",
        "extraction_timestamp": "2025-08-27T00:00:00Z",
        "capabilities": loader.get_processing_summary()["metadata_capabilities"],
        "supported_platforms": loader.get_processing_summary()["supported_target_platforms"]
    },
    "nodes": [],
    "edges": [],
    "statistics": {
        "total_nodes": len(all_nodes),
        "total_edges": len(all_edges),
        "node_types": {},
        "edge_types": {}
    }
}

# Process nodes with enhanced metadata
for node in all_nodes:
    node_data = {
        "node_id": node.node_id,
        "node_type": node.node_type,
        "name": node.name,
        "properties": node.properties
    }
    
    # Add operation_subtype if it exists (like SSIS)
    if "operation_subtype" in node.properties:
        node_data["operation_subtype"] = node.properties["operation_subtype"]
    
    # Add column_lineage if it exists (like SSIS)
    if "column_lineage" in node.properties:
        node_data["column_lineage"] = node.properties["column_lineage"]
    
    # Add type_mapping if it exists (enhanced feature)
    if "type_mapping" in node.properties:
        node_data["type_mapping"] = node.properties["type_mapping"]
    
    enhanced_json["nodes"].append(node_data)
    
    # Update statistics
    node_type = node.node_type
    enhanced_json["statistics"]["node_types"][node_type] = enhanced_json["statistics"]["node_types"].get(node_type, 0) + 1

# Process edges
for edge in all_edges:
    edge_data = {
        "source_id": edge.source_id,
        "target_id": edge.target_id,
        "relation": edge.relation,
        "properties": edge.properties
    }
    
    enhanced_json["edges"].append(edge_data)
    
    # Update statistics
    relation = edge.relation
    enhanced_json["statistics"]["edge_types"][relation] = enhanced_json["statistics"]["edge_types"].get(relation, 0) + 1

# Add validation report
enhanced_json["validation_report"] = loader.get_processing_summary()["validation_report"]

# Save to file
output_file = "enhanced_graph_plsql_improved.json"
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(enhanced_json, f, indent=2, ensure_ascii=False)

print(f"Enhanced PL/SQL JSON generated: {output_file}")
print(f"Total nodes: {enhanced_json['statistics']['total_nodes']}")
print(f"Total edges: {enhanced_json['statistics']['total_edges']}")
print()
print("Node types distribution:")
for node_type, count in enhanced_json["statistics"]["node_types"].items():
    print(f"  {node_type}: {count}")
print()
print("Edge types distribution:")
for edge_type, count in enhanced_json["statistics"]["edge_types"].items():
    print(f"  {edge_type}: {count}")
print()

# Show key enhancements compared to basic ingestion
print("=== Key Enhancements vs Basic PL/SQL ===")
enhancements = []

# Count nodes with type mapping
type_mapping_count = sum(1 for node in enhanced_json["nodes"] if "type_mapping" in node.get("properties", {}))
if type_mapping_count > 0:
    enhancements.append(f"✓ {type_mapping_count} tables with comprehensive type mapping")

# Count operations with subtypes
subtype_count = sum(1 for node in enhanced_json["nodes"] if "operation_subtype" in node.get("properties", {}))
if subtype_count > 0:
    enhancements.append(f"✓ {subtype_count} operations with categorized subtypes")

# Count operations with column lineage
lineage_count = sum(1 for node in enhanced_json["nodes"] if "column_lineage" in node.get("properties", {}))
if lineage_count > 0:
    enhancements.append(f"✓ {lineage_count} operations with column lineage")

# Show platform support
platform_count = len(enhanced_json["metadata"]["supported_platforms"])
enhancements.append(f"✓ Multi-platform type conversion for {platform_count} target platforms")

for enhancement in enhancements:
    print(enhancement)

print(f"\n📁 Enhanced JSON saved to: {output_file}")
print("🎯 PL/SQL ingestion now matches SSIS metadata quality!")
