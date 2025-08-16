"""
Context Collection Functions for LLM Summary Implementation

This module implements Task 1.1: Build Context Collection Functions
- get_operation_context(): Collects operation context with connections and parent pipeline
- get_connection_details(): Extracts connection details like database, server, table names
- summarize_transformations(): Creates human-readable transformation summaries

This is part of the optional LLM enrichment layer and requires a graph client.
"""

from typing import Dict, Any, Optional, List
from metazcode.sdk.graph.graph_client_interface import GraphClientInterface
from metazcode.sdk.models.canonical_types import EdgeType, NodeType


class ContextCollector:
    """
    Collects contextual information from the deterministic graph for LLM enrichment.

    This class provides methods to extract and format graph data for use in LLM prompts,
    enabling context-aware AI analysis of SSIS projects.
    """

    def __init__(self, graph_client: GraphClientInterface):
        """
        Initialize the context collector.

        Args:
            graph_client: The graph client interface for accessing graph data
        """
        self.graph_client = graph_client

    def get_operation_context(self, operation_node_id: str) -> Dict[str, Any]:
        """
        Collect context for an operation node including source/destination connections and parent pipeline.

        Args:
            operation_node_id: The ID of the operation node to collect context for

        Returns:
            Dictionary containing:
            - source_connections: List of connection details that this operation reads from
            - destination_connections: List of connection details that this operation writes to
            - parent_pipeline: Information about the containing pipeline
            - operation_details: Basic operation information
        """
        context = {
            "operation_id": operation_node_id,
            "source_connections": [],
            "destination_connections": [],
            "parent_pipeline": None,
            "operation_details": {},
        }

        # Get the operation node details
        operation_node = self.graph_client.get_node(operation_node_id)
        if not operation_node:
            return context

        context["operation_details"] = {
            "name": operation_node.get("name", "Unknown"),
            "type": operation_node.get("type", "Unknown"),
            "properties": operation_node.get("properties", {}),
        }

        # Get all edges for this operation node to find connections
        graph = self.graph_client.get_graph()

        # Find source connections (READS_FROM edges) - Optimized O(in_degree)
        for source, target, edge_attrs in graph.in_edges(operation_node_id, data=True):
            if edge_attrs.get("type") == EdgeType.READS_FROM.value:
                connection_details = self.get_connection_details(source)
                if connection_details:
                    context["source_connections"].append(connection_details)

        # Find destination connections (WRITES_TO edges) - Optimized O(out_degree)
        for source, target, edge_attrs in graph.out_edges(operation_node_id, data=True):
            if edge_attrs.get("type") == EdgeType.WRITES_TO.value:
                connection_details = self.get_connection_details(target)
                if connection_details:
                    context["destination_connections"].append(connection_details)

        # Find parent pipeline (CONTAINS edge) - Optimized O(in_degree)
        for source, target, edge_attrs in graph.in_edges(operation_node_id, data=True):
            if edge_attrs.get("type") == EdgeType.CONTAINS.value:
                parent_node = self.graph_client.get_node(source)
                if parent_node and parent_node.get("type") == NodeType.PIPELINE.value:
                    context["parent_pipeline"] = {
                        "id": source,
                        "name": parent_node.get("name", "Unknown"),
                        "properties": parent_node.get("properties", {}),
                    }
                    break

        return context

    def get_connection_details(
        self, connection_node_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract connection details like database name, server, table/file name.

        Args:
            connection_node_id: The ID of the connection node

        Returns:
            Dictionary with connection details or None if node not found:
            - id: Connection node ID
            - name: Connection name
            - database: Database name (if available)
            - server: Server name (if available)
            - table_or_file: Table or file name (if available)
            - connection_type: Type of connection (database, file, etc.)
            - properties: Raw connection properties
        """
        connection_node = self.graph_client.get_node(connection_node_id)
        if not connection_node:
            return None

        properties = connection_node.get("properties", {})

        details = {
            "id": connection_node_id,
            "name": connection_node.get("name", "Unknown"),
            "connection_type": connection_node.get("type", "Unknown"),
            "properties": properties,
        }

        # Extract common connection properties
        # Database connections
        if "ServerName" in properties:
            details["server"] = properties["ServerName"]
        if "InitialCatalog" in properties:
            details["database"] = properties["InitialCatalog"]
        if "TableOrViewName" in properties:
            details["table_or_file"] = properties["TableOrViewName"]

        # File connections
        if "ConnectionString" in properties:
            conn_str = properties["ConnectionString"]
            # Extract file path from connection string if it's a file
            if "file://" in conn_str.lower() or "\\" in conn_str or "/" in conn_str:
                details["table_or_file"] = conn_str

        # OLE DB connections
        if "ConnectionString" in properties:
            conn_str = properties["ConnectionString"]
            # Parse common connection string patterns
            if "Data Source=" in conn_str:
                for part in conn_str.split(";"):
                    if part.strip().startswith("Data Source="):
                        details["server"] = part.split("=", 1)[1].strip()
                    elif part.strip().startswith("Initial Catalog="):
                        details["database"] = part.split("=", 1)[1].strip()

        return details

    def summarize_transformations(self, transformations: List[Dict[str, Any]]) -> str:
        """
        Create a human-readable summary of transformations.

        Args:
            transformations: Array of transformation objects from operation properties

        Returns:
            Concise human-readable summary (e.g., "3 derived columns, 1 conditional split, 2 lookup operations")
        """
        if not transformations:
            return "No transformations"

        # Count transformations by type
        transformation_counts = {}

        for transform in transformations:
            transform_type = transform.get("type", "unknown")

            # Normalize transformation type names for readability
            readable_type = self._normalize_transformation_type(transform_type)

            if readable_type in transformation_counts:
                transformation_counts[readable_type] += 1
            else:
                transformation_counts[readable_type] = 1

        # Build summary string
        if not transformation_counts:
            return "No transformations"

        summary_parts = []
        for transform_type, count in transformation_counts.items():
            if count == 1:
                summary_parts.append(f"1 {transform_type}")
            else:
                summary_parts.append(f"{count} {transform_type}s")

        return ", ".join(summary_parts)

    def _normalize_transformation_type(self, transform_type: str) -> str:
        """
        Normalize transformation type names for human readability.

        Args:
            transform_type: Raw transformation type string

        Returns:
            Human-readable transformation type name
        """
        # Mapping of technical names to readable names
        type_mapping = {
            "DerivedColumn": "derived column",
            "ConditionalSplit": "conditional split",
            "Lookup": "lookup operation",
            "DataConversion": "data conversion",
            "Sort": "sort operation",
            "Aggregate": "aggregate operation",
            "Merge": "merge operation",
            "MergeJoin": "merge join",
            "UnionAll": "union all",
            "Multicast": "multicast",
            "OLEDBCommand": "OLE DB command",
            "OLEDBDestination": "OLE DB destination",
            "FlatFileDestination": "flat file destination",
            "ExcelDestination": "Excel destination",
        }

        return type_mapping.get(transform_type, transform_type.lower())

    def get_enriched_operation_summary(self, operation_node_id: str) -> Dict[str, Any]:
        """
        Get a complete enriched summary combining all context collection functions.

        This is a convenience method that combines the output of all three main functions
        to provide a comprehensive context summary for LLM processing.

        Args:
            operation_node_id: The ID of the operation node

        Returns:
            Complete enriched summary with context, connections, and transformation summaries
        """
        context = self.get_operation_context(operation_node_id)

        # Add transformation summaries if available
        if "transformations" in context["operation_details"].get("properties", {}):
            transformations = context["operation_details"]["properties"][
                "transformations"
            ]
            context["transformation_summary"] = self.summarize_transformations(
                transformations
            )
        else:
            context["transformation_summary"] = "No transformations"

        return context
