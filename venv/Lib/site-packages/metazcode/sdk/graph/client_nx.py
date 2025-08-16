import networkx as nx
from typing import Optional, Dict, Any, List

from metazcode.sdk.models.graph import Node, Edge
from metazcode.sdk.models.canonical_types import NodeType
from .graph_client_interface import GraphClientInterface


class NetworkXGraphClient(GraphClientInterface):
    """A graph client that uses NetworkX for in-memory graph representation."""

    def __init__(self):
        self._graph = nx.DiGraph()

    def write_node(self, node: Node):
        """
        Adds or updates a node in the graph.

        The node's `id` is used as the key in the NetworkX graph.
        All other attributes of the Node object are added as node attributes.
        """
        attributes = node.to_dict()
        node_id = attributes.pop("id")

        if self._graph.has_node(node_id):
            nx.set_node_attributes(self._graph, {node_id: attributes})
        else:
            self._graph.add_node(node_id, **attributes)

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a node's data from the graph by its ID."""
        if self._graph.has_node(node_id):
            node_data = self._graph.nodes[node_id]
            return {
                "id": node_id,
                "label": node_data.get("label", node_id),
                "attributes": node_data,
            }
        return None

    def get_all_nodes(self) -> List[Node]:
        """Retrieves all nodes from the graph as Node objects."""
        nodes = []
        for node_id, attributes in self._graph.nodes(data=True):
            # Reconstruct Node object from stored attributes
            node_data = attributes.copy()
            node_data["node_id"] = node_id

            # Create Node object from the stored data
            node = Node(**node_data)
            nodes.append(node)
        return nodes

    def get_nodes_by_type(self, node_type: NodeType) -> List[Dict[str, Any]]:
        """Retrieves all nodes of a specific type."""
        nodes = []
        for node_id, node_data in self._graph.nodes(data=True):
            if node_data.get("node_type") == node_type:
                nodes.append(
                    {
                        "id": node_id,
                        "label": node_data.get("label", node_id),
                        "attributes": node_data,
                    }
                )
        return nodes

    def add_node(self, node_dict: Dict[str, Any]) -> None:
        """Add a node from a dictionary representation."""
        node_id = node_dict["id"]
        attributes = node_dict.get("attributes", {})
        label = node_dict.get("label", node_id)

        # Include label in attributes if not already there
        if "label" not in attributes:
            attributes["label"] = label

        self._graph.add_node(node_id, **attributes)

    def add_edge(self, edge_dict: Dict[str, Any]) -> None:
        """Add an edge from a dictionary representation."""
        source = edge_dict["source"]
        target = edge_dict["target"]
        attributes = edge_dict.get("attributes", {})
        label = edge_dict.get("label", "")

        # Include label in attributes if not already there
        if "label" not in attributes:
            attributes["label"] = label

        self._graph.add_edge(source, target, **attributes)

    def write_edge(self, edge: Edge):
        """
        Adds a directed edge to the graph.

        All attributes of the Edge object are added as edge attributes.
        """
        if not self._graph.has_node(edge.source_id):
            raise ValueError(f"Source node '{edge.source_id}' not found in graph.")
        if not self._graph.has_node(edge.target_id):
            raise ValueError(f"Target node '{edge.target_id}' not found in graph.")

        attributes = edge.to_dict()
        source_id = attributes.pop("source_id")
        target_id = attributes.pop("target_id")

        self._graph.add_edge(source_id, target_id, **attributes)

    def add_nodes(self, nodes: List[Node]):
        """Adds a batch of nodes to the graph."""
        for node in nodes:
            self.write_node(node)

    def add_edges(self, edges: List[Edge]):
        """Adds a batch of edges to the graph."""
        for edge in edges:
            self.write_edge(edge)

    def get_node_count(self) -> int:
        """Returns the total number of nodes in the graph."""
        return self._graph.number_of_nodes()

    def get_edge_count(self) -> int:
        """Returns the total number of edges in the graph."""
        return self._graph.number_of_edges()

    def get_graph(self) -> nx.DiGraph:
        """Returns the underlying NetworkX graph object."""
        return self._graph
