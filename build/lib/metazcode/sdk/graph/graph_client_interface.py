from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List

from metazcode.sdk.models.graph import Node, Edge


class GraphClientInterface(ABC):
    """
    Defines the interface for a graph client.

    This abstract class ensures that any graph backend implementation
    (e.g., NetworkX, Neo4j) provides a consistent API for writing
    and retrieving graph data.
    """

    @abstractmethod
    def write_node(self, node: Node):
        """Persists a node to the graph backend."""
        raise NotImplementedError

    @abstractmethod
    def write_edge(self, edge: Edge):
        """Persists an edge to the graph backend."""
        raise NotImplementedError

    @abstractmethod
    def add_nodes(self, nodes: List[Node]):
        """Persists a list of nodes to the graph backend."""
        raise NotImplementedError

    @abstractmethod
    def add_edges(self, edges: List[Edge]):
        """Persists a list of edges to the graph backend."""
        raise NotImplementedError

    @abstractmethod
    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single node by its ID from the graph."""
        raise NotImplementedError

    @abstractmethod
    def get_all_nodes(self) -> List[Node]:
        """Retrieves all nodes from the graph."""
        raise NotImplementedError

    @abstractmethod
    def get_node_count(self) -> int:
        """Returns the total number of nodes in the graph."""
        raise NotImplementedError

    @abstractmethod
    def get_edge_count(self) -> int:
        """Returns the total number of edges in the graph."""
        raise NotImplementedError

    @abstractmethod
    def get_graph(self):
        raise NotImplementedError
