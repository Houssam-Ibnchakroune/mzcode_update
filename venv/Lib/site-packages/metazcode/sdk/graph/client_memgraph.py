import logging
from typing import Optional, Dict, Any, List
import json

from metazcode.sdk.models.graph import Node, Edge
from metazcode.sdk.models.canonical_types import NodeType
from metazcode.sdk.models.config import DatabaseConfig
from .graph_client_interface import GraphClientInterface

logger = logging.getLogger(__name__)


class MemgraphClient(GraphClientInterface):
    """A graph client that uses Memgraph for persistent graph storage."""

    def __init__(self, config: DatabaseConfig):
        """
        Initialize Memgraph client with connection configuration.

        Args:
            config: Database configuration containing connection details.
        """
        self.config = config
        self._connection = None
        self._connect()

    def _connect(self):
        """Establish connection to Memgraph database."""
        try:
            import mgclient

            # Build connection parameters, only include auth if provided
            connect_params = {"host": self.config.host, "port": self.config.port}

            # Try connection without authentication first (default Memgraph setup)
            try:
                self._connection = mgclient.connect(**connect_params)
                logger.info(
                    f"Connected to Memgraph at {self.config.host}:{self.config.port} (no auth)"
                )
            except Exception as e:
                # If that fails and we have credentials, try with authentication
                if self.config.username and self.config.password:
                    connect_params["username"] = self.config.username
                    connect_params["password"] = self.config.password
                    self._connection = mgclient.connect(**connect_params)
                    logger.info(
                        f"Connected to Memgraph at {self.config.host}:{self.config.port} (with auth)"
                    )
                else:
                    # Re-raise the original error if no credentials available
                    raise e

        except ImportError:
            raise ImportError(
                "mgclient package is required for Memgraph support. Install with: pip install mgclient"
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Memgraph: {e}")

    def test_connection(self) -> bool:
        """Test if the connection to Memgraph is valid."""
        try:
            cursor = self._connection.cursor()
            cursor.execute("RETURN 1")
            result = cursor.fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def _execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Execute a Cypher query with optional parameters."""
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, parameters or {})
            result = cursor.fetchall()
            # Explicitly commit the transaction
            self._connection.commit()
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {query}, error: {e}")
            # Rollback on error
            try:
                self._connection.rollback()
            except:
                pass
            raise

    def write_node(self, node: Node):
        """
        Adds or updates a node in the Memgraph database.

        Uses MERGE to create or update the node with all its properties.
        """
        node_dict = node.to_dict()
        node_id = node_dict.pop("id")

        # Convert properties to JSON-serializable format
        properties = {}
        for key, value in node_dict.items():
            if isinstance(value, (dict, list)):
                properties[key] = json.dumps(value)
            else:
                properties[key] = value

        # Build property string for Cypher query
        if properties:
            prop_string = ", ".join([f"{key}: ${key}" for key in properties.keys()])
            query = f"""
            MERGE (n:Node {{id: $node_id}})
            SET n += {{{prop_string}}}
            """
        else:
            query = f"""
            MERGE (n:Node {{id: $node_id}})
            """

        parameters = {"node_id": node_id, **properties}
        self._execute_query(query, parameters)

    def write_edge(self, edge: Edge):
        """
        Adds a directed edge to the Memgraph database.

        Ensures both source and target nodes exist before creating the edge.
        """
        edge_dict = edge.to_dict()
        source_id = edge_dict.pop("source_id")
        target_id = edge_dict.pop("target_id")

        # Convert properties to JSON-serializable format
        properties = {}
        for key, value in edge_dict.items():
            if isinstance(value, (dict, list)):
                properties[key] = json.dumps(value)
            else:
                properties[key] = value

        # Build property string for Cypher query
        # Use the relation property as the relationship type, default to EDGE
        relation_type = properties.get("relation", "EDGE").upper().replace(" ", "_")

        # Remove relation from properties since we're using it as the label
        filtered_properties = {k: v for k, v in properties.items() if k != "relation"}

        if filtered_properties:
            prop_string = ", ".join(
                [f"{key}: ${key}" for key in filtered_properties.keys()]
            )
            query = f"""
            MATCH (source:Node {{id: $source_id}})
            MATCH (target:Node {{id: $target_id}})
            MERGE (source)-[r:{relation_type} {{{prop_string}}}]->(target)
            """
        else:
            query = f"""
            MATCH (source:Node {{id: $source_id}})
            MATCH (target:Node {{id: $target_id}})
            MERGE (source)-[r:{relation_type}]->(target)
            """

        parameters = {
            "source_id": source_id,
            "target_id": target_id,
            **filtered_properties,
        }
        self._execute_query(query, parameters)

    def add_nodes(self, nodes: List[Node]):
        """Adds a batch of nodes to the database."""
        for node in nodes:
            self.write_node(node)

    def add_edges(self, edges: List[Edge]):
        """Adds a batch of edges to the database."""
        for edge in edges:
            self.write_edge(edge)

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single node by its ID from the database."""
        query = "MATCH (n {id: $node_id}) RETURN n"
        result = self._execute_query(query, {"node_id": node_id})

        if result:
            mg_node = result[0][0]

            # Extract properties from mgclient.Node object
            node_data = self._extract_node_properties(mg_node)

            return {
                "id": node_id,
                "label": node_data.get("label", node_id),
                "attributes": node_data,
            }
        return None

    def get_all_nodes(self) -> List[Node]:
        """Retrieves all nodes from the database as Node objects."""
        query = "MATCH (n) RETURN n"
        results = self._execute_query(query)

        nodes = []
        for result in results:
            mg_node = result[0]

            # Extract properties from mgclient.Node object
            node_data = self._extract_node_properties(mg_node)

            # Ensure we have the required fields for Node construction
            if "node_id" not in node_data and "id" in node_data:
                node_data["node_id"] = node_data["id"]

            try:
                node = Node(**node_data)
                nodes.append(node)
            except Exception as e:
                logger.warning(
                    f"Failed to create Node object from data: {node_data}, error: {e}"
                )
                continue

        return nodes

    def get_nodes_by_type(self, node_type: NodeType) -> List[Dict[str, Any]]:
        """Retrieves all nodes of a specific type."""
        query = "MATCH (n {node_type: $node_type}) RETURN n"
        results = self._execute_query(query, {"node_type": node_type.value})

        nodes = []
        for result in results:
            mg_node = result[0]

            # Extract properties from mgclient.Node object
            node_data = self._extract_node_properties(mg_node)
            node_id = node_data.get("id")

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

        # Convert properties to JSON-serializable format
        properties = {}
        for key, value in attributes.items():
            if isinstance(value, (dict, list)):
                properties[key] = json.dumps(value)
            else:
                properties[key] = value

        # Build property string for Cypher query
        if properties:
            prop_string = ", ".join([f"{key}: ${key}" for key in properties.keys()])
            query = f"""
            MERGE (n {{id: $node_id}})
            SET n += {{{prop_string}}}
            """
        else:
            query = f"""
            MERGE (n {{id: $node_id}})
            """

        parameters = {"node_id": node_id, **properties}
        self._execute_query(query, parameters)

    def add_edge(self, edge_dict: Dict[str, Any]) -> None:
        """Add an edge from a dictionary representation."""
        source = edge_dict["source"]
        target = edge_dict["target"]
        attributes = edge_dict.get("attributes", {})
        label = edge_dict.get("label", "")

        # Include label in attributes if not already there
        if "label" not in attributes:
            attributes["label"] = label

        # Convert properties to JSON-serializable format
        properties = {}
        for key, value in attributes.items():
            if isinstance(value, (dict, list)):
                properties[key] = json.dumps(value)
            else:
                properties[key] = value

        # Build property string for Cypher query
        # Use the relation property as the relationship type, default to EDGE
        relation_type = properties.get("relation", "EDGE").upper().replace(" ", "_")

        # Remove relation from properties since we're using it as the label
        filtered_properties = {k: v for k, v in properties.items() if k != "relation"}

        if filtered_properties:
            prop_string = ", ".join(
                [f"{key}: ${key}" for key in filtered_properties.keys()]
            )
            query = f"""
            MATCH (source {{id: $source}})
            MATCH (target {{id: $target}})
            MERGE (source)-[r:{relation_type} {{{prop_string}}}]->(target)
            """
        else:
            query = f"""
            MATCH (source {{id: $source}})
            MATCH (target {{id: $target}})
            MERGE (source)-[r:{relation_type}]->(target)
            """

        parameters = {"source": source, "target": target, **filtered_properties}
        self._execute_query(query, parameters)

    def get_node_count(self) -> int:
        """Returns the total number of nodes in the database."""
        query = "MATCH (n) RETURN count(n)"
        result = self._execute_query(query)
        return result[0][0] if result else 0

    def get_edge_count(self) -> int:
        """Returns the total number of edges in the database."""
        query = "MATCH ()-[r]->() RETURN count(r)"
        result = self._execute_query(query)
        return result[0][0] if result else 0

    def get_graph(self):
        """
        Returns a representation of the graph.

        Note: Unlike NetworkX, Memgraph doesn't return a single graph object.
        This method returns the connection object for advanced queries.
        """
        return self._connection

    def clear_graph(self):
        """Clear all nodes and edges from the database."""
        query = "MATCH (n) DETACH DELETE n"
        self._execute_query(query)
        logger.info("Graph cleared successfully")

    def _extract_node_properties(self, mg_node) -> Dict[str, Any]:
        """Extract properties from mgclient.Node object."""
        node_data = {}

        try:
            # Try different methods to extract properties from mgclient.Node
            if hasattr(mg_node, "properties"):
                # Method 1: Direct properties attribute
                props = mg_node.properties
            elif hasattr(mg_node, "_properties"):
                # Method 2: Private _properties attribute
                props = mg_node._properties
            elif hasattr(mg_node, "keys") and hasattr(mg_node, "values"):
                # Method 3: Keys and values methods
                props = dict(zip(mg_node.keys(), mg_node.values()))
            else:
                # Method 4: Try to iterate over the node
                props = dict(mg_node)
        except Exception as e:
            logger.warning(f"Failed to extract properties from mgclient.Node: {e}")

            # Fallback: try to access common properties directly
            props = {}
            common_props = [
                "id",
                "node_id",
                "name",
                "node_type",
                "label",
                "properties",
                "context",
            ]
            for prop in common_props:
                try:
                    if hasattr(mg_node, prop):
                        props[prop] = getattr(mg_node, prop)
                except:
                    continue

        # Parse JSON properties back to objects
        for key, value in props.items():
            if isinstance(value, str):
                try:
                    node_data[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    node_data[key] = value  # Keep as string if not valid JSON
            else:
                node_data[key] = value

        return node_data

    def close(self):
        """Close the connection to Memgraph."""
        if self._connection:
            self._connection.close()
            logger.info("Memgraph connection closed")

    def __del__(self):
        """Ensure connection is closed when object is destroyed."""
        self.close()
