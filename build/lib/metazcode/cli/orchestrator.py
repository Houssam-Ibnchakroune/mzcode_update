from typing import List, Optional
import pkgutil
import inspect

from metazcode.sdk.graph.graph_client_interface import GraphClientInterface
from metazcode.sdk.ingestion.ingestion_tool import IngestionTool


class Orchestrator:
    """
    Discovers and runs all available ingestion tools to populate the graph.
    """

    def __init__(
        self,
        graph_client: GraphClientInterface,
        root_path: str,
        target_file: Optional[str] = None,
    ):
        self.graph_client = graph_client
        self.root_path = root_path
        self.target_file = target_file
        self.loaders: List[IngestionTool] = []

    def discover_loaders(self):
        """
        Finds and instantiates all IngestionTool subclasses.
        """
        import metazcode.sdk.ingestion as ingestion_module

        for _, name, _ in pkgutil.walk_packages(
            ingestion_module.__path__, ingestion_module.__name__ + "."
        ):
            module = __import__(name, fromlist=[""])
            for _, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, IngestionTool) and obj is not IngestionTool:
                    # Check if the loader accepts the 'target_file' argument
                    sig = inspect.signature(obj.__init__)
                    if "target_file" in sig.parameters:
                        self.loaders.append(
                            obj(root_path=self.root_path, target_file=self.target_file)
                        )
                    else:
                        self.loaders.append(obj(root_path=self.root_path))

    def run(self):
        """
        Executes the full ingestion pipeline.
        """
        self.discover_loaders()

        for loader in self.loaders:
            for nodes, edges in loader.ingest():
                if nodes:
                    self.graph_client.add_nodes(nodes)
                if edges:
                    self.graph_client.add_edges(edges)

        print("Ingestion complete.")
        print(
            f"Graph now contains {self.graph_client.get_node_count()} nodes and {self.graph_client.get_edge_count()} edges."
        )
