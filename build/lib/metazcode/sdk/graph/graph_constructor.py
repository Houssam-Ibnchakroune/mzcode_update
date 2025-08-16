import logging
from typing import Optional
from metazcode.sdk.graph.graph_client_interface import GraphClientInterface
from metazcode.sdk.graph.client_nx import NetworkXGraphClient
from metazcode.sdk.models.config import DatabaseConfig

logger = logging.getLogger(__name__)


class GraphClientBuilder:
    """
    Constructs and returns a configured graph client.

    This builder provides a single entry point for creating graph clients,
    hiding the complexity of which backend is being used. It supports both
    NetworkX and Memgraph backends based on configuration.
    """

    @staticmethod
    def get_client(config: Optional[DatabaseConfig] = None) -> GraphClientInterface:
        """
        Factory method to get the configured graph client.
        
        Args:
            config: Database configuration. If None, loads from environment.
            
        Returns:
            GraphClientInterface: Configured graph client instance.
            
        Raises:
            ValueError: If invalid backend is specified.
            ConnectionError: If Memgraph backend is requested but unavailable.
        """
        if config is None:
            config = DatabaseConfig.from_environment()
        
        if config.backend == "networkx":
            logger.info("Using NetworkX backend for graph storage")
            return NetworkXGraphClient()
        
        elif config.backend == "memgraph":
            logger.info("Using analytics-ready Memgraph backend for graph storage")
            try:
                from metazcode.sdk.graph.analytics_ready_client import AnalyticsReadyMemgraphClient
                return AnalyticsReadyMemgraphClient(config)
            except ImportError as e:
                logger.error(f"Memgraph dependencies not installed: {e}")
                logger.info("Falling back to NetworkX backend")
                return NetworkXGraphClient()
            except Exception as e:
                logger.error(f"Failed to connect to Memgraph: {e}")
                logger.info("Falling back to NetworkX backend")
                return NetworkXGraphClient()
        
        else:
            raise ValueError(f"Unsupported backend: {config.backend}")

    @staticmethod
    def validate_connection(config: Optional[DatabaseConfig] = None) -> bool:
        """
        Validate that the configured backend is available and accessible.
        
        Args:
            config: Database configuration. If None, loads from environment.
            
        Returns:
            bool: True if connection is valid, False otherwise.
        """
        if config is None:
            config = DatabaseConfig.from_environment()
            
        if config.backend == "networkx":
            return True
            
        elif config.backend == "memgraph":
            try:
                from metazcode.sdk.graph.analytics_ready_client import AnalyticsReadyMemgraphClient
                client = AnalyticsReadyMemgraphClient(config)
                return client.test_connection()
            except Exception as e:
                logger.error(f"Memgraph connection validation failed: {e}")
                return False
                
        return False
