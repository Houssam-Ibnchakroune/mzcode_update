"""
Index Integration Module

Provides coordination between ingestion and indexing components while
maintaining complete separation and isolation between them.
"""

import os
import logging
from typing import Optional, Dict, Any
from pathlib import Path

from ..graph.graph_constructor import GraphClientBuilder
from ...cli.orchestrator import Orchestrator
from ..indexing.ssis_enhanced_index import SSISEnhancedHierarchicalIndex
from ..indexing.index_builder import IndexBuilder

logger = logging.getLogger(__name__)


class IndexConfiguration:
    """Configuration for indexing operations."""
    
    DEFAULT_INDEX_OUTPUT_DIR = ".metazcode/indexes"
    DEFAULT_INDEX_FILENAME = "ssis_index.pkl"
    DEFAULT_METADATA_FILENAME = "ssis_index.meta.json"


class IndexIntegration:
    """
    Coordinates ingestion and indexing without coupling the components.
    
    This class uses existing ingestion components as black boxes and builds
    indexes from the populated graph without modifying any existing functionality.
    """
    
    def __init__(self):
        self.config = IndexConfiguration()
        
    def ingest_and_index(
        self, 
        path: str, 
        index_output: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run both ingestion and indexing together.
        
        Args:
            path: Path to ingest from
            index_output: Optional path to save index file
            project_id: Optional project identifier
            
        Returns:
            Dictionary with ingestion and indexing results
        """
        logger.info(f"Starting integrated ingestion and indexing for: {path}")
        
        # Step 1: Run existing ingestion logic (unchanged)
        logger.info("Step 1: Running ingestion...")
        graph_client = self._run_ingestion(path)
        
        # Step 2: Build enhanced index from populated graph
        logger.info("Step 2: Building enhanced SSIS index...")
        index = self._build_enhanced_index(graph_client, project_id)
        
        # Step 3: Save index if requested
        index_path = None
        metadata_path = None
        if index_output:
            logger.info(f"Step 3: Saving index to {index_output}")
            index_path, metadata_path = self._save_index(index, index_output)
        
        # Step 4: Generate results summary
        results = self._generate_results_summary(graph_client, index, index_path, metadata_path)
        
        logger.info("Integrated ingestion and indexing completed successfully")
        return results
    
    def _run_ingestion(self, path: str):
        """
        Run existing ingestion logic without any modifications.
        
        This uses the existing MetaZenseOrchestrator as a black box.
        """
        # Use existing graph client builder (unchanged)
        graph_client = GraphClientBuilder.get_client()
        
        # Use existing orchestrator (unchanged)
        orchestrator = Orchestrator(
            graph_client=graph_client,
            root_path=path
        )
        orchestrator.run()
        
        logger.info("Ingestion completed successfully")
        return graph_client
    
    def _build_enhanced_index(self, graph_client, project_id: Optional[str]):
        """
        Build enhanced SSIS index from the populated graph.
        
        This operates on the graph after ingestion is complete,
        without modifying any ingestion logic.
        """
        try:
            # Build enhanced SSIS index
            index = SSISEnhancedHierarchicalIndex(graph_client)
            
            if project_id:
                index.set_project_id(project_id)
            
            # Get index statistics
            stats = index.get_stats()
            logger.info(f"Index built successfully: {stats}")
            
            return index
            
        except Exception as e:
            logger.error(f"Failed to build index: {e}")
            raise
    
    def _save_index(self, index, index_output: str):
        """Save index and metadata to files."""
        try:
            # Ensure output directory exists
            output_path = Path(index_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save index
            IndexBuilder.save_index(index, str(output_path))
            index_path = str(output_path)
            
            # Save metadata
            metadata_path = str(output_path.with_suffix('.meta.json'))
            IndexBuilder.save_index_metadata(index, metadata_path)
            
            logger.info(f"Index saved to: {index_path}")
            logger.info(f"Metadata saved to: {metadata_path}")
            
            return index_path, metadata_path
            
        except Exception as e:
            logger.error(f"Failed to save index: {e}")
            raise
    
    def _generate_results_summary(
        self, 
        graph_client, 
        index, 
        index_path: Optional[str], 
        metadata_path: Optional[str]
    ) -> Dict[str, Any]:
        """Generate comprehensive results summary."""
        
        # Get graph statistics
        graph_stats = {
            "node_count": graph_client.get_node_count(),
            "edge_count": graph_client.get_edge_count()
        }
        
        # Get index statistics
        index_stats = index.get_stats()
        
        return {
            "ingestion_results": graph_stats,
            "index_results": index_stats,
            "files_created": {
                "index_file": index_path,
                "metadata_file": metadata_path
            },
            "status": "success"
        }