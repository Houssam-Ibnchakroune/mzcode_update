"""
Index Builder for Hierarchical Entity Indexing

This module provides utilities for creating, saving, and loading hierarchical
entity indexes. It handles serialization and metadata management for indexes.
"""

from typing import Optional, Dict, List, Any
import pickle
import json
import os
import time
from pathlib import Path

from ..graph.graph_client_interface import GraphClientInterface
from .hierarchical_index import HierarchicalEntityIndex


class IndexBuilder:
    """Builder for creating and managing hierarchical entity indexes."""

    @staticmethod
    def build_index(
        graph_client: GraphClientInterface, project_id: Optional[str] = None
    ) -> HierarchicalEntityIndex:
        """
        Build a new hierarchical entity index from the graph client.

        Args:
            graph_client: Graph client implementing GraphClientInterface
            project_id: Optional project identifier

        Returns:
            New HierarchicalEntityIndex instance
        """
        index = HierarchicalEntityIndex(graph_client)
        if project_id:
            index.set_project_id(project_id)
        return index

    @staticmethod
    def save_index(index: HierarchicalEntityIndex, file_path: str) -> None:
        """
        Save the index to a file using pickle serialization.

        Args:
            index: The index to save
            file_path: Path to save the index file
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)

        with open(file_path, "wb") as f:
            pickle.dump(index, f)

    @staticmethod
    def load_index(file_path: str) -> Optional[HierarchicalEntityIndex]:
        """
        Load an index from a file.

        Args:
            file_path: Path to the index file

        Returns:
            Loaded HierarchicalEntityIndex instance, or None if loading failed
        """
        try:
            with open(file_path, "rb") as f:
                return pickle.load(f)
        except (FileNotFoundError, pickle.UnpicklingError, EOFError) as e:
            # Return None for any loading errors
            return None

    @staticmethod
    def save_index_metadata(index: HierarchicalEntityIndex, metadata_path: str) -> None:
        """
        Save index metadata to a JSON file.

        Args:
            index: The index to extract metadata from
            metadata_path: Path to save the metadata file
        """
        metadata = {
            "project_id": index.get_project_id(),
            "stats": index.get_stats(),
            "created_at": time.time(),
            "index_version": "1.0",
            "implementation_status": "Level 1 & 2 Complete",
        }

        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(metadata_path)), exist_ok=True)

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    @staticmethod
    def list_indexes(index_dir: str) -> List[Dict[str, Any]]:
        """
        List all available indexes in the specified directory.

        Args:
            index_dir: Directory containing index files

        Returns:
            List of metadata dictionaries for each index
        """
        indexes = []
        try:
            for filename in os.listdir(index_dir):
                if filename.endswith(".meta.json"):
                    metadata_path = os.path.join(index_dir, filename)
                    try:
                        with open(metadata_path, "r") as f:
                            metadata = json.load(f)
                            indexes.append(metadata)
                    except (FileNotFoundError, json.JSONDecodeError):
                        # Skip invalid metadata files
                        continue
        except FileNotFoundError:
            # Directory doesn't exist
            pass
        return indexes
