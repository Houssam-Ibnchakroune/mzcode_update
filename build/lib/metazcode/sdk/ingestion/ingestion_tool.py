from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Generator, Tuple

from metazcode.sdk.models.graph import Node, Edge


class IngestionTool(ABC):
    """Abstract base class for all ingestion tools."""

    def __init__(self, root_path: str):
        self.root_path = Path(root_path)
        if not self.root_path.is_dir():
            raise ValueError(
                f"The provided root path '{root_path}' is not a valid directory."
            )

    def discover_files(self, file_pattern: str) -> List[Path]:
        """
        Discover all files in the root_path that match a given pattern.
        """
        return list(self.root_path.rglob(file_pattern))

    @abstractmethod
    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Main method to ingest a project.
        """
        raise NotImplementedError
