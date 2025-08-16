from pathlib import Path
from typing import Generator, List, Tuple, Dict, Any, Optional
import logging

from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType, EdgeType
from ..ingestion_tool import IngestionTool
from .parser import PlsqlParser

logger = logging.getLogger(__name__)


class PlsqlLoader(IngestionTool):
    """IngestionTool implementation for PL/SQL ETL projects."""

    def __init__(self, root_dir: Optional[str] = None):
        self.root_dir = Path(root_dir) if root_dir else Path('.')

    def discover_files(self, pattern: str = "*.sql") -> List[Path]:
        return list(self.root_dir.rglob(pattern))

    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        plsql_files = self.discover_files("*.sql") + self.discover_files("*.pks") + self.discover_files("*.pkb")
    logger.info(f"Found {len(plsql_files)} PL/SQL file(s).")
    parser = PlsqlParser()
    for file_path in plsql_files:
            try:
                logger.info(f"Parsing file: {file_path}")
                yield from parser.parse(str(file_path))
            except Exception as e:
                logger.error(f"Failed to parse {file_path}: {e}", exc_info=True)
                continue
