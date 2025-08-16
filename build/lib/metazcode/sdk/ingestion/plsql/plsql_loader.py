import logging
from typing import Generator, Tuple, List, Optional

from ..ingestion_tool import IngestionTool
from .plsql_parser import CanonicalPlsqlParser
from ...models.graph import Node, Edge


logger = logging.getLogger(__name__)


class PlsqlLoader(IngestionTool):
    """Discover and orchestrate PL/SQL ETL parsing (pure .sql/.pks/.pkb)."""

    def __init__(self, root_path: str, target_file: Optional[str] = None):
        super().__init__(root_path)
        self.target_file = target_file

    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        parser = CanonicalPlsqlParser()
        files: List[str] = []
        if self.target_file:
            files = [self.target_file]
        else:
            for pattern in ("*.sql", "*.pks", "*.pkb"):
                files.extend(self.discover_files(pattern))

        logger.info(f"Found {len(files)} PL/SQL file(s).")
        for file_path in files:
            try:
                logger.info(f"Parsing PL/SQL file: {file_path}")
                yield from parser.parse(str(file_path))
            except Exception as e:
                logger.error(f"Failed to parse {file_path}: {e}", exc_info=True)
                continue
