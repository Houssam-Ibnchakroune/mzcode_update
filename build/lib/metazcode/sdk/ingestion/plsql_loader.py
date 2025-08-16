"""
PLSQL ETL Loader and Parser for MetaZCode

This module discovers and parses Oracle PL/SQL ETL files (.sql, .pks, .pkb),
extracts procedures, tables, and basic read/write operations, and transforms them
into graph nodes and edges for MetaZCode. It mirrors the SSIS ingestion pattern
and integrates with the Orchestrator via the IngestionTool base class.
"""

import re
from pathlib import Path
from typing import Generator, Tuple, List, Dict, Any, Optional, Set
import logging

from .ingestion_tool import IngestionTool
from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType, EdgeType

logger = logging.getLogger(__name__)

class PlsqlLoader(IngestionTool):
    """
    Loader for Oracle PL/SQL ETL projects. Discovers and orchestrates parsing of all relevant files.
    """
    def __init__(self, root_path: str, target_file: Optional[str] = None):
        super().__init__(root_path)
        self.target_file = target_file

    def _collect_plsql_files(self) -> List[Path]:
        if self.target_file:
            return [Path(self.target_file)]
        files: List[Path] = []
        for pattern in ("*.sql", "*.pks", "*.pkb"):
            files.extend(self.discover_files(pattern))
        # De-duplicate
        return sorted(set(files))

    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        plsql_files = self._collect_plsql_files()
        logger.info(f"Found {len(plsql_files)} PL/SQL file(s).")
        parser = CanonicalPlsqlParser()
        for file_path in plsql_files:
            try:
                logger.info(f"Parsing file: {file_path}")
                yield from parser.parse(str(file_path))
            except Exception as e:
                logger.error(f"Failed to parse {file_path}: {e}", exc_info=True)
                continue

class CanonicalPlsqlParser:
    """
    Parses Oracle PL/SQL ETL files and yields graph nodes and edges.
    """
    def parse(self, file_path: str) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        nodes: List[Node] = []
        edges: List[Edge] = []
        created_node_ids = set()
        created_edges = set()
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        # Remove comments and normalize
        code = self._remove_comments(content)
    # Extract procedures/functions (treat each as an operation)
    procedures = self._extract_procedures(code)
        for proc in procedures:
            proc_id = f"procedure:{proc['name']}"
            if proc_id not in created_node_ids:
                nodes.append(Node(
                    node_id=proc_id,
                    node_type=NodeType.OPERATION.value,
                    name=proc['name'],
                    properties={"file": file_path, "type": proc['type']}
                ))
                created_node_ids.add(proc_id)
            # Extract READ/WRITE table references
            reads, writes = self._extract_read_write_tables(proc['body'])

            for table in sorted(reads | writes):
                table_id = f"table:{table}"
                if table_id not in created_node_ids:
                    nodes.append(Node(
                        node_id=table_id,
                        node_type=NodeType.DATA_ASSET.value,
                        name=table,
                        properties={"discovered_in": file_path}
                    ))
                    created_node_ids.add(table_id)

            for table in sorted(reads):
                edge_key = (proc_id, f"table:{table}", EdgeType.READS_FROM.value)
                if edge_key not in created_edges:
                    edges.append(Edge(
                        source_id=proc_id,
                        target_id=f"table:{table}",
                        relation=EdgeType.READS_FROM.value,
                        properties={}
                    ))
                    created_edges.add(edge_key)

            for table in sorted(writes):
                edge_key = (proc_id, f"table:{table}", EdgeType.WRITES_TO.value)
                if edge_key not in created_edges:
                    edges.append(Edge(
                        source_id=proc_id,
                        target_id=f"table:{table}",
                        relation=EdgeType.WRITES_TO.value,
                        properties={}
                    ))
                    created_edges.add(edge_key)
        # If no procedures/functions found, treat entire file as a single operation
        if not procedures:
            op_name = Path(file_path).stem
            op_id = f"script:{op_name}"
            if op_id not in created_node_ids:
                nodes.append(Node(
                    node_id=op_id,
                    node_type=NodeType.OPERATION.value,
                    name=op_name,
                    properties={"file": file_path, "type": "SCRIPT"}
                ))
                created_node_ids.add(op_id)

            reads, writes = self._extract_read_write_tables(code)
            for table in sorted(reads | writes):
                table_id = f"table:{table}"
                if table_id not in created_node_ids:
                    nodes.append(Node(
                        node_id=table_id,
                        node_type=NodeType.DATA_ASSET.value,
                        name=table,
                        properties={"discovered_in": file_path}
                    ))
                    created_node_ids.add(table_id)

            for table in sorted(reads):
                edge_key = (op_id, f"table:{table}", EdgeType.READS_FROM.value)
                if edge_key not in created_edges:
                    edges.append(Edge(
                        source_id=op_id,
                        target_id=f"table:{table}",
                        relation=EdgeType.READS_FROM.value,
                        properties={}
                    ))
                    created_edges.add(edge_key)

            for table in sorted(writes):
                edge_key = (op_id, f"table:{table}", EdgeType.WRITES_TO.value)
                if edge_key not in created_edges:
                    edges.append(Edge(
                        source_id=op_id,
                        target_id=f"table:{table}",
                        relation=EdgeType.WRITES_TO.value,
                        properties={}
                    ))
                    created_edges.add(edge_key)

        yield nodes, edges

    def _remove_comments(self, code: str) -> str:
        # Remove single-line and multi-line comments
        code = re.sub(r'--.*', '', code)
        code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        return code

    def _extract_procedures(self, code: str) -> List[Dict[str, Any]]:
        # Robust regex for procedures/functions (with or without params)
        pattern = re.compile(r"(PROCEDURE|FUNCTION)\s+(\w+)(?:\s*\((.*?)\))?\s*(?:IS|AS)(.*?)(?:END\s+\2\b)", re.IGNORECASE | re.DOTALL)
        matches = pattern.finditer(code)
        procedures = []
        for match in matches:
            procedures.append({
                "type": match.group(1).upper(),
                "name": match.group(2),
                "params": match.group(3) or "",
                "body": match.group(4) or ""
            })
        return procedures

    def _extract_table_references(self, code: str) -> List[str]:
        # Find table names in SQL statements (SELECT, INSERT, UPDATE, DELETE, MERGE)
        pattern = re.compile(r"\b(?:FROM|INTO|UPDATE|JOIN|MERGE)\s+([\w\"\'\.]+)", re.IGNORECASE)
        raw = pattern.findall(code)
        cleaned: Set[str] = set()
        for r in raw:
            # Remove surrounding quotes/brackets and handle schema.table
            name = r.strip().strip('"').strip("'")
            if '.' in name:
                name = name.split('.')[-1]
            # remove trailing punctuation
            name = re.sub(r"[^0-9A-Za-z_]+$", '', name)
            if name:
                cleaned.add(name.upper())
        return list(cleaned)

    def _extract_read_write_tables(self, code: str) -> Tuple[Set[str], Set[str]]:
        """Extract read (SELECT/JOIN) and write (INSERT/UPDATE/DELETE/MERGE) targets."""
        reads: Set[str] = set()
        writes: Set[str] = set()

        def _norm(name: str) -> str:
            n = name.strip().strip('"').strip("'")
            if '.' in n:
                n = n.split('.')[-1]
            n = re.sub(r"[^0-9A-Za-z_]+$", '', n)
            return n.upper()

        # Reads (FROM, JOIN)
        for m in re.findall(r"\b(?:FROM|JOIN)\s+([\w\"\'\.]+)", code, flags=re.IGNORECASE):
            n = _norm(m)
            if n:
                reads.add(n)

        # INSERT INTO target
        for m in re.findall(r"\bINSERT\s+INTO\s+([\w\"\'\.]+)", code, flags=re.IGNORECASE):
            n = _norm(m)
            if n:
                writes.add(n)

        # UPDATE target
        for m in re.findall(r"\bUPDATE\s+([\w\"\'\.]+)", code, flags=re.IGNORECASE):
            n = _norm(m)
            if n:
                writes.add(n)

        # DELETE FROM target
        for m in re.findall(r"\bDELETE\s+FROM\s+([\w\"\'\.]+)", code, flags=re.IGNORECASE):
            n = _norm(m)
            if n:
                writes.add(n)

        # MERGE INTO target
        for m in re.findall(r"\bMERGE\s+INTO\s+([\w\"\'\.]+)", code, flags=re.IGNORECASE):
            n = _norm(m)
            if n:
                writes.add(n)

        return reads, writes

# Example usage:
# loader = PlsqlLoader(root_dir="/path/to/plsql/project")
# for nodes, edges in loader.ingest():
#     ... # process graph
