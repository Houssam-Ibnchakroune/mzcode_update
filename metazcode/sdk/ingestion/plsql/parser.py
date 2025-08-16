import re
from typing import List, Dict, Any, Tuple, Generator
from pathlib import Path
import logging

from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType, EdgeType

logger = logging.getLogger(__name__)


class PlsqlParser:
    """Canonical PL/SQL parser that produces Nodes and Edges similar to SSIS parser."""

    def parse(self, file_path: str) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        nodes: List[Node] = []
        edges: List[Edge] = []
        created_node_ids = set()
        created_edges = set()

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        code = self._remove_comments(content)
        procedures = self._extract_procedures(code)

        package_id = f"pipeline:{Path(file_path).name}"
        if package_id not in created_node_ids:
            nodes.append(Node(node_id=package_id, node_type=NodeType.PIPELINE.value, name=Path(file_path).name,
                              properties={"file": file_path}))
            created_node_ids.add(package_id)

        for proc in procedures:
            proc_id = f"{package_id}:operation:{proc['name']}"
            if proc_id not in created_node_ids:
                nodes.append(Node(node_id=proc_id, node_type=NodeType.OPERATION.value, name=proc['name'],
                                  properties={"file": file_path, "type": proc['type']}))
                created_node_ids.add(proc_id)

                # link to package
                edges.append(Edge(source_id=package_id, target_id=proc_id, relation=EdgeType.CONTAINS.value))

            tables = self._extract_table_references(proc['body'])
            for table, access in tables:
                table_id = f"table:{table}"
                if table_id not in created_node_ids:
                    nodes.append(Node(node_id=table_id, node_type=NodeType.TABLE.value, name=table,
                                      properties={"source_procedure": proc['name']}))
                    created_node_ids.add(table_id)

                rel = EdgeType.READS_FROM.value if access == 'R' else EdgeType.WRITES_TO.value
                edge_key = (proc_id, table_id, rel)
                if edge_key not in created_edges:
                    edges.append(Edge(source_id=proc_id, target_id=table_id, relation=rel))
                    created_edges.add(edge_key)

        yield nodes, edges

    def _remove_comments(self, code: str) -> str:
        code = re.sub(r'--.*', '', code)
        code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        return code

    def _extract_procedures(self, code: str) -> List[Dict[str, Any]]:
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

    def _extract_table_references(self, code: str) -> List[Tuple[str, str]]:
        # returns list of (table_name, access_mode) where access_mode is 'R' or 'W'
        pattern = re.compile(r"\b(SELECT|INSERT|UPDATE|DELETE|MERGE)\b([\s\S]{0,200}?)\b(?:FROM|INTO|JOIN|UPDATE)\s+([\w\"\'\.]+)", re.IGNORECASE)
        matches = pattern.findall(code)
        results = []
        for op, ctx, tbl in matches:
            name = tbl.strip().strip('"').strip("'")
            if '.' in name:
                name = name.split('.')[-1]
            name = re.sub(r"[^0-9A-Za-z_]+$", '', name)
            mode = 'R' if op.upper() == 'SELECT' else 'W'
            results.append((name.upper(), mode))
        # deduplicate keeping write if conflict
        final = {}
        for name, mode in results:
            if name in final and final[name] == 'W':
                continue
            final[name] = mode
        return list(final.items())
