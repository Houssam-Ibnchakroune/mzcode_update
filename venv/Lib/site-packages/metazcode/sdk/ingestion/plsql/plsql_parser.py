import re
import logging
from pathlib import Path
from typing import Generator, List, Tuple, Set, Optional

from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType, EdgeType
from ..ssis.sql_semantics import EnhancedSqlParser

try:
    # sqlglot provides robust SQL parsing & AST; we'll use it to extract tables and joins when possible
    import sqlglot
    from sqlglot import exp
    _HAS_SQLGLOT = True
except Exception:  # pragma: no cover - optional dependency
    _HAS_SQLGLOT = False

logger = logging.getLogger(__name__)


class CanonicalPlsqlParser:
    """Lightweight PL/SQL parser to emit canonical Nodes/Edges.

    Scope: heuristic regex-based extraction for procedures/functions and DML.
    - OPERATION nodes for procedures/functions/anonymous blocks
    - DATA_ASSET nodes for tables referenced
    - Edges: READS_FROM, WRITES_TO, CALLS
    """

    PROC_RE = re.compile(r"\b(create|replace)\s+(or\s+replace\s+)?(procedure|function)\s+([a-zA-Z0-9_\$#]+)", re.IGNORECASE)
    BEGIN_RE = re.compile(r"\bbegin\b", re.IGNORECASE)
    END_RE = re.compile(r"\bend\b", re.IGNORECASE)

    SELECT_RE = re.compile(r"\bfrom\s+([a-zA-Z0-9_\.\$#\"]+)", re.IGNORECASE)
    INSERT_RE = re.compile(r"\binsert\s+into\s+([a-zA-Z0-9_\.\$#\"]+)", re.IGNORECASE)
    UPDATE_RE = re.compile(r"\bupdate\s+([a-zA-Z0-9_\.\$#\"]+)\b", re.IGNORECASE)
    MERGE_RE = re.compile(r"\bmerge\s+into\s+([a-zA-Z0-9_\.\$#\"]+)", re.IGNORECASE)

    CALL_RE = re.compile(r"\b([a-zA-Z0-9_\$#]+)\s*\(", re.IGNORECASE)

    IDENT_RE = re.compile(r"\b([a-zA-Z][a-zA-Z0-9_\$#]*)\b")

    def __init__(self):
        self.sql_parser = EnhancedSqlParser()

    def _strip_comments(self, text: str) -> str:
        # Remove /* */ and -- comments
        text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
        text = re.sub(r"--.*?$", " ", text, flags=re.M)
        return text

    @staticmethod
    def _is_reserved(name: str) -> bool:
        reserved = {
            # SQL keywords
            "select","from","insert","update","delete","merge","into","values","where","and","or","not","exists","in","between","like",
            "join","inner","left","right","full","outer","on","group","order","by","having","union","all","distinct","case","when","then","else","end",
            # Oracle pseudo tables/columns
            "dual","rownum",
            # Built-ins and types commonly seen in ETL scripts
            "to_date","to_char","substr","extract","nvl","decode","coalesce","trim","ltrim","rtrim","upper","lower","sysdate","current_date","cast",
            "varchar2","number","date","char","timestamp","with","as","is","begin","declare","loop","for","while","commit","rollback",
            # DDL words that can precede '(' like PRIMARY KEY (...)
            "primary","key","constraint","references","unique","check","foreign","using","set","of",
        }
        return name.lower() in reserved

    def _extract_tables_from_select(self, sql: str) -> Set[str]:
        result: Set[str] = set()
        for m in self.SELECT_RE.finditer(sql):
            name = m.group(1).strip('"')
            if not self._is_reserved(name):
                result.add(name)
        return result

    def _extract_tables_from_dml(self, sql: str) -> Tuple[Set[str], Set[str]]:
        writes = set()
        reads = set()
        for m in self.INSERT_RE.finditer(sql):
            name = m.group(1).strip('"')
            if not self._is_reserved(name):
                writes.add(name)
        for m in self.UPDATE_RE.finditer(sql):
            name = m.group(1).strip('"')
            if not self._is_reserved(name):
                writes.add(name)
        for m in self.MERGE_RE.finditer(sql):
            name = m.group(1).strip('"')
            if not self._is_reserved(name):
                writes.add(name)
        # FROM clauses could appear in INSERT ... SELECT or MERGE
        reads |= self._extract_tables_from_select(sql)
        return reads, writes

    def _detect_operations(self, text: str) -> List[Tuple[str, Tuple[int, int]]]:
        ops: List[Tuple[str, Tuple[int, int]]] = []
        # Named procedures/functions
        for m in self.PROC_RE.finditer(text):
            name = m.group(4)
            ops.append((name, (m.start(), len(text))))  # until EOF heuristically
        # Anonymous block fallback
        if not ops and self.BEGIN_RE.search(text):
            ops.append(("anonymous_block", (0, len(text))))
        return ops

    def _make_node(self, node_id: str, node_type: NodeType, name: str, properties=None) -> Node:
        return Node(node_id=node_id, node_type=node_type.value, name=name, properties=properties or {})

    def _make_edge(self, source: str, target: str, relation: EdgeType, properties=None) -> Edge:
        return Edge(source_id=source, target_id=target, relation=relation.value, properties=properties or {})

    def parse(self, file_path: str) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        path = Path(file_path)
        text = path.read_text(encoding="utf-8", errors="ignore")
        raw = self._strip_comments(text)

        ops = self._detect_operations(raw)
        if not ops:
            return

        for op_name, (start, end) in ops:
            block = raw[start:end]
            op_id = f"plsql::{path.name}::{op_name}"
            pipeline_id = f"plsql::{path.name}"
            nodes: List[Node] = [
                self._make_node(op_id, NodeType.OPERATION, op_name, {"file": str(path)})
            ]
            # Add a pipeline node per file and contains edge
            nodes.append(self._make_node(pipeline_id, NodeType.PIPELINE, path.stem, {"file": str(path)}))
            edges: List[Edge] = []
            edges.append(self._make_edge(pipeline_id, op_id, EdgeType.CONTAINS))
            seen_assets: Set[str] = set()

            # Reads from plain SELECTs in the block
            for tbl in self._extract_tables_from_select(block):
                asset_id = f"table::{tbl}"
                if asset_id not in seen_assets:
                    nodes.append(self._make_node(asset_id, NodeType.TABLE, tbl))
                    seen_assets.add(asset_id)
                edges.append(self._make_edge(op_id, asset_id, EdgeType.READS_FROM))

            # DML: reads and writes
            reads, writes = self._extract_tables_from_dml(block)
            for tbl in reads:
                asset_id = f"table::{tbl}"
                if asset_id not in seen_assets:
                    nodes.append(self._make_node(asset_id, NodeType.TABLE, tbl))
                    seen_assets.add(asset_id)
                edges.append(self._make_edge(op_id, asset_id, EdgeType.READS_FROM))

            for tbl in writes:
                asset_id = f"table::{tbl}"
                if asset_id not in seen_assets:
                    nodes.append(self._make_node(asset_id, NodeType.TABLE, tbl))
                    seen_assets.add(asset_id)
                edges.append(self._make_edge(op_id, asset_id, EdgeType.WRITES_TO))

            # Procedure calls inside block (heuristic)
            tables_in_block = {t.lower() for t in self._extract_tables_from_select(block)} | {t.lower() for t in reads} | {t.lower() for t in writes}
            for m in self.CALL_RE.finditer(block):
                callee = m.group(1)
                low = callee.lower()
                # Skip SQL keywords, built-ins, and any identifier that matches a table name in this block
                if self._is_reserved(low) or low in tables_in_block:
                    continue
                # Skip if immediately preceded by DML patterns like 'insert into <name>(' etc.
                left_ctx = block[max(0, m.start() - 40):m.start()].lower()
                if ("insert into" in left_ctx) or ("update" in left_ctx) or ("merge into" in left_ctx) or ("delete from" in left_ctx):
                    continue
                callee_id = f"plsql::{callee}"
                nodes.append(self._make_node(callee_id, NodeType.OPERATION, callee))
                edges.append(self._make_edge(op_id, callee_id, EdgeType.DEPENDS_ON))

            # Enhanced SQL semantics (SSIS-style) for SELECT statements
            semantics_list = []
            for sql_stmt in self._extract_sql_statements(block):
                used_any = False
                # First try the existing EnhancedSqlParser
                try:
                    semantics = self.sql_parser.parse_sql_semantics(sql_stmt)
                    if semantics and semantics.tables:
                        used_any = True
                        semantics_list.append(semantics.to_dict())
                        # Ensure table nodes exist
                        for t in semantics.tables:
                            asset_id = f"table::{t.name}"
                            if asset_id not in seen_assets:
                                nodes.append(self._make_node(asset_id, NodeType.TABLE, t.name))
                                seen_assets.add(asset_id)
                        # Create join/reference edges between tables
                        for j in semantics.joins:
                            left_id = f"table::{j.left_table.name}"
                            right_id = f"table::{j.right_table.name}"
                            # Ensure nodes exist (defensive)
                            if left_id not in seen_assets:
                                nodes.append(self._make_node(left_id, NodeType.TABLE, j.left_table.name))
                                seen_assets.add(left_id)
                            if right_id not in seen_assets:
                                nodes.append(self._make_node(right_id, NodeType.TABLE, j.right_table.name))
                                seen_assets.add(right_id)
                            edges.append(self._make_edge(
                                left_id,
                                right_id,
                                EdgeType.REFERENCES,
                                properties={
                                    "join_type": j.join_type.value,
                                    "condition": j.condition,
                                    "relationship_type": "join_relationship",
                                },
                            ))
                except Exception:
                    # Ignore parser failures and try sqlglot next
                    pass

                # Fallback: use sqlglot to extract tables and join relationships
                if not used_any:
                    tj = self._sqlglot_tables_and_joins(sql_stmt)
                    if tj:
                        used_any = True
                        tables, joins = tj
                        # Attach a minimal semantics object
                        if tables:
                            semantics_list.append({
                                "dialect": "oracle",
                                "tables": [{"name": t} for t in sorted(tables)],
                                "joins": [
                                    {"left_table": {"name": l}, "right_table": {"name": r}, "condition": cond, "join_type": "UNKNOWN"}
                                    for (l, r, cond) in joins
                                ],
                            })
                        for t in tables:
                            asset_id = f"table::{t}"
                            if asset_id not in seen_assets:
                                nodes.append(self._make_node(asset_id, NodeType.TABLE, t))
                                seen_assets.add(asset_id)
                        for (l, r, cond) in joins:
                            left_id = f"table::{l}"
                            right_id = f"table::{r}"
                            if left_id not in seen_assets:
                                nodes.append(self._make_node(left_id, NodeType.TABLE, l))
                                seen_assets.add(left_id)
                            if right_id not in seen_assets:
                                nodes.append(self._make_node(right_id, NodeType.TABLE, r))
                                seen_assets.add(right_id)
                            edges.append(self._make_edge(
                                left_id,
                                right_id,
                                EdgeType.REFERENCES,
                                properties={
                                    "join_type": "UNKNOWN",
                                    "condition": cond,
                                    "relationship_type": "join_relationship",
                                },
                            ))

            # Attach aggregated semantics to operation node
            if semantics_list:
                for n in nodes:
                    if n.node_id == op_id:
                        n.properties["sql_semantics"] = semantics_list
                        break

            # Deduplicate nodes/edges by id tuples
            uniq_nodes = { (n.node_id, n.node_type): n for n in nodes }.values()
            uniq_edges = { (e.source_id, e.target_id, e.relation): e for e in edges }.values()

            yield list(uniq_nodes), list(uniq_edges)

    def _extract_sql_statements(self, block: str) -> List[str]:
        """Extract SQL SELECT statements from a PL/SQL block.

        Prefer parsing PL/SQL segments via sqlglot (Oracle dialect). Fallback to regex.
        """
        # Quick escape if block contains no SELECT keyword
        if "select" not in block.lower():
            return []

        if _HAS_SQLGLOT:
            stmts: List[str] = []
            # Split by semicolon; sqlglot struggles with PL/SQL blocks, so we try per statement
            for piece in filter(None, [p.strip() for p in block.split(";")]):
                if "select" not in piece.lower() or " from " not in piece.lower():
                    continue
                try:
                    # Attempt to parse as a single SELECT (Oracle dialect)
                    parsed = sqlglot.parse_one(piece, read="oracle")
                    # Only keep if it's a SELECT or contains one
                    if isinstance(parsed, exp.Select) or parsed.find(exp.Select):
                        stmts.append(piece)
                except Exception:
                    # Ignore parse failures, we'll fallback to regex later
                    pass
            if stmts:
                return stmts

        # Fallback regex-based extraction
        text = " ".join(block.split())
        pattern = re.compile(r"SELECT\s+.+?\s+FROM\s+.+?(?=;|$)", re.IGNORECASE)
        return [m.group(0) for m in pattern.finditer(text)]

    def _sqlglot_tables_and_joins(self, sql: str) -> Optional[Tuple[Set[str], list[Tuple[str, str, str]]]]:
        """Parse SQL with sqlglot to extract table names and join pairs with conditions.

        Returns (tables, joins) where joins is a list of (left, right, condition_sql).
        """
        if not _HAS_SQLGLOT:
            return None
        try:
            root = sqlglot.parse_one(sql, read="oracle")
        except Exception:
            return None

        tables: Set[str] = set()
        joins: list[Tuple[str, str, str]] = []

        # Collect all table names
        for t in root.find_all(exp.Table):
            if t.this:
                tables.add(t.this.name)

        # Collect joins
        for j in root.find_all(exp.Join):
            left_tbl = j.find_ancestor(exp.Select)
            # Try to resolve left and right tables from join expression
            left = None
            right = None
            # Right table is usually in j.this (a Table or Subquery)
            if isinstance(j.this, exp.Table) and j.this.this:
                right = j.this.this.name
            # Left table might be the last table in FROM or previous join chain
            from_ = j.find_ancestor(exp.Select).args.get("from") if j.find_ancestor(exp.Select) else None
            if from_:
                # Walk FROM expressions to find first/base table
                for e in from_.find_all(exp.Table):
                    if e.this:
                        left = e.this.name
                        break
            cond_sql = j.args.get("on")
            cond_str = cond_sql.sql(dialect="oracle") if isinstance(cond_sql, exp.Expression) else None
            if left and right:
                joins.append((left, right, cond_str or ""))
        return tables, joins
