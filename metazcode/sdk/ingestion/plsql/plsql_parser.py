import re
import logging
from pathlib import Path
from typing import Generator, List, Tuple, Set, Optional, Dict, Any

from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType, EdgeType
from .sql_semantics import EnhancedPlsqlParser, SqlSemantics, create_join_edges_from_semantics
from .type_mapping import PLSQLDataTypeMapper, detect_column_types_from_sql, TargetPlatform
from ...models.traceability import SourceContext

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

    # Basic DDL detection (CREATE TABLE ...). We keep it simple and robust.
    CREATE_TABLE_RE = re.compile(r"\bcreate\s+table\s+([a-zA-Z0-9_\.\$#\"]+)", re.IGNORECASE | re.DOTALL)

    CALL_RE = re.compile(r"\b([a-zA-Z0-9_\$#]+)\s*\(", re.IGNORECASE)

    IDENT_RE = re.compile(r"\b([a-zA-Z][a-zA-Z0-9_\$#]*)\b")
    
    # Enhanced patterns for better analysis
    CURSOR_RE = re.compile(r"\bcursor\s+([a-zA-Z0-9_\$#]+)\s+is\s+(.*?)(?=\bopen\b|\bfor\b|;)", re.IGNORECASE | re.DOTALL)
    FUNCTION_CALL_RE = re.compile(r"\b(ROUND|AVG|SUM|COUNT|MAX|MIN|SUBSTR|TO_DATE|TO_CHAR|NVL|DECODE)\s*\(", re.IGNORECASE)
    
    # Task/operation detection patterns
    TASK_COMMENT_RE = re.compile(r"--\s*(.*?(?:task|step|phase|load|extract|transform|analyze).*?)$", re.IGNORECASE | re.MULTILINE)
    BLOCK_COMMENT_RE = re.compile(r"/\*\s*(.*?(?:task|step|phase|load|extract|transform|analyze).*?)\s*\*/", re.IGNORECASE | re.DOTALL)
    
    # Error handling patterns
    ERROR_HANDLING_RE = re.compile(r"\b(exception\s+when|raise_application_error|sqlcode|sqlerrm|others\s+then)\b", re.IGNORECASE)
    
    # Common Oracle functions that should not be considered as tables
    ORACLE_FUNCTIONS = {
        'round', 'avg', 'sum', 'count', 'max', 'min', 'substr', 'to_date', 'to_char', 
        'nvl', 'decode', 'coalesce', 'trim', 'ltrim', 'rtrim', 'upper', 'lower',
        'sysdate', 'current_date', 'cast', 'extract', 'trunc', 'ceil', 'floor',
        'abs', 'mod', 'power', 'sqrt', 'sign', 'length', 'instr', 'replace',
        'translate', 'lpad', 'rpad', 'soundex', 'ascii', 'chr', 'initcap'
    }

    def __init__(self, 
                 connections_context: Optional[Dict[str, Dict[str, Any]]] = None,
                 parameters_context: Optional[Dict[str, Dict[str, Any]]] = None,
                 enable_type_mapping: bool = True,
                 target_platforms: Optional[List[str]] = None):
        """
        Initialize the PL/SQL parser with enhanced configuration matching SSIS approach.
        
        Args:
            connections_context: Oracle connection context from tnsnames.ora, etc.
            parameters_context: Oracle parameter context from config files
            enable_type_mapping: Whether to enable Oracle->target platform type mapping
            target_platforms: List of target platforms for type conversion
        """
        self.connections_context = connections_context or {}
        self.parameters_context = parameters_context or {}
        self.enable_type_mapping = enable_type_mapping
        self.target_platforms = self._parse_target_platforms(target_platforms or ["sql_server", "postgresql"])
        
        # Initialize enhanced SQL parser and type mapper
        self.sql_parser = EnhancedPlsqlParser()
        self.sql_semantics = self.sql_parser  # Alias for backward compatibility
        self.type_mapper = PLSQLDataTypeMapper() if enable_type_mapping else None
        
        # Store connection and parameter contexts for node creation
        self.connections = connections_context
        self.parameters = parameters_context
        
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        
        self.validation_report = {
            'total_files_processed': 0,
            'fake_tables_rejected': 0,
            'keyword_aliases_removed': 0,
            'joins_normalized': 0,
            'sql_strings_repaired': 0,
            'inline_views_processed': 0
        }

    def _parse_target_platforms(self, target_platforms: List[str]) -> List[TargetPlatform]:
        """Parse target platform strings to TargetPlatform enum values."""
        platforms = []
        for platform_str in target_platforms:
            try:
                platforms.append(TargetPlatform(platform_str))
            except ValueError:
                logger.warning(f"Unknown target platform: {platform_str}")
        return platforms

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
            # Extra tokens seen in DDL/comments/prompts and noise identifiers
            "table","column","row","add","comment","prompt","source",
        }
        return name.lower() in reserved

    def _is_fake_table_node(self, table_name: str) -> bool:
        """Check if this is a fake table node that should be rejected (Fix A)."""
        return table_name == "(" or table_name.startswith("(")
    
    def _create_fqn_table(self, table_name: str, schema: str = None, service: str = "oracle", database: str = "default") -> str:
        """Create Fully Qualified Name for tables: service.db.schema.table"""
        # Clean table name from quotes and whitespace
        clean_table = table_name.strip('"').strip("'").strip()
        
        # If table already contains dots, parse it
        if '.' in clean_table:
            parts = clean_table.split('.')
            if len(parts) == 2:
                schema = parts[0]
                clean_table = parts[1]
            elif len(parts) >= 3:
                # Already fully qualified
                return clean_table
        
        # Default schema if not provided
        if not schema:
            schema = "public"
            
        return f"{service}.{database}.{schema}.{clean_table}"
    
    def _create_fqn_pipeline(self, pipeline_name: str, service: str = "oracle") -> str:
        """Create Fully Qualified Name for pipelines: pipelineService/tasks"""
        clean_name = pipeline_name.strip()
        return f"{service}/{clean_name}"
    
    def _clean_expression(self, expression: str) -> str:
        """Clean SQL expressions and group complex functions"""
        if not expression:
            return expression
            
        # Remove extra whitespace
        cleaned = ' '.join(expression.split())
        
        # Group complete ROUND(AVG(...),2) type expressions first (most specific)
        round_avg_pattern = r'ROUND\s*\(\s*AVG\s*\([^)]+\)\s*,\s*\d+\s*\)'
        if re.search(round_avg_pattern, cleaned, re.IGNORECASE):
            cleaned = re.sub(round_avg_pattern, 'ROUNDED_AVERAGE', cleaned, flags=re.IGNORECASE)
            
        # Group other complete complex expressions
        complex_patterns = [
            (r'ROUND\s*\(\s*SUM\s*\([^)]+\)\s*,\s*\d+\s*\)', 'ROUNDED_SUM'),
            (r'ROUND\s*\(\s*COUNT\s*\([^)]+\)\s*,\s*\d+\s*\)', 'ROUNDED_COUNT'),
            (r'TO_DATE\s*\([^)]+\)', 'DATE_CONVERSION'),
            (r'TO_CHAR\s*\([^)]+\)', 'CHAR_CONVERSION'),
            (r'EXTRACT\s*\([^)]+\)', 'DATE_PART_EXTRACTION'),
            (r'NVL\s*\([^,]+,\s*[^)]+\)', 'NULL_VALUE_REPLACEMENT'),
            (r'DECODE\s*\([^)]+\)', 'CONDITIONAL_LOGIC'),
            (r'COUNT\s*\([^)]+\)\s+\w+', 'AGGREGATE_WITH_ALIAS'),  # COUNT(measurement) numberOfMeasurements
            (r'AVG\s*\([^)]+\)\s+\w+', 'AVERAGE_WITH_ALIAS'),
            (r'SUM\s*\([^)]+\)\s+\w+', 'SUM_WITH_ALIAS')
        ]
        
        for pattern, replacement in complex_patterns:
            cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
            
        return cleaned
    
    def _is_oracle_function(self, name: str) -> bool:
        """Check if the name is a known Oracle function that should not be treated as a table"""
        return name.lower() in self.ORACLE_FUNCTIONS
    
    def _extract_task_name_from_block(self, block: str, file_path: str) -> str:
        """Extract meaningful task name from SQL block comments or content"""
        # Try to find task description in comments first
        task_matches = self.TASK_COMMENT_RE.findall(block)
        if task_matches:
            return task_matches[0].strip()
            
        block_matches = self.BLOCK_COMMENT_RE.findall(block)
        if block_matches:
            return block_matches[0].strip()
        
        # Look for CURSOR names as task indicators
        cursor_matches = self.CURSOR_RE.findall(block)
        if cursor_matches:
            return f"cursor_processing_{cursor_matches[0][0]}"
        
        # Look for specific SQL patterns to identify task type
        if re.search(r'\binsert\s+into\b', block, re.IGNORECASE):
            if re.search(r'\bselect\b', block, re.IGNORECASE):
                return "data_load_task"
            else:
                return "data_insert_task"
        elif re.search(r'\bupdate\b', block, re.IGNORECASE):
            return "data_update_task"
        elif re.search(r'\bcreate\s+table\b', block, re.IGNORECASE):
            return "table_creation_task"
        elif re.search(r'\bselect\b.*\bfrom\b', block, re.IGNORECASE):
            if re.search(r'\bgroup\s+by\b', block, re.IGNORECASE):
                return "data_analysis_task"
            else:
                return "data_query_task"
        
        # Default fallback
        return "anonymous_block"
    
    def _detect_error_handling(self, block: str) -> Dict[str, Any]:
        """Detect error handling patterns in PL/SQL block"""
        error_info = {
            "has_error_handling": False,
            "error_patterns": [],
            "error_outputs": []
        }
        
        if self.ERROR_HANDLING_RE.search(block):
            error_info["has_error_handling"] = True
            
            # Find specific error handling patterns
            patterns = self.ERROR_HANDLING_RE.findall(block)
            error_info["error_patterns"] = [p.strip() for p in patterns]
            
            # Look for error logging or output
            if re.search(r'\bdbms_output\.put_line\b', block, re.IGNORECASE):
                error_info["error_outputs"].append("DBMS_OUTPUT")
            if re.search(r'\binsert\s+into\s+\w*log\w*', block, re.IGNORECASE):
                error_info["error_outputs"].append("ERROR_LOG_TABLE")
            if re.search(r'\braise_application_error\b', block, re.IGNORECASE):
                error_info["error_outputs"].append("APPLICATION_ERROR")
                
        return error_info
    
    def _extract_cursor_column_lineage(self, block: str) -> List[Dict[str, Any]]:
        """Extract column lineage from cursor definitions and LOAD operations"""
        lineage = []
        
        # Find cursor definitions
        for match in self.CURSOR_RE.finditer(block):
            cursor_name = match.group(1)
            cursor_sql = match.group(2)
            
            # Parse the cursor SQL to extract column mappings
            try:
                if self.sql_semantics:
                    semantics = self.sql_semantics.parse_sql_semantics(cursor_sql)
                    if semantics:
                        # Extract column information
                        input_columns = []
                        output_columns = []
                        
                        # Get source tables and columns
                        for table in semantics.tables:
                            input_columns.append({
                                "table_name": table.name,
                                "schema": table.schema or "public",
                                "fqn": self._create_fqn_table(table.name, table.schema)
                            })
                        
                        # Get selected columns (avoid SELECT *)
                        for col in semantics.columns:
                            if col.column_name and col.column_name != '*':
                                output_columns.append({
                                    "column_name": col.column_name,
                                    "alias": col.alias,
                                    "expression": self._clean_expression(col.expression),
                                    "source_table": col.source_table
                                })
                        
                        if input_columns or output_columns:
                            lineage.append({
                                "cursor_name": cursor_name,
                                "operation_type": "CURSOR_LOAD",
                                "input_columns": input_columns,
                                "output_columns": output_columns,
                                "source_sql": cursor_sql.strip()
                            })
                            
            except Exception as e:
                self.logger.warning(f"Failed to parse cursor {cursor_name}: {e}")
                
        return lineage
    
    def _extract_comprehensive_column_lineage(self, block: str, sql_statements: List[str]) -> List[Dict[str, Any]]:
        """Extract comprehensive column lineage from all SQL statements"""
        lineage = []
        
        for sql_stmt in sql_statements:
            try:
                if self.sql_semantics:
                    semantics = self.sql_semantics.parse_sql_semantics(sql_stmt)
                    if semantics and semantics.columns:
                        for col in semantics.columns:
                            # Clean and normalize expression
                            clean_expr = self._clean_expression(col.expression) if col.expression else col.column_name
                            
                            # Determine transformation type
                            transformation_type = "DIRECT"
                            if clean_expr and clean_expr != col.column_name:
                                if any(func in clean_expr.upper() for func in ['ROUNDED_', 'DATE_CONVERSION', 'AGGREGATE_']):
                                    transformation_type = "TRANSFORMED"
                                elif col.alias:
                                    transformation_type = "DERIVED"
                                else:
                                    transformation_type = "COMPUTED"
                            
                            lineage_entry = {
                                "source_expression": col.expression or col.column_name,
                                "cleaned_expression": clean_expr,
                                "target_column": col.alias or col.column_name or "unknown",
                                "transformation_type": transformation_type,
                                "sql_statement": sql_stmt[:100] + "..." if len(sql_stmt) > 100 else sql_stmt,
                                "source_table": col.source_table,
                                "source_alias": col.source_alias
                            }
                            lineage.append(lineage_entry)
                            
            except Exception as e:
                self.logger.warning(f"Failed to extract lineage from statement: {e}")
                
        return lineage

    def _validate_node_for_serialization(self, node: Node) -> bool:
        """Validate that a node should be serialized (Loader Rules)."""
        # Reject fake table nodes
        if node.node_type == NodeType.TABLE.value and self._is_fake_table_node(node.name):
            self.validation_report['fake_tables_rejected'] += 1
            logger.debug(f"Rejected fake table node: {node.name}")
            return False
        
        # Reject Oracle functions treated as tables
        if node.node_type == NodeType.TABLE.value and self._is_oracle_function(node.name):
            self.validation_report['oracle_functions_rejected'] = self.validation_report.get('oracle_functions_rejected', 0) + 1
            logger.debug(f"Rejected Oracle function as table: {node.name}")
            return False
        
        # Reject nodes with keyword aliases in properties
        if 'sql_semantics' in node.properties:
            sql_semantics = node.properties['sql_semantics']
            if isinstance(sql_semantics, list):
                for semantics in sql_semantics:
                    # Handle both dict and object types
                    if hasattr(semantics, 'to_dict'):
                        # It's a SqlSemantics object
                        semantics_dict = semantics.to_dict()
                        for table in semantics_dict.get('tables', []):
                            if table.get('alias') and table['alias'].lower() in self.sql_parser.SQL_KEYWORDS:
                                self.validation_report['keyword_aliases_removed'] += 1
                                logger.debug(f"Removed keyword alias: {table['alias']}")
                                table['alias'] = None
                    elif isinstance(semantics, dict):
                        # It's already a dictionary
                        for table in semantics.get('tables', []):
                            if table.get('alias') and table['alias'].lower() in self.sql_parser.SQL_KEYWORDS:
                                self.validation_report['keyword_aliases_removed'] += 1
                                logger.debug(f"Removed keyword alias: {table['alias']}")
                                table['alias'] = None
        
        return True

    def _validate_edge_for_serialization(self, edge: Edge) -> bool:
        """Validate that an edge should be serialized (Loader Rules)."""
        # Reject edges from/to fake table nodes
        if (edge.source_id.startswith("table::(") or 
            edge.target_id.startswith("table::(")):
            logger.debug(f"Rejected edge from/to fake table: {edge.source_id} -> {edge.target_id}")
            return False
        
        # Reject edges from/to Oracle functions treated as tables
        source_table = edge.source_id.replace("table::", "") if edge.source_id.startswith("table::") else ""
        target_table = edge.target_id.replace("table::", "") if edge.target_id.startswith("table::") else ""
        
        if (source_table and self._is_oracle_function(source_table)) or (target_table and self._is_oracle_function(target_table)):
            logger.debug(f"Rejected edge from/to Oracle function: {edge.source_id} -> {edge.target_id}")
            return False
        
        return True

    def _normalize_schema_in_semantics(self, semantics_dict: dict) -> dict:
        """Fix E: Normalize schema fields in semantics."""
        for table in semantics_dict.get('tables', []):
            if table.get('schema') == "":
                table['schema'] = None
        
        for join in semantics_dict.get('joins', []):
            for table_ref in ['left_table', 'right_table']:
                if table_ref in join and join[table_ref].get('schema') == "":
                    join[table_ref]['schema'] = None
        
        return semantics_dict

    def _resolve_inline_view_lineage(self, semantics_list: List[dict]) -> List[Edge]:
        """Fix F: Generate lineage edges for inline views to base tables."""
        lineage_edges = []
        
        for semantics in semantics_list:
            inline_views = semantics.get('inline_views', [])
            joins = semantics.get('joins', [])
            
            # For each join involving an inline view, create edges to base tables
            for join in joins:
                left_table = join.get('left_table', {})
                right_table = join.get('right_table', {})
                
                # Check if either side is an inline view
                for inline_view in inline_views:
                    iv_alias = inline_view.get('alias')
                    base_tables = inline_view.get('base_tables', [])
                    
                    # If left table is inline view alias, resolve to base tables
                    if (left_table.get('name') == iv_alias or left_table.get('alias') == iv_alias):
                        for base_table in base_tables:
                            if not self._is_fake_table_node(base_table):
                                lineage_edges.append(Edge(
                                    source_id=f"table::{base_table}",
                                    target_id=f"table::{right_table.get('name')}",
                                    relation=EdgeType.REFERENCES.value,
                                    properties={
                                        "join_type": join.get("join_type", "INNER JOIN"),
                                        "condition": join.get("condition"),
                                        "relationship_type": "inline_view_lineage",
                                        "inline_view_alias": iv_alias
                                    }
                                ))
                    
                    # If right table is inline view alias, resolve to base tables  
                    if (right_table.get('name') == iv_alias or right_table.get('alias') == iv_alias):
                        for base_table in base_tables:
                            if not self._is_fake_table_node(base_table):
                                lineage_edges.append(Edge(
                                    source_id=f"table::{left_table.get('name')}",
                                    target_id=f"table::{base_table}",
                                    relation=EdgeType.REFERENCES.value,
                                    properties={
                                        "join_type": join.get("join_type", "INNER JOIN"),
                                        "condition": join.get("condition"),
                                        "relationship_type": "inline_view_lineage",
                                        "inline_view_alias": iv_alias
                                    }
                                ))
        
        return lineage_edges

    def get_validation_report(self) -> dict:
        """Get comprehensive validation report for regression testing."""
        parser_report = self.sql_parser.get_validation_report()
        combined_report = self.validation_report.copy()
        combined_report.update(parser_report)
        return combined_report

    def _extract_tables_from_select(self, sql: str) -> Set[str]:
        result: Set[str] = set()
        for m in self.SELECT_RE.finditer(sql):
            name = m.group(1).strip('"')
            if not self._is_reserved(name) and not self._is_oracle_function(name):
                result.add(name)
        return result

    def _extract_tables_from_dml(self, sql: str) -> Tuple[Set[str], Set[str]]:
        writes = set()
        reads = set()
        for m in self.INSERT_RE.finditer(sql):
            name = m.group(1).strip('"')
            if not self._is_reserved(name) and not self._is_oracle_function(name):
                writes.add(name)
        for m in self.UPDATE_RE.finditer(sql):
            name = m.group(1).strip('"')
            if not self._is_reserved(name) and not self._is_oracle_function(name):
                writes.add(name)
        for m in self.MERGE_RE.finditer(sql):
            name = m.group(1).strip('"')
            if not self._is_reserved(name) and not self._is_oracle_function(name):
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

    def _make_node(self, node_id: str, node_type: NodeType, name: str, properties=None, file_path: str = "") -> Node:
        """Create a node with enhanced traceability and type mapping enrichment."""
        node_props = properties or {}
        
        # Add FQN based on node type
        if node_type == NodeType.TABLE:
            schema = node_props.get('schema', 'public')
            fqn = self._create_fqn_table(name, schema)
            node_props['fqn'] = fqn
            node_props['qualified_name'] = fqn
        elif node_type == NodeType.PIPELINE:
            fqn = self._create_fqn_pipeline(name)
            node_props['fqn'] = fqn
            node_props['qualified_name'] = fqn
        
        # Add enhanced traceability like SSIS
        if file_path:
            node_props.update(SourceContext.create_node_traceability(
                source_file_path=file_path,
                source_file_type="sql",
                xml_path=f"//plsql_operation[@name='{name}']",
                technology="ORACLE"
            ))
        
        # Add technology marker
        node_props["technology"] = "ORACLE"
        
        # Add type mapping for table nodes if we can detect column information
        if node_type == NodeType.TABLE and 'create_statement' in node_props:
            try:
                column_types = detect_column_types_from_sql(node_props['create_statement'])
                if column_types:
                    node_props['type_mapping'] = {
                        'columns': column_types,
                        'source_platform': 'oracle',
                        'target_platforms': [p.value for p in self.target_platforms],
                        'mapping_confidence': self._calculate_mapping_confidence(column_types)
                    }
                    
                    # Add platform support flags like SSIS
                    node_props['supported_platforms'] = [p.value for p in self.target_platforms]
                    node_props['type_mapping_enabled'] = True
            except Exception as e:
                logger.debug(f"Could not extract type mapping for table {name}: {e}")
        
        return Node(node_id=node_id, node_type=node_type.value, name=name, properties=node_props)
    
    def _calculate_mapping_confidence(self, column_types: List[Dict[str, Any]]) -> float:
        """Calculate overall mapping confidence based on column type conversion confidence."""
        if not column_types:
            return 0.0
        
        confidences = [col.get('conversion_confidence', 0.5) for col in column_types]
        return sum(confidences) / len(confidences)
    
    def _categorize_operation_subtype(self, sql_statements: List[str]) -> str:
        """Categorize operation subtype based on SQL patterns (matching SSIS approach)."""
        has_select = any('select' in stmt.lower() for stmt in sql_statements)
        has_insert = any('insert' in stmt.lower() for stmt in sql_statements)
        has_update = any('update' in stmt.lower() for stmt in sql_statements)
        has_merge = any('merge' in stmt.lower() for stmt in sql_statements)
        has_create = any('create table' in stmt.lower() for stmt in sql_statements)
        has_procedure = any('create procedure' in stmt.lower() or 'create function' in stmt.lower() for stmt in sql_statements)
        
        # Match SSIS operation subtypes
        if has_procedure:
            return "EXECUTE"  # PL/SQL procedures/functions are execution operations
        elif has_merge:
            return "DATA_FLOW"  # MERGE operations are data flow
        elif has_create:
            return "EXECUTE"   # DDL operations are execute operations
        elif has_insert and has_select:
            return "DATA_FLOW"  # INSERT..SELECT is data flow (like ETL)
        elif has_update:
            return "DATA_FLOW"  # UPDATE operations are data flow
        elif has_insert:
            return "DATA_FLOW"  # INSERT operations are data flow
        elif has_select:
            return "DATA_FLOW"  # SELECT operations are data flow
        else:
            return "EXECUTE"    # Default to execute (like SSIS script tasks)
    
    def _extract_column_lineage(self, sql_statements: List[str]) -> List[Dict[str, Any]]:
        """Extract column-level lineage information."""
        lineage = []
        
        for stmt in sql_statements:
            try:
                # Basic column lineage extraction using regex patterns
                # This is a simplified version - more sophisticated parsing would use AST
                select_pattern = r'select\s+(.*?)\s+from'
                match = re.search(select_pattern, stmt.lower(), re.IGNORECASE | re.DOTALL)
                
                if match:
                    columns_text = match.group(1)
                    
                    # Extract individual columns (simplified)
                    column_items = [col.strip() for col in columns_text.split(',')]
                    
                    for col_item in column_items:
                        if col_item and col_item != '*':
                            # Handle aliased columns: column AS alias or column alias
                            as_pattern = r'(.+?)\s+as\s+(\w+)'
                            as_match = re.search(as_pattern, col_item, re.IGNORECASE)
                            
                            if as_match:
                                source_expr = as_match.group(1).strip()
                                target_column = as_match.group(2).strip()
                            else:
                                # Simple column name
                                parts = col_item.split()
                                if len(parts) >= 2 and not any(op in parts[1].lower() for op in ['from', 'where', 'group', 'order']):
                                    source_expr = parts[0]
                                    target_column = parts[1]
                                else:
                                    source_expr = col_item
                                    target_column = col_item.split('.')[-1] if '.' in col_item else col_item
                            
                            lineage.append({
                                'source_expression': source_expr,
                                'target_column': target_column,
                                'transformation_type': 'DIRECT' if source_expr == target_column else 'DERIVED',
                                'sql_statement': stmt[:100] + '...' if len(stmt) > 100 else stmt
                            })
            
            except Exception as e:
                logger.debug(f"Could not extract column lineage from statement: {e}")
                continue
        
        return lineage
    
    def _get_table_properties_with_type_mapping(self, table_name: str, block: str) -> Dict[str, Any]:
        """
        Get table properties with type mapping if available.
        
        Args:
            table_name: Name of the table
            block: SQL block that might contain CREATE TABLE statement
            
        Returns:
            Dictionary with table properties including type mapping if found
        """
        table_props = {}
        
        # Try to find CREATE TABLE statement for this table in the block using balanced parentheses
        create_start_pattern = rf"create\s+table\s+{re.escape(table_name)}\s*\("
        start_match = re.search(create_start_pattern, block, re.IGNORECASE)
        
        if start_match:
            start_pos = start_match.start()
            # Find the matching closing parenthesis
            paren_count = 0
            pos = start_match.end() - 1  # Start at the opening parenthesis
            
            for i in range(pos, len(block)):
                if block[i] == '(':
                    paren_count += 1
                elif block[i] == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        # Found the matching closing parenthesis
                        end_pos = i + 1
                        # Look for optional semicolon
                        if end_pos < len(block) and block[end_pos:end_pos+1].strip() == ';':
                            end_pos += 1
                        create_statement = block[start_pos:end_pos].strip()
                        break
            else:
                create_statement = None
                
            if create_statement:
                table_props['create_statement'] = create_statement
                
                # Extract column type information
                try:
                    column_types = detect_column_types_from_sql(create_statement)
                    if column_types:
                        table_props['type_mapping'] = {
                            'columns': column_types,
                            'source_platform': 'oracle',
                            'target_platforms': [
                                TargetPlatform.SQL_SERVER.value,
                                TargetPlatform.POSTGRESQL.value,
                                TargetPlatform.MYSQL.value
                            ],
                            'mapping_confidence': self._calculate_mapping_confidence(column_types)
                        }
                except Exception as e:
                    logger.debug(f"Could not extract type mapping for table {table_name}: {e}")
        else:
            # If no CREATE TABLE found, add basic metadata indicating it's a referenced table
            table_props['table_source'] = 'referenced'
            table_props['type_mapping_available'] = False
            
        return table_props

    def _make_edge(self, source: str, target: str, relation: EdgeType, properties=None, file_path: str = "") -> Edge:
        """Create an edge with enhanced traceability like SSIS."""
        edge_props = properties or {}
        
        # Add traceability information like SSIS
        if file_path:
            edge_props.update(SourceContext.create_edge_traceability(
                source_file_path=file_path,
                derivation_method="sql_parsing",
                xml_location="//plsql_statement",
                context_info=SourceContext.create_sql_derivation_context(
                    sql_statement="PL/SQL operation",
                    component_type="PL/SQL Parser",
                    property_name="relation_detection"
                ),
                confidence_level="high",
                technology="ORACLE"
            ))
        
        return Edge(source_id=source, target_id=target, relation=relation.value, properties=edge_props)

    def parse(self, file_path: str) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        path = Path(file_path)
        text = path.read_text(encoding="utf-8", errors="ignore")
        raw = self._strip_comments(text)
        
        self.validation_report['total_files_processed'] += 1

        ops = self._detect_operations(raw)
        # If no explicit BEGIN/PROC, but file contains DML/DDL, treat entire file as an anonymous operation
        if not ops:
            if any(r.search(raw) for r in (self.SELECT_RE, self.INSERT_RE, self.UPDATE_RE, self.MERGE_RE, self.CREATE_TABLE_RE)):
                ops = [("anonymous_block", (0, len(raw)))]
            else:
                return

        # Track tables created with type mapping at file level to prevent override
        file_created_tables: Set[str] = set()
        # Track assets at file level to avoid duplicates across operations
        file_seen_assets: Set[str] = set()

        for op_name, (start, end) in ops:
            block = raw[start:end]
            op_id = f"plsql::{path.name}::{op_name}"
            pipeline_id = f"plsql::{path.name}"
            
            # Extract SQL statements for analysis
            sql_statements = self._extract_sql_statements(block)
            
            # Get meaningful task name instead of generic "anonymous_block"
            task_name = self._extract_task_name_from_block(block, str(path))
            actual_op_name = task_name if task_name != "anonymous_block" else op_name
            
            # Update operation ID with meaningful name
            op_id = f"plsql::{path.name}::{actual_op_name}"
            
            # Categorize operation subtype
            operation_subtype = self._categorize_operation_subtype(sql_statements)
            
            # Extract comprehensive column lineage (all statements + cursors)
            column_lineage = self._extract_comprehensive_column_lineage(block, sql_statements)
            
            # Extract cursor column lineage for LOAD operations
            cursor_lineage = self._extract_cursor_column_lineage(block)
            
            # Detect error handling
            error_info = self._detect_error_handling(block)
            
            # Create operation node with enhanced metadata
            operation_properties = {
                "file": str(path),
                "operation_subtype": operation_subtype,
                "sql_statements_count": len(sql_statements),
                "block_size": end - start,
                "task_name": actual_op_name,
                "has_explicit_task_name": task_name != "anonymous_block"
            }
            
            # Add comprehensive column lineage
            if column_lineage:
                operation_properties["column_lineage"] = column_lineage
                operation_properties["total_column_mappings"] = len(column_lineage)
            
            # Add cursor lineage for LOAD operations
            if cursor_lineage:
                operation_properties["cursor_lineage"] = cursor_lineage
                operation_properties["has_load_operations"] = True
                operation_properties["cursor_count"] = len(cursor_lineage)
            
            # Add error handling information
            if error_info["has_error_handling"]:
                operation_properties["error_handling"] = error_info
            
            nodes: List[Node] = [
                self._make_node(op_id, NodeType.OPERATION, actual_op_name, operation_properties, str(path))
            ]
            
            # Add a pipeline node per file and contains edge
            pipeline_properties = {
                "file": str(path),
                "technology": "ORACLE"
            }
            nodes.append(self._make_node(pipeline_id, NodeType.PIPELINE, path.stem, pipeline_properties, str(path)))
            edges: List[Edge] = []
            edges.append(self._make_edge(pipeline_id, op_id, EdgeType.CONTAINS, file_path=str(path)))

            # PHASE 1: Process CREATE TABLE statements first for this operation
            for m in self.CREATE_TABLE_RE.finditer(block):
                tname = m.group(1).strip('"')
                if not self._is_fake_table_node(tname):
                    asset_id = f"table::{tname}"
                    
                    # Extract CREATE TABLE statement for type analysis using balanced parentheses
                    table_properties = {}
                    create_start_pattern = rf"create\s+table\s+{re.escape(tname)}\s*\("
                    start_match = re.search(create_start_pattern, block, re.IGNORECASE)
                    
                    if start_match:
                        start_pos = start_match.start()
                        # Find the matching closing parenthesis
                        paren_count = 0
                        pos = start_match.end() - 1  # Start at the opening parenthesis
                        
                        for i in range(pos, len(block)):
                            if block[i] == '(':
                                paren_count += 1
                            elif block[i] == ')':
                                paren_count -= 1
                                if paren_count == 0:
                                    # Found the matching closing parenthesis
                                    end_pos = i + 1
                                    # Look for optional semicolon
                                    if end_pos < len(block) and block[end_pos:end_pos+1].strip() == ';':
                                        end_pos += 1
                                    create_statement = block[start_pos:end_pos].strip()
                                    break
                        else:
                            create_statement = None
                            
                        if create_statement:
                            table_properties['create_statement'] = create_statement
                            table_properties['table_source'] = 'CREATE TABLE statement'
                            
                            # Extract column type information
                            try:
                                column_types = detect_column_types_from_sql(create_statement)
                                if column_types:
                                    table_properties['type_mapping'] = {
                                        'columns': column_types,
                                        'source_platform': 'oracle',
                                        'target_platforms': [
                                            TargetPlatform.SQL_SERVER.value,
                                            TargetPlatform.POSTGRESQL.value,
                                            TargetPlatform.MYSQL.value
                                        ],
                                        'mapping_confidence': self._calculate_mapping_confidence(column_types)
                                    }
                                    logger.debug(f"Created type mapping for table {tname} with {len(column_types)} columns")
                            except Exception as e:
                                logger.debug(f"Could not extract type mapping for table {tname}: {e}")
                    
                    # Always create the table node for CREATE TABLE statements
                    if asset_id not in file_seen_assets:
                        nodes.append(self._make_node(asset_id, NodeType.TABLE, tname, table_properties))
                        file_seen_assets.add(asset_id)
                    
                    # Mark this table as created with type mapping (file-level protection)
                    file_created_tables.add(tname)
                    
                    # Treat creation as a write to the asset with enhanced traceability
                    edge_properties = {
                        "operation_type": "CREATE_TABLE",
                        "source_file": str(path),
                        "line_number": start_match.start() if start_match else 0
                    }
                    edges.append(self._make_edge(op_id, asset_id, EdgeType.WRITES_TO, edge_properties, str(path)))

            # PHASE 2: Process table references, but only if not already created with type mapping

            # Reads from plain SELECTs in the block
            for tbl in self._extract_tables_from_select(block):
                if not self._is_fake_table_node(tbl) and not self._is_oracle_function(tbl):  # Enhanced filtering
                    asset_id = f"table::{tbl}"
                    if asset_id not in file_seen_assets and tbl not in file_created_tables:
                        # Enhanced table properties with type mapping and metadata
                        table_props = {
                            'table_source': 'referenced',
                            'schema': '',
                            'operation_type': 'SELECT',
                            'sql_semantics': {},
                            'cleaned_expressions': []
                        }
                        
                        # Use SQLGlot to analyze SQL semantics if available
                        if self.sql_semantics:
                            try:
                                semantics = self.sql_semantics.parse_sql_semantics(block)
                                if semantics:
                                    # Clean expressions in columns
                                    cleaned_columns = []
                                    for col in semantics.columns:
                                        if col.expression:
                                            cleaned_expr = self._clean_expression(col.expression)
                                            table_props['cleaned_expressions'].append({
                                                'original': col.expression,
                                                'cleaned': cleaned_expr,
                                                'column_name': col.column_name or 'unknown'
                                            })
                                        cleaned_columns.append(col)
                                    
                                    table_props["sql_semantics"] = {
                                        "joins": [{"left_table": j.left_table, "right_table": j.right_table, "join_type": j.join_type} for j in semantics.joins],
                                        "tables": semantics.tables,
                                        "columns": cleaned_columns,
                                        "where_clause": semantics.where_clause
                                    }
                            except Exception as e:
                                self.logger.warning(f"Failed to analyze SQL semantics for {tbl}: {e}")
                        
                        nodes.append(self._make_node(asset_id, NodeType.TABLE, tbl, table_props, str(path)))
                        file_seen_assets.add(asset_id)
                    edges.append(self._make_edge(op_id, asset_id, EdgeType.READS_FROM, file_path=str(path)))

            # DML: reads and writes
            reads, writes = self._extract_tables_from_dml(block)
            for tbl in reads:
                if not self._is_fake_table_node(tbl) and not self._is_oracle_function(tbl):  # Enhanced filtering
                    asset_id = f"table::{tbl}"
                    if asset_id not in file_seen_assets and tbl not in file_created_tables:
                        # Enhanced table properties for DML reads
                        table_props = {
                            'table_source': 'referenced',
                            'schema': '',
                            'operation_type': 'DML_READ',
                            'sql_semantics': {},
                            'cleaned_expressions': []
                        }
                        
                        # Use SQLGlot to analyze SQL semantics if available
                        if self.sql_semantics:
                            try:
                                semantics = self.sql_semantics.parse_sql_semantics(block)
                                if semantics:
                                    table_props["sql_semantics"] = {
                                        "joins": [{"left_table": j.left_table, "right_table": j.right_table, "join_type": j.join_type} for j in semantics.joins],
                                        "tables": semantics.tables,
                                        "columns": semantics.columns,
                                        "where_clause": semantics.where_clause
                                    }
                            except Exception as e:
                                self.logger.warning(f"Failed to analyze SQL semantics for {tbl}: {e}")
                        
                        nodes.append(self._make_node(asset_id, NodeType.TABLE, tbl, table_props, str(path)))
                        file_seen_assets.add(asset_id)
                    edges.append(self._make_edge(op_id, asset_id, EdgeType.READS_FROM, file_path=str(path)))

            for tbl in writes:
                if not self._is_fake_table_node(tbl) and not self._is_oracle_function(tbl):  # Enhanced filtering
                    asset_id = f"table::{tbl}"
                    if asset_id not in file_seen_assets and tbl not in file_created_tables:
                        # Enhanced table properties for DML writes
                        table_props = {
                            'table_source': 'referenced',
                            'schema': '',
                            'operation_type': 'DML_WRITE',
                            'sql_semantics': {},
                            'cleaned_expressions': []
                        }
                        
                        # Use SQLGlot to analyze SQL semantics if available
                        if self.sql_semantics:
                            try:
                                semantics = self.sql_semantics.parse_sql_semantics(block)
                                if semantics:
                                    table_props["sql_semantics"] = {
                                        "joins": [{"left_table": j.left_table, "right_table": j.right_table, "join_type": j.join_type} for j in semantics.joins],
                                        "tables": semantics.tables,
                                        "columns": semantics.columns,
                                        "where_clause": semantics.where_clause
                                    }
                            except Exception as e:
                                self.logger.warning(f"Failed to analyze SQL semantics for {tbl}: {e}")
                        
                        nodes.append(self._make_node(asset_id, NodeType.TABLE, tbl, table_props, str(path)))
                        file_seen_assets.add(asset_id)
                    edges.append(self._make_edge(op_id, asset_id, EdgeType.WRITES_TO, file_path=str(path)))

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
                # Skip in DDL and comment/prompt contexts
                if ("create " in left_ctx) or ("alter " in left_ctx) or ("comment " in left_ctx) or ("prompt " in left_ctx):
                    continue
                callee_id = f"plsql::{callee}"
                nodes.append(self._make_node(callee_id, NodeType.OPERATION, callee))
                edges.append(self._make_edge(op_id, callee_id, EdgeType.DEPENDS_ON))

            # Enhanced SQL semantics using the new parser with all fixes
            semantics_list = []
            for sql_stmt in self._extract_sql_statements(block):
                try:
                    # Use enhanced PL/SQL parser with all defect fixes
                    semantics = self.sql_parser.parse_sql_semantics(sql_stmt)
                    if semantics and semantics.tables:
                        semantics_dict = semantics.to_dict()
                        
                        # Apply Fix E: Schema normalization
                        semantics_dict = self._normalize_schema_in_semantics(semantics_dict)
                        
                        semantics_list.append(semantics_dict)
                        
                        # Ensure table nodes exist (excluding fake tables)
                        for t in semantics_dict.get("tables", []):
                            tname = t.get("name")
                            if tname and not self._is_fake_table_node(tname):
                                asset_id = f"table::{tname}"
                                if asset_id not in file_seen_assets:
                                    # Only create reference nodes for tables not already created with type mapping
                                    if tname not in file_created_tables:
                                        table_props = {'table_source': 'referenced'}
                                    else:
                                        table_props = {}  # Table already created with full type mapping
                                    nodes.append(self._make_node(asset_id, NodeType.TABLE, tname, table_props))
                                    file_seen_assets.add(asset_id)
                                # Add reads_from relationship for all tables in SQL semantics
                                edges.append(self._make_edge(op_id, asset_id, EdgeType.READS_FROM))
                        
                        # Create unidirectional join/reference edges (avoiding duplicates per SSIS pattern)
                        processed_join_pairs = set()  # Track (left, right) pairs to avoid duplicates
                        
                        for j in semantics_dict.get("joins", []):
                            left = j.get("left_table", {}).get("name")
                            right = j.get("right_table", {}).get("name")
                            if (left and right and 
                                not self._is_fake_table_node(left) and 
                                not self._is_fake_table_node(right)):
                                left_id = f"table::{left}"
                                right_id = f"table::{right}"
                                
                                # Ensure table nodes exist
                                if left_id not in file_seen_assets:
                                    if left not in file_created_tables:
                                        table_props = {'table_source': 'referenced'}
                                    else:
                                        table_props = {}
                                    nodes.append(self._make_node(left_id, NodeType.TABLE, left, table_props))
                                    file_seen_assets.add(left_id)
                                if right_id not in file_seen_assets:
                                    if right not in file_created_tables:
                                        table_props = {'table_source': 'referenced'}
                                    else:
                                        table_props = {}
                                    nodes.append(self._make_node(right_id, NodeType.TABLE, right, table_props))
                                    file_seen_assets.add(right_id)
                                
                                # Create normalized join pair key to avoid bidirectional duplicates
                                # Use consistent direction: always left→right as specified in join
                                join_pair = (left, right)
                                
                                if join_pair not in processed_join_pairs:
                                    # Create single unidirectional edge from left to right
                                    edge = self._make_edge(
                                        left_id,
                                        right_id,
                                        EdgeType.REFERENCES,
                                        properties={
                                            "join_type": j.get("join_type", "INNER JOIN"),
                                            "condition": j.get("condition"),
                                            "relationship_type": "join_relationship",
                                        },
                                    )
                                    if self._validate_edge_for_serialization(edge):
                                        edges.append(edge)
                                        processed_join_pairs.add(join_pair)
                        
                        # Fix F: Add inline view lineage edges
                        lineage_edges = self._resolve_inline_view_lineage([semantics_dict])
                        for edge in lineage_edges:
                            if self._validate_edge_for_serialization(edge):
                                edges.append(edge)

                except Exception as e:
                    logger.error(f"Failed to parse SQL semantics for statement: {sql_stmt[:100]}... Error: {e}")
                    continue

            # Attach aggregated semantics to operation node (with validation)
            if semantics_list:
                for n in nodes:
                    if n.node_id == op_id:
                        n.properties["sql_semantics"] = semantics_list
                        break

            # Add connection nodes from context if available (similar to SSIS)
            if hasattr(self, 'connections') and self.connections:
                for conn_id, conn_info in self.connections.items():
                    connection_node = self._make_node(
                        f"connection::{conn_id}", 
                        NodeType.CONNECTION, 
                        conn_id, 
                        {
                            "connection_string": conn_info.get("connection_string", ""),
                            "database": conn_info.get("database", ""),
                            "server": conn_info.get("server", ""),
                            "port": conn_info.get("port", ""),
                            "technology": "ORACLE"
                        },
                        str(path)
                    )
                    nodes.append(connection_node)
                    
                    # Connect pipeline to connection
                    edge = self._make_edge(
                        pipeline_id, 
                        f"connection::{conn_id}", 
                        EdgeType.USES, 
                        {"connection_type": "oracle"},
                        str(path)
                    )
                    edges.append(edge)

            # Add parameter nodes from context if available (similar to SSIS)
            if hasattr(self, 'parameters') and self.parameters:
                for param_name, param_info in self.parameters.items():
                    parameter_node = self._make_node(
                        f"parameter::{param_name}", 
                        NodeType.PARAMETER, 
                        param_name, 
                        {
                            "parameter_value": param_info.get("value", ""),
                            "parameter_type": param_info.get("type", "VARCHAR2"),
                            "source_file": param_info.get("source_file", str(path)),
                            "technology": "ORACLE"
                        },
                        str(path)
                    )
                    nodes.append(parameter_node)
                    
                    # Connect pipeline to parameter
                    edge = self._make_edge(
                        pipeline_id, 
                        f"parameter::{param_name}", 
                        EdgeType.USES, 
                        {"parameter_usage": "oracle_define"},
                        str(path)
                    )
                    edges.append(edge)

            # Apply validation rules before yielding
            validated_nodes = [n for n in nodes if self._validate_node_for_serialization(n)]
            validated_edges = [e for e in edges if self._validate_edge_for_serialization(e)]

            # Deduplicate nodes/edges by id tuples
            uniq_nodes = { (n.node_id, n.node_type): n for n in validated_nodes }.values()
            uniq_edges = { (e.source_id, e.target_id, e.relation): e for e in validated_edges }.values()

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
                # Normalize SELECT ... INTO ... FROM ... to SELECT ... FROM ...
                patt = re.compile(r"(select\s+.*?)\s+into\s+.+?\s+(from\s+)", re.IGNORECASE | re.DOTALL)
                return [re.sub(patt, r"\\1 \\2", s) for s in stmts]

        # Fallback regex-based extraction
        text = " ".join(block.split())
        pattern = re.compile(r"SELECT\s+.+?\s+FROM\s+.+?(?=;|$)", re.IGNORECASE)
        stmts = [m.group(0) for m in pattern.finditer(text)]
        # Normalize SELECT ... INTO ...
        patt = re.compile(r"(select\s+.*?)\s+into\s+.+?\s+(from\s+)", re.IGNORECASE | re.DOTALL)
        return [re.sub(patt, r"\\1 \\2", s) for s in stmts]

    def _sqlglot_tables_and_joins(self, sql: str) -> Optional[Tuple[Set[str], List[Tuple[str, str, str]]]]:
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
        joins: List[Tuple[str, str, str]] = []

        # Collect all table names
        for t in root.find_all(exp.Table):
            if t.this:
                tables.add(t.this.name)

        # Collect joins
        for j in root.find_all(exp.Join):
            # Try to resolve left and right tables from join expression
            left = None
            right = None
            # Right table is usually in j.this (a Table or Subquery)
            if isinstance(j.this, exp.Table) and j.this.this:
                right = j.this.this.name
            # Left table might be the last table in FROM or previous join chain
            select_ = j.find_ancestor(exp.Select)
            from_ = select_.args.get("from") if select_ else None
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
