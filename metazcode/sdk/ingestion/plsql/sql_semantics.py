#!/usr/bin/env python3
"""
Enhanced SQL Semantics Parser for PL/SQL Migration Support

This module provides enhanced SQL parsing capabilities using SQLGlot to capture complete
SQL semantics including JOIN relationships, column aliases, and query structure
for PL/SQL to target platform migration.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
from enum import Enum

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.expressions import Select, Table, Join, Column, Identifier, Subquery
    _HAS_SQLGLOT = True
except ImportError:
    _HAS_SQLGLOT = False
    
import re

logger = logging.getLogger(__name__)

class JoinType(str, Enum):
    """Supported SQL JOIN types."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN" 
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"
    CROSS = "CROSS JOIN"

@dataclass
class TableReference:
    """Represents a table reference with optional alias."""
    name: str
    alias: Optional[str] = None
    schema: Optional[str] = None
    
    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name
    
    @property
    def display_name(self) -> str:
        """Get display name (alias if available, otherwise name)."""
        return self.alias or self.name

@dataclass 
class JoinRelationship:
    """Represents a JOIN relationship between tables."""
    join_type: JoinType
    left_table: TableReference
    right_table: TableReference
    condition: str
    raw_condition: str  # Original condition text
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "join_type": self.join_type.value,
            "left_table": {
                "name": self.left_table.name,
                "alias": self.left_table.alias,
                "schema": self.left_table.schema,
                "full_name": self.left_table.full_name
            },
            "right_table": {
                "name": self.right_table.name,
                "alias": self.right_table.alias, 
                "schema": self.right_table.schema,
                "full_name": self.right_table.full_name
            },
            "condition": self.condition,
            "raw_condition": self.raw_condition
        }

@dataclass
class ColumnExpression:
    """Represents a column expression in SELECT clause."""
    expression: str
    alias: Optional[str] = None
    source_table: Optional[str] = None
    source_alias: Optional[str] = None
    column_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "expression": self.expression,
            "alias": self.alias,
            "source_table": self.source_table,
            "source_alias": self.source_alias,
            "column_name": self.column_name,
            "effective_name": self.alias or self.column_name or self.expression
        }

@dataclass
class InlineView:
    """Represents an inline view/subquery with base table references."""
    alias: str
    sql: str
    base_tables: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "alias": self.alias,
            "sql": self.sql,
            "base_tables": self.base_tables
        }

@dataclass
class SqlSemantics:
    """Complete SQL semantics metadata for migration support."""
    original_query: str
    tables: List[TableReference]
    joins: List[JoinRelationship]
    columns: List[ColumnExpression]
    inline_views: List[InlineView]
    where_clause: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "original_query": self.original_query,
            "tables": [
                {
                    "name": t.name,
                    "alias": t.alias,
                    "schema": t.schema,
                    "full_name": t.full_name
                } for t in self.tables
            ],
            "joins": [j.to_dict() for j in self.joins],
            "columns": [c.to_dict() for c in self.columns],
            "inline_views": [iv.to_dict() for iv in self.inline_views],
            "where_clause": self.where_clause,
            "migration_metadata": {
                "table_count": len(self.tables),
                "join_count": len(self.joins),
                "column_count": len(self.columns),
                "inline_view_count": len(self.inline_views),
                "has_aliases": any(c.alias for c in self.columns),
                "has_joins": len(self.joins) > 0,
                "has_inline_views": len(self.inline_views) > 0,
                "join_types": list(set(j.join_type.value for j in self.joins))
            }
        }

class EnhancedPlsqlParser:
    """
    Enhanced PL/SQL SQL parser using SQLGlot for accurate parsing.
    
    This parser extracts:
    1. Table names with aliases and schemas
    2. JOIN relationships with conditions and types
    3. Column expressions with aliases
    4. Inline views/subqueries
    5. Complete query structure
    """
    
    # Oracle SQL keywords that should not be treated as table aliases
    SQL_KEYWORDS = {
        'select', 'from', 'where', 'join', 'inner', 'left', 'right', 'full', 'outer',
        'union', 'order', 'group', 'by', 'having', 'distinct', 'as', 'on', 'and', 'or',
        'not', 'in', 'exists', 'case', 'when', 'then', 'else', 'end', 'null', 'is',
        'between', 'like', 'into', 'values', 'insert', 'update', 'delete', 'merge',
        'create', 'alter', 'drop', 'table', 'view', 'index', 'sequence', 'constraint',
        'primary', 'key', 'foreign', 'references', 'unique', 'check', 'default',
        'varchar2', 'number', 'date', 'timestamp', 'char', 'clob', 'blob', 'rowid',
        'dual', 'sysdate', 'systimestamp', 'rownum', 'nextval', 'currval'
    }
    
    # Oracle built-in functions that should not be treated as tables
    ORACLE_FUNCTIONS = {
        'to_date', 'to_char', 'to_number', 'extract', 'substr', 'length', 'instr',
        'upper', 'lower', 'initcap', 'trim', 'ltrim', 'rtrim', 'replace', 'translate',
        'decode', 'nvl', 'nvl2', 'coalesce', 'nullif', 'greatest', 'least',
        'abs', 'ceil', 'floor', 'round', 'trunc', 'mod', 'power', 'sqrt', 'sign',
        'sin', 'cos', 'tan', 'asin', 'acos', 'atan', 'atan2', 'exp', 'ln', 'log',
        'avg', 'count', 'max', 'min', 'sum', 'stddev', 'variance',
        'rank', 'dense_rank', 'row_number', 'lead', 'lag', 'first_value', 'last_value',
        'sysdate', 'systimestamp', 'current_date', 'current_timestamp', 'localtimestamp',
        'add_months', 'months_between', 'next_day', 'last_day', 'trunc_date'
    }
    
    def __init__(self):
        """Initialize the enhanced PL/SQL parser."""
        self.logger = logging.getLogger(__name__)
        self.validation_report = {
            'sql_statements_parsed': 0,
            'sqlglot_parse_failures': 0,
            'regex_fallback_used': 0,
            'joins_extracted': 0,
            'inline_views_found': 0,
            'tables_resolved': 0,
            'columns_extracted': 0
        }
    
    def get_validation_report(self) -> Dict[str, int]:
        """Get validation report for testing and monitoring."""
        return self.validation_report.copy()
    
    def parse_sql_semantics(self, sql_query: str) -> Optional[SqlSemantics]:
        """
        Parse complete SQL semantics from a PL/SQL query using SQLGlot.
        
        Args:
            sql_query: SQL query string to parse
            
        Returns:
            SqlSemantics object with complete metadata
        """
        if not sql_query or not isinstance(sql_query, str):
            return None
        
        self.validation_report['sql_statements_parsed'] += 1
        
        # Clean and normalize the SQL
        sql = self._normalize_sql(sql_query)
        
        # Try SQLGlot parsing first
        if _HAS_SQLGLOT:
            try:
                parsed = sqlglot.parse_one(sql, dialect="oracle")
                if parsed:
                    return self._extract_semantics_from_ast(parsed, sql)
            except Exception as e:
                self.logger.debug(f"SQLGlot parsing failed: {e}")
                self.validation_report['sqlglot_parse_failures'] += 1
        
        # Fallback to regex-based parsing
        self.validation_report['regex_fallback_used'] += 1
        return self._extract_semantics_with_regex(sql)
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for consistent parsing."""
        # Remove extra whitespace and normalize line breaks
        sql = ' '.join(sql.split())
        
        # Remove PL/SQL-specific constructs that might confuse parsing
        sql = re.sub(r'\s*INTO\s+[^F\s]+(?=\s+FROM)', ' ', sql, flags=re.IGNORECASE)
        
        # Ensure consistent spacing around keywords
        sql = re.sub(r'\s*(,)\s*', r'\1 ', sql)
        sql = re.sub(r'\s+(FROM|JOIN|WHERE|ON|AS|UNION|ORDER|GROUP)\s+', r' \1 ', sql, flags=re.IGNORECASE)
        
        return sql.strip()
    
    def _extract_semantics_from_ast(self, parsed_ast, original_sql: str) -> SqlSemantics:
        """Extract semantics using SQLGlot AST."""
        tables = self._extract_tables_from_ast(parsed_ast)
        joins = self._extract_joins_from_ast(parsed_ast, tables)
        columns = self._extract_columns_from_ast(parsed_ast, tables)
        inline_views = self._extract_inline_views_from_ast(parsed_ast)
        where_clause = self._extract_where_from_ast(parsed_ast)
        
        self.validation_report['joins_extracted'] += len(joins)
        self.validation_report['inline_views_found'] += len(inline_views)
        self.validation_report['tables_resolved'] += len(tables)
        self.validation_report['columns_extracted'] += len(columns)
        
        return SqlSemantics(
            original_query=original_sql,
            tables=tables,
            joins=joins,
            columns=columns,
            inline_views=inline_views,
            where_clause=where_clause
        )
    
    def _extract_tables_from_ast(self, ast) -> List[TableReference]:
        """Extract table references from SQLGlot AST."""
        tables = []
        
        # Find all table expressions
        for table_node in ast.find_all(Table):
            table_name = self._get_table_name_from_node(table_node)
            schema = self._get_schema_from_node(table_node)
            alias = self._get_alias_from_node(table_node)
            
            if table_name:
                # Filter out Oracle built-in functions that might be mistaken for tables
                if table_name.lower() in self.ORACLE_FUNCTIONS:
                    continue
                    
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                tables.append(TableReference(
                    name=table_name,
                    schema=schema,
                    alias=alias
                ))
        
        return tables
    
    def _extract_joins_from_ast(self, ast, tables: List[TableReference]) -> List[JoinRelationship]:
        """Extract JOIN relationships from SQLGlot AST."""
        joins = []
        
        for join_node in ast.find_all(Join):
            # Determine join type
            join_type = self._get_join_type_from_node(join_node)
            
            # Get right table (the table being joined)
            right_table = None
            if hasattr(join_node, 'this') and isinstance(join_node.this, Table):
                right_table_name = self._get_table_name_from_node(join_node.this)
                right_schema = self._get_schema_from_node(join_node.this)
                right_alias = self._get_alias_from_node(join_node.this)
                
                if right_table_name:
                    # Filter out Oracle built-in functions that might be mistaken for tables
                    if right_table_name.lower() in self.ORACLE_FUNCTIONS:
                        continue
                        
                    if right_alias and right_alias.lower() in self.SQL_KEYWORDS:
                        right_alias = None
                    right_table = TableReference(
                        name=right_table_name,
                        schema=right_schema,
                        alias=right_alias
                    )
            
            # Get left table (find the previous table in the FROM clause or join chain)
            left_table = None
            if tables:
                # For now, use the first table as left table
                # More sophisticated logic could track join chains
                left_table = tables[0]
                
                # Filter out Oracle functions from left table too
                if left_table.name.lower() in self.ORACLE_FUNCTIONS:
                    continue
            
            # Get join condition
            condition = ""
            if hasattr(join_node, 'on') and join_node.on:
                condition = join_node.on.sql(dialect="oracle")
            
            if left_table and right_table:
                joins.append(JoinRelationship(
                    join_type=join_type,
                    left_table=left_table,
                    right_table=right_table,
                    condition=condition,
                    raw_condition=condition
                ))
        
        return joins
    
    def _extract_columns_from_ast(self, ast, tables: List[TableReference]) -> List[ColumnExpression]:
        """Extract column expressions from SQLGlot AST."""
        columns = []
        
        # Find the main SELECT node
        select_node = ast if isinstance(ast, Select) else ast.find(Select)
        if not select_node or not hasattr(select_node, 'expressions'):
            return columns
        
        # Create table alias lookup
        alias_to_table = {}
        for table in tables:
            if table.alias:
                alias_to_table[table.alias] = table.name
        
        for expr in select_node.expressions:
            column_expr = self._extract_column_from_expression(expr, alias_to_table)
            if column_expr:
                columns.append(column_expr)
        
        return columns
    
    def _extract_column_from_expression(self, expr, alias_to_table: Dict[str, str]) -> Optional[ColumnExpression]:
        """Extract column information from a SELECT expression."""
        # Get the full expression as SQL
        expression_sql = expr.sql(dialect="oracle")
        
        # Get alias if present
        alias = None
        if hasattr(expr, 'alias') and expr.alias:
            alias = expr.alias
        
        # Extract source information
        source_table = None
        source_alias = None
        column_name = None
        
        # Handle simple column references
        if isinstance(expr, Column):
            column_name = expr.name
            if hasattr(expr, 'table') and expr.table:
                source_alias = expr.table
                source_table = alias_to_table.get(source_alias)
        
        return ColumnExpression(
            expression=expression_sql,
            alias=alias,
            source_table=source_table,
            source_alias=source_alias,
            column_name=column_name
        )
    
    def _extract_inline_views_from_ast(self, ast) -> List[InlineView]:
        """Extract inline views/subqueries from SQLGlot AST."""
        inline_views = []
        
        for subquery in ast.find_all(Subquery):
            alias = self._get_alias_from_node(subquery)
            if alias:
                sql = subquery.sql(dialect="oracle")
                
                # Extract base tables from the subquery
                base_tables = []
                for table_node in subquery.find_all(Table):
                    table_name = self._get_table_name_from_node(table_node)
                    if table_name:
                        base_tables.append(table_name)
                
                inline_views.append(InlineView(
                    alias=alias,
                    sql=sql,
                    base_tables=base_tables
                ))
        
        return inline_views
    
    def _extract_where_from_ast(self, ast) -> Optional[str]:
        """Extract WHERE clause from SQLGlot AST."""
        select_node = ast if isinstance(ast, Select) else ast.find(Select)
        if select_node and hasattr(select_node, 'where') and select_node.where:
            return select_node.where.sql(dialect="oracle")
        return None
    
    def _get_table_name_from_node(self, table_node) -> Optional[str]:
        """Extract table name from a table node."""
        if hasattr(table_node, 'this') and table_node.this:
            if isinstance(table_node.this, Identifier):
                return table_node.this.this
            elif hasattr(table_node.this, 'name'):
                return table_node.this.name
        return None
    
    def _get_schema_from_node(self, table_node) -> Optional[str]:
        """Extract schema name from a table node."""
        if hasattr(table_node, 'db') and table_node.db:
            if isinstance(table_node.db, Identifier):
                return table_node.db.this
            elif hasattr(table_node.db, 'name'):
                return table_node.db.name
        return None
    
    def _get_alias_from_node(self, node) -> Optional[str]:
        """Extract alias from a node."""
        if hasattr(node, 'alias') and node.alias:
            if isinstance(node.alias, Identifier):
                return node.alias.this
            elif hasattr(node.alias, 'name'):
                return node.alias.name
            elif isinstance(node.alias, str):
                return node.alias
        return None
    
    def _get_join_type_from_node(self, join_node) -> JoinType:
        """Extract join type from a join node."""
        if hasattr(join_node, 'kind') and join_node.kind:
            kind = join_node.kind.upper()
            if 'LEFT' in kind:
                return JoinType.LEFT
            elif 'RIGHT' in kind:
                return JoinType.RIGHT
            elif 'FULL' in kind:
                return JoinType.FULL
            elif 'CROSS' in kind:
                return JoinType.CROSS
        return JoinType.INNER
    
    def _extract_semantics_with_regex(self, sql: str) -> SqlSemantics:
        """Fallback regex-based semantic extraction."""
        tables = self._extract_table_references_regex(sql)
        joins = self._extract_join_relationships_regex(sql, tables)
        columns = self._extract_column_expressions_regex(sql, tables)
        inline_views = []  # Not easily extractable with regex
        where_clause = self._extract_where_clause_regex(sql)
        
        return SqlSemantics(
            original_query=sql,
            tables=tables,
            joins=joins,
            columns=columns,
            inline_views=inline_views,
            where_clause=where_clause
        )
    
    def _extract_table_references_regex(self, sql: str) -> List[TableReference]:
        """Extract table references using regex patterns."""
        tables = []
        
        # FROM clause
        from_pattern = r'FROM\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        from_match = re.search(from_pattern, sql, re.IGNORECASE)
        if from_match:
            schema = from_match.group(1)
            table_name = from_match.group(2)
            alias = from_match.group(3)
            
            # Filter out Oracle built-in functions that might be mistaken for tables
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                tables.append(TableReference(name=table_name, schema=schema, alias=alias))
        
        # JOIN clauses
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
        for join_match in re.finditer(join_pattern, sql, re.IGNORECASE):
            schema = join_match.group(1)
            table_name = join_match.group(2)
            alias = join_match.group(3)
            
            # Filter out Oracle built-in functions that might be mistaken for tables
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                tables.append(TableReference(name=table_name, schema=schema, alias=alias))
        
        return tables
    
    def _extract_join_relationships_regex(self, sql: str, tables: List[TableReference]) -> List[JoinRelationship]:
        """Extract JOIN relationships using regex patterns."""
        joins = []
        
        join_pattern = r'((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN)\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+([^$]+?)(?=\s*(?:INNER|LEFT|RIGHT|FULL|CROSS|WHERE|ORDER|GROUP|HAVING|$))'
        
        for join_match in re.finditer(join_pattern, sql, re.IGNORECASE | re.DOTALL):
            join_type_raw = join_match.group(1).strip().upper()
            if join_type_raw == 'JOIN':
                join_type_raw = 'INNER JOIN'
            
            try:
                join_type = JoinType(join_type_raw)
            except ValueError:
                join_type = JoinType.INNER
            
            schema = join_match.group(2)
            table_name = join_match.group(3)
            alias = join_match.group(4)
            condition = join_match.group(5).strip()
            
            # Filter out Oracle built-in functions that might be mistaken for tables
            if table_name and table_name.lower() not in self.ORACLE_FUNCTIONS:
                # Validate alias is not a SQL keyword
                if alias and alias.lower() in self.SQL_KEYWORDS:
                    alias = None
                
                right_table = TableReference(name=table_name, schema=schema, alias=alias)
                left_table = tables[0] if tables else TableReference(name="Unknown")
                
                # Filter out Oracle functions from left table too
                if left_table.name.lower() not in self.ORACLE_FUNCTIONS:
                    joins.append(JoinRelationship(
                        join_type=join_type,
                        left_table=left_table,
                        right_table=right_table,
                        condition=condition,
                        raw_condition=condition
                    ))
        
        return joins
    
    def _extract_column_expressions_regex(self, sql: str, tables: List[TableReference]) -> List[ColumnExpression]:
        """Extract column expressions using regex patterns."""
        columns = []
        
        # Find SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return columns
        
        select_clause = select_match.group(1).strip()
        
        # Skip if SELECT *
        if select_clause.strip() == '*':
            return columns
        
        # Split by commas (simple approach)
        column_expressions = [expr.strip() for expr in select_clause.split(',')]
        
        # Create table alias lookup
        alias_to_table = {}
        for table in tables:
            if table.alias:
                alias_to_table[table.alias] = table.name
        
        for expr in column_expressions:
            if not expr:
                continue
            
            # Check for alias (AS keyword)
            as_match = re.search(r'^(.+?)\s+AS\s+(\w+)$', expr, re.IGNORECASE)
            if as_match:
                source_expr = as_match.group(1).strip()
                alias = as_match.group(2)
            else:
                source_expr = expr
                alias = None
            
            # Extract source information
            source_table = None
            source_alias = None
            column_name = None
            
            # Check for table.column format
            table_col_match = re.match(r'^(\w+)\.(\w+)$', source_expr)
            if table_col_match:
                source_alias = table_col_match.group(1)
                column_name = table_col_match.group(2)
                source_table = alias_to_table.get(source_alias)
            else:
                column_name = source_expr
            
            columns.append(ColumnExpression(
                expression=expr,
                alias=alias,
                source_table=source_table,
                source_alias=source_alias,
                column_name=column_name
            ))
        
        return columns
    
    def _extract_where_clause_regex(self, sql: str) -> Optional[str]:
        """Extract WHERE clause using regex."""
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+(?:ORDER|GROUP|HAVING|$))', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            return where_match.group(1).strip()
        return None

def create_join_edges_from_semantics(semantics: SqlSemantics) -> List[Dict[str, Any]]:
    """
    Create graph edges from SQL semantics for integration into MetaZCode graph.
    
    Args:
        semantics: Parsed SQL semantics
        
    Returns:
        List of edge dictionaries ready for graph integration
    """
    edges = []
    
    for join in semantics.joins:
        # Create JOIN edge between tables
        edge = {
            "source_id": f"table::{join.left_table.name}",
            "target_id": f"table::{join.right_table.name}",
            "edge_type": "REFERENCES",
            "properties": {
                "join_type": join.join_type.value,
                "condition": join.condition,
                "left_alias": join.left_table.alias,
                "right_alias": join.right_table.alias,
                "raw_condition": join.raw_condition,
                "relationship_type": "join_relationship"
            }
        }
        edges.append(edge)
    
    return edges
