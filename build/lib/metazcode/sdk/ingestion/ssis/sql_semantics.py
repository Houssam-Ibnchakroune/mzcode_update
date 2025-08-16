#!/usr/bin/env python3
"""
Enhanced SQL Semantics Parser for SSIS Migration Support

This module provides enhanced SQL parsing capabilities to capture complete
SQL semantics including JOIN relationships, column aliases, and query structure
for migration purposes.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

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
class SqlSemantics:
    """Complete SQL semantics metadata for migration support."""
    original_query: str
    tables: List[TableReference]
    joins: List[JoinRelationship]
    columns: List[ColumnExpression]
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
            "where_clause": self.where_clause,
            "migration_metadata": {
                "table_count": len(self.tables),
                "join_count": len(self.joins),
                "column_count": len(self.columns),
                "has_aliases": any(c.alias for c in self.columns),
                "has_joins": len(self.joins) > 0,
                "join_types": list(set(j.join_type.value for j in self.joins))
            }
        }

class EnhancedSqlParser:
    """
    Enhanced SQL parser that captures complete semantics for migration.
    
    This parser extracts:
    1. Table names with aliases and schemas
    2. JOIN relationships with conditions and types
    3. Column expressions with aliases
    4. Complete query structure
    """
    
    def __init__(self):
        """Initialize the enhanced SQL parser."""
        self.logger = logging.getLogger(__name__)
    
    def parse_sql_semantics(self, sql_query: str) -> SqlSemantics:
        """
        Parse complete SQL semantics from a query.
        
        Args:
            sql_query: SQL query string to parse
            
        Returns:
            SqlSemantics object with complete metadata
        """
        if not sql_query or not isinstance(sql_query, str):
            return SqlSemantics("", [], [], [])
        
        # Clean and normalize the SQL
        sql = self._normalize_sql(sql_query)
        
        try:
            tables = self._extract_table_references(sql)
            joins = self._extract_join_relationships(sql, tables)
            columns = self._extract_column_expressions(sql, tables)
            where_clause = self._extract_where_clause(sql)
            
            semantics = SqlSemantics(
                original_query=sql,
                tables=tables,
                joins=joins,
                columns=columns,
                where_clause=where_clause
            )
            
            self.logger.debug(f"Parsed SQL semantics: {len(tables)} tables, {len(joins)} joins, {len(columns)} columns")
            return semantics
            
        except Exception as e:
            self.logger.error(f"Error parsing SQL semantics: {e}")
            return SqlSemantics(sql, [], [], [])
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for consistent parsing."""
        # Remove extra whitespace and normalize line breaks
        sql = ' '.join(sql.split())
        
        # Ensure consistent spacing around keywords
        sql = re.sub(r'\s*(,)\s*', r'\1 ', sql)
        sql = re.sub(r'\s+(FROM|JOIN|WHERE|ON|AS)\s+', r' \1 ', sql, flags=re.IGNORECASE)
        
        return sql.strip()
    
    def _extract_table_references(self, sql: str) -> List[TableReference]:
        """Extract all table references with aliases and schemas."""
        tables = []
        
        # FROM clause
        from_pattern = r'FROM\s+(?:\[?([^\s\[\]\.]+)\]?\.)?(?:\[?([^\s\[\]\.]+)\]?)(?:\s+(?:AS\s+)?([^\s]+))?'
        from_match = re.search(from_pattern, sql, re.IGNORECASE)
        if from_match:
            schema = from_match.group(1)
            table_name = from_match.group(2) or from_match.group(1)  # Handle single name case
            alias = from_match.group(3)
            
            if from_match.group(2):  # Schema.Table format
                schema = from_match.group(1)
                table_name = from_match.group(2)
            else:  # Single table name
                schema = None
                table_name = from_match.group(1)
            
            tables.append(TableReference(name=table_name, alias=alias, schema=schema))
        
        # JOIN clauses
        join_pattern = r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN\s+(?:\[?([^\s\[\]\.]+)\]?\.)?(?:\[?([^\s\[\]\.]+)\]?)(?:\s+(?:AS\s+)?([^\s]+))?'
        join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
        
        for schema, table_name, alias in join_matches:
            if not table_name and schema:  # Single name case
                table_name = schema
                schema = None
            
            if table_name:
                tables.append(TableReference(name=table_name, alias=alias, schema=schema))
        
        return tables
    
    def _extract_join_relationships(self, sql: str, tables: List[TableReference]) -> List[JoinRelationship]:
        """Extract JOIN relationships with conditions."""
        joins = []
        
        # Pattern to match JOIN ... ON conditions
        join_pattern = r'((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN)\s+(?:\[?([^\s\[\]\.]+)\]?\.)?(?:\[?([^\s\[\]\.]+)\]?)(?:\s+(?:AS\s+)?([^\s]+))?\s+ON\s+([^$]+?)(?=\s*(?:INNER|LEFT|RIGHT|FULL|CROSS|WHERE|ORDER|GROUP|HAVING|$))'
        
        join_matches = re.findall(join_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        for join_type_raw, schema, table_name, alias, condition in join_matches:
            # Normalize join type
            join_type_clean = join_type_raw.strip().upper()
            if join_type_clean == 'JOIN':
                join_type_clean = 'INNER JOIN'
            
            try:
                join_type = JoinType(join_type_clean)
            except ValueError:
                join_type = JoinType.INNER  # Default fallback
            
            # Handle table name
            if not table_name and schema:
                table_name = schema
                schema = None
            
            # Find matching table references
            right_table = None
            for table in tables:
                if table.name == table_name:
                    right_table = table
                    break
            
            if not right_table:
                right_table = TableReference(name=table_name, alias=alias, schema=schema)
            
            # Get left table (FROM table)
            left_table = tables[0] if tables else TableReference(name="Unknown")
            
            joins.append(JoinRelationship(
                join_type=join_type,
                left_table=left_table,
                right_table=right_table,
                condition=condition.strip(),
                raw_condition=condition.strip()
            ))
        
        return joins
    
    def _extract_column_expressions(self, sql: str, tables: List[TableReference]) -> List[ColumnExpression]:
        """Extract column expressions from SELECT clause."""
        columns = []
        
        # Find SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return columns
        
        select_clause = select_match.group(1).strip()
        
        # Split by commas (basic approach - could be enhanced for complex expressions)
        column_expressions = self._split_select_columns(select_clause)
        
        # Create table alias lookup
        alias_to_table = {}
        for table in tables:
            if table.alias:
                alias_to_table[table.alias] = table.name
        
        for expr in column_expressions:
            expr = expr.strip()
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
                # Simple column name
                column_name = source_expr
            
            columns.append(ColumnExpression(
                expression=expr,
                alias=alias,
                source_table=source_table,
                source_alias=source_alias,
                column_name=column_name
            ))
        
        return columns
    
    def _split_select_columns(self, select_clause: str) -> List[str]:
        """Split SELECT clause by commas, handling nested parentheses."""
        columns = []
        current_column = ""
        paren_depth = 0
        
        for char in select_clause:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                columns.append(current_column.strip())
                current_column = ""
                continue
            
            current_column += char
        
        # Add the last column
        if current_column.strip():
            columns.append(current_column.strip())
        
        return columns
    
    def _extract_where_clause(self, sql: str) -> Optional[str]:
        """Extract WHERE clause if present."""
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
        # Create JOIN edge between tables only (columns may not exist as nodes)
        edge = {
            "source_id": f"table:{join.left_table.name}",
            "target_id": f"table:{join.right_table.name}",
            "edge_type": "REFERENCES",  # Use existing edge type
            "properties": {
                "join_type": join.join_type.value,
                "condition": join.condition,
                "left_alias": join.left_table.alias,
                "right_alias": join.right_table.alias,
                "raw_condition": join.raw_condition,
                "relationship_type": "join_relationship"  # Identify this as JOIN semantics
            }
        }
        edges.append(edge)
    
    # Skip column alias edges for now - columns may not exist as individual nodes
    # The column information is preserved in the sql_semantics metadata
    
    return edges