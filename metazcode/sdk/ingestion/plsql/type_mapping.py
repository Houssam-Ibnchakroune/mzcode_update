"""
PL/SQL Data Type Mapping Engine

This module provides comprehensive data type mapping capabilities for PL/SQL metadata extraction.
It handles conversion between Oracle PL/SQL native types and target platform types, providing
canonical type definitions and conversion rules.
"""

from typing import Dict, List, Optional, Any, Set
from enum import Enum
import logging
import re

logger = logging.getLogger(__name__)


class CanonicalDataType(Enum):
    """Canonical data type definitions for cross-platform compatibility."""
    
    # Numeric types
    INTEGER = "INTEGER"
    BIGINT = "BIGINT" 
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    FLOAT = "FLOAT"
    REAL = "REAL"
    MONEY = "MONEY"
    
    # String types
    STRING = "STRING"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"
    CHAR = "CHAR"
    NCHAR = "NCHAR"
    TEXT = "TEXT"
    CLOB = "CLOB"
    NCLOB = "NCLOB"
    
    # Date/Time types
    DATETIME = "DATETIME"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"
    
    # Binary types
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    BLOB = "BLOB"
    RAW = "RAW"
    
    # Boolean type
    BOOLEAN = "BOOLEAN"
    
    # Special Oracle types
    ROWID = "ROWID"
    UROWID = "UROWID"
    REF_CURSOR = "REF_CURSOR"
    XMLTYPE = "XMLTYPE"
    JSON = "JSON"
    UNKNOWN = "UNKNOWN"


class ConversionRisk(Enum):
    """Risk levels for data type conversions."""
    NONE = "none"          # No conversion needed
    LOW = "low"            # Safe conversion with no data loss
    MEDIUM = "medium"      # Conversion with potential precision loss
    HIGH = "high"          # Conversion with potential data loss
    UNSAFE = "unsafe"      # Conversion likely to cause errors


class TargetPlatform(Enum):
    """Supported target platforms for type mapping."""
    SQL_SERVER = "sql_server"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    AZURE_SYNAPSE = "azure_synapse"


class PLSQLDataTypeMapper:
    """
    Maps Oracle PL/SQL data types to canonical types and target platform types.
    """
    
    def __init__(self):
        self._plsql_to_canonical = self._build_plsql_canonical_mapping()
        self._canonical_to_platforms = self._build_platform_mappings()
        self._conversion_rules = self._build_conversion_rules()
    
    def _build_plsql_canonical_mapping(self) -> Dict[str, CanonicalDataType]:
        """Build mapping from Oracle PL/SQL data types to canonical types."""
        return {
            # Numeric types
            "NUMBER": CanonicalDataType.DECIMAL,
            "INTEGER": CanonicalDataType.INTEGER,
            "INT": CanonicalDataType.INTEGER,
            "SMALLINT": CanonicalDataType.SMALLINT,
            "DECIMAL": CanonicalDataType.DECIMAL,
            "DEC": CanonicalDataType.DECIMAL,
            "NUMERIC": CanonicalDataType.NUMERIC,
            "FLOAT": CanonicalDataType.FLOAT,
            "REAL": CanonicalDataType.REAL,
            "DOUBLE PRECISION": CanonicalDataType.FLOAT,
            "BINARY_FLOAT": CanonicalDataType.REAL,
            "BINARY_DOUBLE": CanonicalDataType.FLOAT,
            "PLS_INTEGER": CanonicalDataType.INTEGER,
            "BINARY_INTEGER": CanonicalDataType.INTEGER,
            
            # String types
            "VARCHAR2": CanonicalDataType.VARCHAR,
            "VARCHAR": CanonicalDataType.VARCHAR,
            "CHAR": CanonicalDataType.CHAR,
            "NCHAR": CanonicalDataType.NCHAR,
            "NVARCHAR2": CanonicalDataType.NVARCHAR,
            "CLOB": CanonicalDataType.CLOB,
            "NCLOB": CanonicalDataType.NCLOB,
            "LONG": CanonicalDataType.TEXT,
            
            # Date/Time types
            "DATE": CanonicalDataType.DATETIME,  # Oracle DATE includes time
            "TIMESTAMP": CanonicalDataType.TIMESTAMP,
            "TIMESTAMP WITH TIME ZONE": CanonicalDataType.TIMESTAMP,
            "TIMESTAMP WITH LOCAL TIME ZONE": CanonicalDataType.TIMESTAMP,
            "INTERVAL YEAR TO MONTH": CanonicalDataType.INTERVAL,
            "INTERVAL DAY TO SECOND": CanonicalDataType.INTERVAL,
            
            # Binary types
            "RAW": CanonicalDataType.RAW,
            "LONG RAW": CanonicalDataType.VARBINARY,
            "BLOB": CanonicalDataType.BLOB,
            "BFILE": CanonicalDataType.BLOB,
            
            # Boolean type (Oracle 23c+)
            "BOOLEAN": CanonicalDataType.BOOLEAN,
            
            # Special Oracle types
            "ROWID": CanonicalDataType.ROWID,
            "UROWID": CanonicalDataType.UROWID,
            "REF CURSOR": CanonicalDataType.REF_CURSOR,
            "SYS_REFCURSOR": CanonicalDataType.REF_CURSOR,
            "XMLTYPE": CanonicalDataType.XMLTYPE,
            "JSON": CanonicalDataType.JSON,
        }
    
    def _build_platform_mappings(self) -> Dict[CanonicalDataType, Dict[TargetPlatform, str]]:
        """Build mappings from canonical types to target platform types."""
        return {
            CanonicalDataType.INTEGER: {
                TargetPlatform.SQL_SERVER: "int",
                TargetPlatform.POSTGRESQL: "integer", 
                TargetPlatform.MYSQL: "int",
                TargetPlatform.ORACLE: "number(10)",
                TargetPlatform.SNOWFLAKE: "number(38,0)",
                TargetPlatform.BIGQUERY: "int64",
                TargetPlatform.AZURE_SYNAPSE: "int"
            },
            CanonicalDataType.DECIMAL: {
                TargetPlatform.SQL_SERVER: "decimal({precision},{scale})",
                TargetPlatform.POSTGRESQL: "decimal({precision},{scale})",
                TargetPlatform.MYSQL: "decimal({precision},{scale})",
                TargetPlatform.ORACLE: "number({precision},{scale})",
                TargetPlatform.SNOWFLAKE: "number({precision},{scale})",
                TargetPlatform.BIGQUERY: "numeric({precision},{scale})",
                TargetPlatform.AZURE_SYNAPSE: "decimal({precision},{scale})"
            },
            CanonicalDataType.VARCHAR: {
                TargetPlatform.SQL_SERVER: "varchar({length})",
                TargetPlatform.POSTGRESQL: "varchar({length})",
                TargetPlatform.MYSQL: "varchar({length})",
                TargetPlatform.ORACLE: "varchar2({length})",
                TargetPlatform.SNOWFLAKE: "varchar({length})",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "varchar({length})"
            },
            CanonicalDataType.NVARCHAR: {
                TargetPlatform.SQL_SERVER: "nvarchar({length})",
                TargetPlatform.POSTGRESQL: "varchar({length})",
                TargetPlatform.MYSQL: "varchar({length})",
                TargetPlatform.ORACLE: "nvarchar2({length})",
                TargetPlatform.SNOWFLAKE: "varchar({length})",
                TargetPlatform.BIGQUERY: "string", 
                TargetPlatform.AZURE_SYNAPSE: "nvarchar({length})"
            },
            CanonicalDataType.DATETIME: {
                TargetPlatform.SQL_SERVER: "datetime2",
                TargetPlatform.POSTGRESQL: "timestamp",
                TargetPlatform.MYSQL: "datetime",
                TargetPlatform.ORACLE: "date",
                TargetPlatform.SNOWFLAKE: "timestamp",
                TargetPlatform.BIGQUERY: "datetime",
                TargetPlatform.AZURE_SYNAPSE: "datetime2"
            },
            CanonicalDataType.TIMESTAMP: {
                TargetPlatform.SQL_SERVER: "datetime2",
                TargetPlatform.POSTGRESQL: "timestamp with time zone",
                TargetPlatform.MYSQL: "timestamp",
                TargetPlatform.ORACLE: "timestamp",
                TargetPlatform.SNOWFLAKE: "timestamp",
                TargetPlatform.BIGQUERY: "timestamp",
                TargetPlatform.AZURE_SYNAPSE: "datetime2"
            },
            CanonicalDataType.BOOLEAN: {
                TargetPlatform.SQL_SERVER: "bit",
                TargetPlatform.POSTGRESQL: "boolean",
                TargetPlatform.MYSQL: "boolean",
                TargetPlatform.ORACLE: "number(1)",
                TargetPlatform.SNOWFLAKE: "boolean",
                TargetPlatform.BIGQUERY: "bool",
                TargetPlatform.AZURE_SYNAPSE: "bit"
            },
            CanonicalDataType.CLOB: {
                TargetPlatform.SQL_SERVER: "nvarchar(max)",
                TargetPlatform.POSTGRESQL: "text",
                TargetPlatform.MYSQL: "longtext",
                TargetPlatform.ORACLE: "clob",
                TargetPlatform.SNOWFLAKE: "varchar",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "nvarchar(max)"
            },
            CanonicalDataType.BLOB: {
                TargetPlatform.SQL_SERVER: "varbinary(max)",
                TargetPlatform.POSTGRESQL: "bytea",
                TargetPlatform.MYSQL: "longblob",
                TargetPlatform.ORACLE: "blob",
                TargetPlatform.SNOWFLAKE: "binary",
                TargetPlatform.BIGQUERY: "bytes",
                TargetPlatform.AZURE_SYNAPSE: "varbinary(max)"
            },
            CanonicalDataType.ROWID: {
                TargetPlatform.SQL_SERVER: "uniqueidentifier",
                TargetPlatform.POSTGRESQL: "varchar(18)",
                TargetPlatform.MYSQL: "varchar(18)",
                TargetPlatform.ORACLE: "rowid",
                TargetPlatform.SNOWFLAKE: "varchar(18)",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "varchar(18)"
            }
        }
    
    def _build_conversion_rules(self) -> Dict[tuple, ConversionRisk]:
        """Build conversion risk rules between canonical types."""
        rules = {}
        
        # Safe conversions (no data loss)
        safe_conversions = [
            (CanonicalDataType.TINYINT, CanonicalDataType.SMALLINT),
            (CanonicalDataType.SMALLINT, CanonicalDataType.INTEGER),
            (CanonicalDataType.INTEGER, CanonicalDataType.BIGINT),
            (CanonicalDataType.REAL, CanonicalDataType.FLOAT),
            (CanonicalDataType.CHAR, CanonicalDataType.VARCHAR),
            (CanonicalDataType.NCHAR, CanonicalDataType.NVARCHAR),
            (CanonicalDataType.DATE, CanonicalDataType.DATETIME),
        ]
        
        for source, target in safe_conversions:
            rules[(source, target)] = ConversionRisk.LOW
            
        # Medium risk conversions (potential precision loss)
        medium_conversions = [
            (CanonicalDataType.FLOAT, CanonicalDataType.REAL),
            (CanonicalDataType.DECIMAL, CanonicalDataType.INTEGER),
            (CanonicalDataType.DATETIME, CanonicalDataType.DATE),
            (CanonicalDataType.TIMESTAMP, CanonicalDataType.DATETIME),
            (CanonicalDataType.NVARCHAR, CanonicalDataType.VARCHAR),
            (CanonicalDataType.CLOB, CanonicalDataType.VARCHAR)
        ]
        
        for source, target in medium_conversions:
            rules[(source, target)] = ConversionRisk.MEDIUM
            
        # High risk conversions (potential data loss)
        high_conversions = [
            (CanonicalDataType.VARCHAR, CanonicalDataType.INTEGER),
            (CanonicalDataType.NVARCHAR, CanonicalDataType.INTEGER),
            (CanonicalDataType.DATETIME, CanonicalDataType.DATE),
            (CanonicalDataType.ROWID, CanonicalDataType.VARCHAR),
            (CanonicalDataType.XMLTYPE, CanonicalDataType.VARCHAR)
        ]
        
        for source, target in high_conversions:
            rules[(source, target)] = ConversionRisk.HIGH
            
        return rules
    
    def parse_oracle_type(self, type_declaration: str) -> Dict[str, Any]:
        """
        Parse Oracle type declaration and extract type information.
        
        Examples:
            "VARCHAR2(100)" -> {"base_type": "VARCHAR2", "length": 100}
            "NUMBER(10,2)" -> {"base_type": "NUMBER", "precision": 10, "scale": 2}
            "TIMESTAMP(6)" -> {"base_type": "TIMESTAMP", "precision": 6}
        """
        if not type_declaration:
            return {"base_type": "UNKNOWN"}
        
        # Clean up the type declaration
        type_decl = type_declaration.strip().upper()
        
        # Pattern to match Oracle type declarations
        pattern = r'^(\w+(?:\s+\w+)*?)(?:\(([^)]+)\))?(?:\s+(.*))?$'
        match = re.match(pattern, type_decl)
        
        if not match:
            return {"base_type": type_decl}
        
        base_type = match.group(1)
        params = match.group(2)
        modifiers = match.group(3)
        
        result = {"base_type": base_type}
        
        # Parse parameters
        if params:
            param_parts = [p.strip() for p in params.split(',')]
            if len(param_parts) == 1:
                # Single parameter - could be length, precision, or scale
                if param_parts[0].isdigit():
                    param_value = int(param_parts[0])
                    if base_type in ['VARCHAR2', 'VARCHAR', 'CHAR', 'NCHAR', 'NVARCHAR2', 'RAW']:
                        result['length'] = param_value
                    elif base_type in ['NUMBER', 'DECIMAL', 'NUMERIC']:
                        result['precision'] = param_value
                    elif base_type in ['TIMESTAMP', 'INTERVAL']:
                        result['precision'] = param_value
            elif len(param_parts) == 2:
                # Two parameters - precision and scale for numeric types
                if param_parts[0].isdigit() and param_parts[1].isdigit():
                    result['precision'] = int(param_parts[0])
                    result['scale'] = int(param_parts[1])
        
        # Parse modifiers
        if modifiers:
            if 'NOT NULL' in modifiers:
                result['nullable'] = False
            if 'DEFAULT' in modifiers:
                result['has_default'] = True
        
        return result
    
    def get_canonical_type(self, oracle_type: str) -> CanonicalDataType:
        """Get canonical type for Oracle data type."""
        if not oracle_type:
            return CanonicalDataType.UNKNOWN
        
        # Parse the type to get base type
        type_info = self.parse_oracle_type(oracle_type)
        base_type = type_info.get('base_type', '').upper()
        
        return self._plsql_to_canonical.get(base_type, CanonicalDataType.UNKNOWN)
    
    def get_platform_type(self, canonical_type: CanonicalDataType, 
                         platform: TargetPlatform, 
                         length: Optional[int] = None,
                         precision: Optional[int] = None,
                         scale: Optional[int] = None) -> str:
        """Get platform-specific type for canonical type."""
        platform_mapping = self._canonical_to_platforms.get(canonical_type, {})
        type_template = platform_mapping.get(platform, "unknown")
        
        # Replace placeholders with actual values
        if "{length}" in type_template and length:
            type_template = type_template.replace("{length}", str(length))
        if "{precision}" in type_template and precision:
            type_template = type_template.replace("{precision}", str(precision))
        if "{scale}" in type_template and scale:
            type_template = type_template.replace("{scale}", str(scale))
            
        return type_template
    
    def enrich_column_properties(self, oracle_type: str,
                               nullable: Optional[bool] = None,
                               default_value: Optional[str] = None,
                               target_platforms: Optional[List[TargetPlatform]] = None) -> Dict[str, Any]:
        """
        Enrich column properties with comprehensive type mapping information.
        
        Args:
            oracle_type: Oracle data type (e.g., "VARCHAR2(100)", "NUMBER(10,2)")
            nullable: Whether column allows nulls
            default_value: Default value if any
            target_platforms: List of target platforms to map to
            
        Returns:
            Dictionary with enriched type mapping properties
        """
        # Parse Oracle type
        type_info = self.parse_oracle_type(oracle_type)
        canonical_type = self.get_canonical_type(oracle_type)
        
        # Default target platforms if not specified
        if target_platforms is None:
            target_platforms = [
                TargetPlatform.SQL_SERVER,
                TargetPlatform.POSTGRESQL,
                TargetPlatform.MYSQL
            ]
        
        # Build target type mappings
        target_types = {}
        conversion_confidence = 1.0
        potential_issues = []
        
        for platform in target_platforms:
            platform_type = self.get_platform_type(
                canonical_type, platform, 
                type_info.get('length'),
                type_info.get('precision'),
                type_info.get('scale')
            )
            target_types[platform.value] = platform_type
            
            # Check for potential conversion issues
            if platform_type == "unknown":
                potential_issues.append(f"No mapping defined for {platform.value}")
                conversion_confidence = min(conversion_confidence, 0.5)
        
        # Additional validation
        if canonical_type == CanonicalDataType.UNKNOWN:
            potential_issues.append(f"Unknown Oracle type: {oracle_type}")
            conversion_confidence = 0.3
        
        # Check for Oracle-specific types that might need special handling
        if canonical_type in [CanonicalDataType.ROWID, CanonicalDataType.UROWID, 
                             CanonicalDataType.REF_CURSOR, CanonicalDataType.XMLTYPE]:
            potential_issues.append(f"Oracle-specific type {oracle_type} may require custom handling")
            conversion_confidence = min(conversion_confidence, 0.7)
        
        if type_info.get('length', 0) > 4000:
            potential_issues.append("Large column length may require CLOB/TEXT type")
            conversion_confidence = min(conversion_confidence, 0.8)
        
        return {
            "oracle_native_type": oracle_type,
            "parsed_type_info": type_info,
            "canonical_type": canonical_type.value,
            "target_types": target_types,
            "type_precision": type_info.get('precision'),
            "type_scale": type_info.get('scale'),
            "type_length": type_info.get('length'),
            "nullable": nullable,
            "default_value": default_value,
            "conversion_confidence": conversion_confidence,
            "potential_issues": potential_issues,
            "type_category": self._get_type_category(canonical_type),
            "supports_indexing": self._supports_indexing(canonical_type),
            "supports_sorting": self._supports_sorting(canonical_type),
            "oracle_specific": self._is_oracle_specific(canonical_type)
        }
    
    def _get_type_category(self, canonical_type: CanonicalDataType) -> str:
        """Categorize canonical type."""
        numeric_types = {CanonicalDataType.INTEGER, CanonicalDataType.BIGINT, 
                        CanonicalDataType.SMALLINT, CanonicalDataType.TINYINT,
                        CanonicalDataType.DECIMAL, CanonicalDataType.NUMERIC,
                        CanonicalDataType.FLOAT, CanonicalDataType.REAL, CanonicalDataType.MONEY}
        
        string_types = {CanonicalDataType.STRING, CanonicalDataType.VARCHAR,
                       CanonicalDataType.NVARCHAR, CanonicalDataType.CHAR,
                       CanonicalDataType.NCHAR, CanonicalDataType.TEXT, 
                       CanonicalDataType.CLOB, CanonicalDataType.NCLOB}
        
        datetime_types = {CanonicalDataType.DATETIME, CanonicalDataType.DATE,
                         CanonicalDataType.TIME, CanonicalDataType.TIMESTAMP,
                         CanonicalDataType.INTERVAL}
        
        binary_types = {CanonicalDataType.BINARY, CanonicalDataType.VARBINARY, 
                       CanonicalDataType.BLOB, CanonicalDataType.RAW}
        
        if canonical_type in numeric_types:
            return "numeric"
        elif canonical_type in string_types:
            return "string"
        elif canonical_type in datetime_types:
            return "datetime"
        elif canonical_type in binary_types:
            return "binary"
        elif canonical_type == CanonicalDataType.BOOLEAN:
            return "boolean"
        else:
            return "oracle_special"
    
    def _supports_indexing(self, canonical_type: CanonicalDataType) -> bool:
        """Check if type supports database indexing."""
        non_indexable = {CanonicalDataType.CLOB, CanonicalDataType.NCLOB,
                        CanonicalDataType.BLOB, CanonicalDataType.XMLTYPE,
                        CanonicalDataType.JSON}
        return canonical_type not in non_indexable
    
    def _supports_sorting(self, canonical_type: CanonicalDataType) -> bool:
        """Check if type supports sorting operations."""
        non_sortable = {CanonicalDataType.BLOB, CanonicalDataType.XMLTYPE,
                       CanonicalDataType.JSON, CanonicalDataType.REF_CURSOR}
        return canonical_type not in non_sortable
    
    def _is_oracle_specific(self, canonical_type: CanonicalDataType) -> bool:
        """Check if type is Oracle-specific."""
        oracle_specific = {CanonicalDataType.ROWID, CanonicalDataType.UROWID,
                          CanonicalDataType.REF_CURSOR, CanonicalDataType.INTERVAL}
        return canonical_type in oracle_specific


def detect_column_types_from_sql(sql_statement: str) -> List[Dict[str, Any]]:
    """
    Detect column types from SQL CREATE TABLE or ALTER TABLE statements.
    
    Args:
        sql_statement: SQL statement containing column definitions
        
    Returns:
        List of column type information dictionaries
    """
    mapper = PLSQLDataTypeMapper()
    columns = []
    
    # Clean up the SQL statement
    sql_clean = re.sub(r'--.*$', '', sql_statement, flags=re.MULTILINE)  # Remove comments
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)     # Remove block comments
    
    # Find CREATE TABLE statements
    create_table_pattern = r'CREATE\s+TABLE\s+\w+\s*\((.*?)\)'
    create_matches = re.findall(create_table_pattern, sql_clean, re.IGNORECASE | re.DOTALL)
    
    for table_def in create_matches:
        # Split by commas but be smart about nested parentheses
        column_definitions = []
        current_def = ""
        paren_count = 0
        
        for char in table_def + ",":  # Add comma at end to flush last definition
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                if current_def.strip():
                    column_definitions.append(current_def.strip())
                current_def = ""
                continue
            current_def += char
        
        for col_def in column_definitions:
            col_def = col_def.strip()
            
            # Skip constraint definitions
            if any(keyword in col_def.upper() for keyword in ['CONSTRAINT', 'PRIMARY KEY', 'FOREIGN KEY', 'CHECK', 'UNIQUE']):
                continue
            
            # Extract column name and type using regex
            col_match = re.match(r'(\w+)\s+(.+)', col_def, re.IGNORECASE)
            if col_match:
                col_name = col_match.group(1).strip()
                col_type = col_match.group(2).strip()
                
                type_info = mapper.enrich_column_properties(col_type)
                type_info['column_name'] = col_name
                columns.append(type_info)
    
    return columns
