"""
SSIS Data Type Mapping Engine

This module provides comprehensive data type mapping capabilities for SSIS metadata extraction.
It handles conversion between SSIS native types and target platform types, providing
canonical type definitions and conversion rules.
"""

from typing import Dict, List, Optional, Any, Set
from enum import Enum
import logging

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
    NTEXT = "NTEXT"
    
    # Date/Time types
    DATETIME = "DATETIME"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    
    # Binary types
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    IMAGE = "IMAGE"
    
    # Boolean type
    BOOLEAN = "BOOLEAN"
    
    # Special types
    GUID = "GUID"
    JSON = "JSON"
    XML = "XML"
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


class SSISDataTypeMapper:
    """
    Maps SSIS data types to canonical types and target platform types.
    """
    
    def __init__(self):
        self._ssis_to_canonical = self._build_ssis_canonical_mapping()
        self._canonical_to_platforms = self._build_platform_mappings()
        self._conversion_rules = self._build_conversion_rules()
    
    def _build_ssis_canonical_mapping(self) -> Dict[str, CanonicalDataType]:
        """Build mapping from SSIS data types to canonical types."""
        return {
            # Numeric types - both DT_ prefixed and short forms
            "DT_I1": CanonicalDataType.TINYINT,
            "i1": CanonicalDataType.TINYINT,
            "DT_I2": CanonicalDataType.SMALLINT,
            "i2": CanonicalDataType.SMALLINT,
            "DT_I4": CanonicalDataType.INTEGER,
            "i4": CanonicalDataType.INTEGER,
            "DT_I8": CanonicalDataType.BIGINT,
            "i8": CanonicalDataType.BIGINT,
            "DT_UI1": CanonicalDataType.TINYINT,
            "ui1": CanonicalDataType.TINYINT,
            "DT_UI2": CanonicalDataType.SMALLINT,
            "ui2": CanonicalDataType.SMALLINT,
            "DT_UI4": CanonicalDataType.INTEGER,
            "ui4": CanonicalDataType.INTEGER,
            "DT_UI8": CanonicalDataType.BIGINT,
            "ui8": CanonicalDataType.BIGINT,
            "DT_R4": CanonicalDataType.REAL,
            "r4": CanonicalDataType.REAL,
            "DT_R8": CanonicalDataType.FLOAT,
            "r8": CanonicalDataType.FLOAT,
            "DT_DECIMAL": CanonicalDataType.DECIMAL,
            "decimal": CanonicalDataType.DECIMAL,
            "DT_NUMERIC": CanonicalDataType.NUMERIC,
            "numeric": CanonicalDataType.NUMERIC,
            "DT_CY": CanonicalDataType.MONEY,
            "cy": CanonicalDataType.MONEY,
            
            # String types - both DT_ prefixed and short forms
            "DT_STR": CanonicalDataType.VARCHAR,
            "str": CanonicalDataType.VARCHAR,
            "DT_WSTR": CanonicalDataType.NVARCHAR,
            "wstr": CanonicalDataType.NVARCHAR,
            "DT_TEXT": CanonicalDataType.TEXT,
            "text": CanonicalDataType.TEXT,
            "DT_NTEXT": CanonicalDataType.NTEXT,
            "ntext": CanonicalDataType.NTEXT,
            
            # Date/Time types - both DT_ prefixed and short forms
            "DT_DBTIMESTAMP": CanonicalDataType.DATETIME,
            "dbtimestamp": CanonicalDataType.DATETIME,
            "DT_DBTIMESTAMP2": CanonicalDataType.DATETIME,
            "dbtimestamp2": CanonicalDataType.DATETIME,
            "DT_DBDATE": CanonicalDataType.DATE,
            "dbdate": CanonicalDataType.DATE,
            "DT_DBTIME": CanonicalDataType.TIME,
            "dbtime": CanonicalDataType.TIME,
            "DT_DBTIME2": CanonicalDataType.TIME,
            "dbtime2": CanonicalDataType.TIME,
            "DT_DBTIMESTAMPOFFSET": CanonicalDataType.TIMESTAMP,
            "dbtimestampoffset": CanonicalDataType.TIMESTAMP,
            
            # Binary types - both DT_ prefixed and short forms
            "DT_BYTES": CanonicalDataType.VARBINARY,
            "bytes": CanonicalDataType.VARBINARY,
            "DT_IMAGE": CanonicalDataType.IMAGE,
            "image": CanonicalDataType.IMAGE,
            
            # Boolean type - both DT_ prefixed and short forms
            "DT_BOOL": CanonicalDataType.BOOLEAN,
            "bool": CanonicalDataType.BOOLEAN,
            
            # Special types - both DT_ prefixed and short forms
            "DT_GUID": CanonicalDataType.GUID,
            "guid": CanonicalDataType.GUID,
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
            CanonicalDataType.BIGINT: {
                TargetPlatform.SQL_SERVER: "bigint",
                TargetPlatform.POSTGRESQL: "bigint",
                TargetPlatform.MYSQL: "bigint", 
                TargetPlatform.ORACLE: "number(19)",
                TargetPlatform.SNOWFLAKE: "number(38,0)",
                TargetPlatform.BIGQUERY: "int64",
                TargetPlatform.AZURE_SYNAPSE: "bigint"
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
                TargetPlatform.ORACLE: "timestamp",
                TargetPlatform.SNOWFLAKE: "timestamp",
                TargetPlatform.BIGQUERY: "datetime",
                TargetPlatform.AZURE_SYNAPSE: "datetime2"
            },
            CanonicalDataType.DATE: {
                TargetPlatform.SQL_SERVER: "date",
                TargetPlatform.POSTGRESQL: "date",
                TargetPlatform.MYSQL: "date",
                TargetPlatform.ORACLE: "date",
                TargetPlatform.SNOWFLAKE: "date",
                TargetPlatform.BIGQUERY: "date",
                TargetPlatform.AZURE_SYNAPSE: "date"
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
            CanonicalDataType.BOOLEAN: {
                TargetPlatform.SQL_SERVER: "bit",
                TargetPlatform.POSTGRESQL: "boolean",
                TargetPlatform.MYSQL: "boolean",
                TargetPlatform.ORACLE: "number(1)",
                TargetPlatform.SNOWFLAKE: "boolean",
                TargetPlatform.BIGQUERY: "bool",
                TargetPlatform.AZURE_SYNAPSE: "bit"
            },
            CanonicalDataType.GUID: {
                TargetPlatform.SQL_SERVER: "uniqueidentifier",
                TargetPlatform.POSTGRESQL: "uuid",
                TargetPlatform.MYSQL: "char(36)",
                TargetPlatform.ORACLE: "char(36)",
                TargetPlatform.SNOWFLAKE: "varchar(36)",
                TargetPlatform.BIGQUERY: "string",
                TargetPlatform.AZURE_SYNAPSE: "uniqueidentifier"
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
            (CanonicalDataType.TIME, CanonicalDataType.DATETIME)
        ]
        
        for source, target in safe_conversions:
            rules[(source, target)] = ConversionRisk.LOW
            
        # Medium risk conversions (potential precision loss)
        medium_conversions = [
            (CanonicalDataType.BIGINT, CanonicalDataType.INTEGER),
            (CanonicalDataType.FLOAT, CanonicalDataType.REAL),
            (CanonicalDataType.DECIMAL, CanonicalDataType.INTEGER),
            (CanonicalDataType.DATETIME, CanonicalDataType.DATE),
            (CanonicalDataType.NVARCHAR, CanonicalDataType.VARCHAR)
        ]
        
        for source, target in medium_conversions:
            rules[(source, target)] = ConversionRisk.MEDIUM
            
        # High risk conversions (potential data loss)
        high_conversions = [
            (CanonicalDataType.VARCHAR, CanonicalDataType.INTEGER),
            (CanonicalDataType.NVARCHAR, CanonicalDataType.INTEGER),
            (CanonicalDataType.DATETIME, CanonicalDataType.TIME)
        ]
        
        for source, target in high_conversions:
            rules[(source, target)] = ConversionRisk.HIGH
            
        return rules
    
    def get_canonical_type(self, ssis_type: str) -> CanonicalDataType:
        """Get canonical type for SSIS data type."""
        return self._ssis_to_canonical.get(ssis_type, CanonicalDataType.UNKNOWN)
    
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
    
    def get_conversion_risk(self, source_type: CanonicalDataType, 
                          target_type: CanonicalDataType) -> ConversionRisk:
        """Get conversion risk between two canonical types."""
        if source_type == target_type:
            return ConversionRisk.NONE
        return self._conversion_rules.get((source_type, target_type), ConversionRisk.UNSAFE)
    
    def enrich_column_properties(self, ssis_type: str, 
                               length: Optional[str] = None,
                               precision: Optional[str] = None,
                               scale: Optional[str] = None,
                               nullable: Optional[bool] = None,
                               target_platforms: Optional[List[TargetPlatform]] = None) -> Dict[str, Any]:
        """
        Enrich column properties with comprehensive type mapping information.
        
        Args:
            ssis_type: SSIS native data type (e.g., "DT_I4")
            length: Column length if applicable
            precision: Numeric precision if applicable
            scale: Numeric scale if applicable
            nullable: Whether column allows nulls
            target_platforms: List of target platforms to map to
            
        Returns:
            Dictionary with enriched type mapping properties
        """
        canonical_type = self.get_canonical_type(ssis_type)
        
        # Convert string parameters to integers
        length_int = int(length) if length and length.isdigit() else None
        precision_int = int(precision) if precision and precision.isdigit() else None
        scale_int = int(scale) if scale and scale.isdigit() else None
        
        # Default target platforms if not specified
        if target_platforms is None:
            target_platforms = [
                TargetPlatform.SQL_SERVER,
                TargetPlatform.POSTGRESQL,
                TargetPlatform.MYSQL,
                TargetPlatform.ORACLE
            ]
        
        # Build target type mappings
        target_types = {}
        conversion_confidence = 1.0
        potential_issues = []
        
        for platform in target_platforms:
            platform_type = self.get_platform_type(
                canonical_type, platform, length_int, precision_int, scale_int
            )
            target_types[platform.value] = platform_type
            
            # Check for potential conversion issues
            if platform_type == "unknown":
                potential_issues.append(f"No mapping defined for {platform.value}")
                conversion_confidence = min(conversion_confidence, 0.5)
        
        # Additional validation
        if canonical_type == CanonicalDataType.UNKNOWN:
            potential_issues.append(f"Unknown SSIS type: {ssis_type}")
            conversion_confidence = 0.3
        
        if length_int and length_int > 8000:
            potential_issues.append("Large column length may require special handling")
            conversion_confidence = min(conversion_confidence, 0.8)
        
        return {
            "ssis_native_type": ssis_type,
            "canonical_type": canonical_type.value,
            "target_types": target_types,
            "type_precision": precision_int,
            "type_scale": scale_int,
            "type_length": length_int,
            "nullable": nullable,
            "conversion_confidence": conversion_confidence,
            "potential_issues": potential_issues,
            "type_category": self._get_type_category(canonical_type),
            "supports_indexing": self._supports_indexing(canonical_type),
            "supports_sorting": self._supports_sorting(canonical_type)
        }
    
    def _get_type_category(self, canonical_type: CanonicalDataType) -> str:
        """Categorize canonical type."""
        numeric_types = {CanonicalDataType.INTEGER, CanonicalDataType.BIGINT, 
                        CanonicalDataType.SMALLINT, CanonicalDataType.TINYINT,
                        CanonicalDataType.DECIMAL, CanonicalDataType.NUMERIC,
                        CanonicalDataType.FLOAT, CanonicalDataType.REAL, CanonicalDataType.MONEY}
        
        string_types = {CanonicalDataType.STRING, CanonicalDataType.VARCHAR,
                       CanonicalDataType.NVARCHAR, CanonicalDataType.CHAR,
                       CanonicalDataType.NCHAR, CanonicalDataType.TEXT, CanonicalDataType.NTEXT}
        
        datetime_types = {CanonicalDataType.DATETIME, CanonicalDataType.DATE,
                         CanonicalDataType.TIME, CanonicalDataType.TIMESTAMP}
        
        binary_types = {CanonicalDataType.BINARY, CanonicalDataType.VARBINARY, CanonicalDataType.IMAGE}
        
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
            return "special"
    
    def _supports_indexing(self, canonical_type: CanonicalDataType) -> bool:
        """Check if type supports database indexing."""
        non_indexable = {CanonicalDataType.TEXT, CanonicalDataType.NTEXT, 
                        CanonicalDataType.IMAGE, CanonicalDataType.JSON, CanonicalDataType.XML}
        return canonical_type not in non_indexable
    
    def _supports_sorting(self, canonical_type: CanonicalDataType) -> bool:
        """Check if type supports sorting operations."""
        non_sortable = {CanonicalDataType.IMAGE, CanonicalDataType.JSON, CanonicalDataType.XML}
        return canonical_type not in non_sortable
    
    def analyze_type_conversions(self, transformations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze type conversions in a list of transformations.
        
        Args:
            transformations: List of transformation definitions with source/target types
            
        Returns:
            Analysis of type conversion patterns and risks
        """
        analysis = {
            "total_conversions": len(transformations),
            "conversion_risks": {"none": 0, "low": 0, "medium": 0, "high": 0, "unsafe": 0},
            "risk_summary": [],
            "common_patterns": {},
            "recommendations": []
        }
        
        for transform in transformations:
            source_type = transform.get("source_canonical_type")
            target_type = transform.get("target_canonical_type")
            
            if source_type and target_type:
                try:
                    source_enum = CanonicalDataType(source_type)
                    target_enum = CanonicalDataType(target_type)
                    risk = self.get_conversion_risk(source_enum, target_enum)
                    
                    analysis["conversion_risks"][risk.value] += 1
                    
                    if risk in [ConversionRisk.HIGH, ConversionRisk.UNSAFE]:
                        analysis["risk_summary"].append({
                            "source": source_type,
                            "target": target_type,
                            "risk": risk.value,
                            "transformation": transform.get("column_name", "unknown")
                        })
                        
                    # Track conversion patterns
                    pattern_key = f"{source_type}->{target_type}"
                    analysis["common_patterns"][pattern_key] = analysis["common_patterns"].get(pattern_key, 0) + 1
                    
                except ValueError:
                    analysis["conversion_risks"]["unsafe"] += 1
        
        # Generate recommendations
        if analysis["conversion_risks"]["high"] > 0:
            analysis["recommendations"].append("Review high-risk type conversions for potential data loss")
        if analysis["conversion_risks"]["unsafe"] > 0:
            analysis["recommendations"].append("Validate unsafe type conversions before deployment")
        if analysis["total_conversions"] > 50:
            analysis["recommendations"].append("Consider implementing automated type validation testing")
            
        return analysis