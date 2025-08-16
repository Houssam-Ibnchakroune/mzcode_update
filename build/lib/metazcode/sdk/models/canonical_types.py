from enum import Enum


class NodeType(str, Enum):
    """Canonical node types for technology-agnostic representation"""

    DIRECTORY = "directory"
    FILE = "file"
    PIPELINE = "pipeline"
    OPERATION = "operation"
    DATA_ASSET = "data_asset"
    CONNECTION = "connection"
    PARAMETER = "parameter"
    VARIABLE = "variable"
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    ENTITY = "entity"  # Fallback for unknown elements
    TRANSFORMATION = "transformation"
    # Phase 2: AI Enrichment Types
    OPERATION_SUMMARY = "operation_summary"
    PIPELINE_SUMMARY = "pipeline_summary"


class EdgeType(str, Enum):
    """Canonical relationship types for technology-agnostic representation"""

    CONTAINS = "contains"
    READS_FROM = "reads_from"
    WRITES_TO = "writes_to"
    USES_CONNECTION = "uses_connection"
    USES_PARAMETER = "uses_parameter"
    USES_VARIABLE = "uses_variable"
    # Phase 2: AI Enrichment Types
    SUMMARIZES = "summarizes"
    EXECUTES = "executes"
    DERIVED_FROM = "derived_from"
    TRANSFORMS = "transforms"
    PART_OF = "part_of"
    REFERENCES = "references"
    CONFIGURES = "configures"
    PRECEDES = "precedes"
    # Cross-Package Dependency Types
    DEPENDS_ON = "depends_on"
    SHARES_RESOURCE = "shares_resource"
    # SQL Semantic Types for Migration
    JOINS_WITH = "joins_with"
    ALIASES_AS = "aliases_as"