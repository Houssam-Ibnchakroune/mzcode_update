import logging
from typing import Generator, Tuple, List, Optional, Dict, Any
import json
import re
from pathlib import Path

from ..ingestion_tool import IngestionTool
from .plsql_parser import CanonicalPlsqlParser
from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType


logger = logging.getLogger(__name__)


class PlsqlLoader(IngestionTool):
    """
    A PL/SQL ingestion tool that mirrors the SSIS framework architecture.
    It discovers and orchestrates the parsing of all relevant PL/SQL files (.sql/.pks/.pkb).
    """

    def __init__(self, root_path: str, target_file: Optional[str] = None, 
                 enable_type_mapping: bool = True,
                 target_platforms: Optional[List[str]] = None):
        super().__init__(root_path)
        self.target_file = target_file
        self.enable_type_mapping = enable_type_mapping
        self.target_platforms = target_platforms or ["sql_server", "postgresql"]

    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Discovers and parses all PL/SQL files in the project directory.
        First parses configuration files to build connection and parameter context for enrichment.
        """
        # Parse Oracle connection configurations (like tnsnames.ora, sqlnet.ora)
        connections_context = self._parse_oracle_connections()
        logger.info(
            f"Found {len(connections_context)} Oracle connection(s) for enrichment."
        )

        # Parse Oracle configuration parameters and variables
        parameters_context = self._parse_oracle_parameters()
        logger.info(
            f"Found {len(parameters_context)} Oracle parameter(s) for enrichment."
        )

        # Create connection nodes from Oracle configurations
        connection_nodes = self._create_connection_nodes_from_context(
            connections_context
        )
        logger.info(
            f"Created {len(connection_nodes)} connection node(s) from Oracle configs."
        )

        # Create parameter nodes from Oracle parameters
        parameter_nodes = self._create_parameter_nodes_from_context(parameters_context)
        logger.info(f"Created {len(parameter_nodes)} Oracle parameter node(s).")

        # Create parser with connection and parameter contexts (matching SSIS approach)
        parser = CanonicalPlsqlParser(
            connections_context=connections_context,
            parameters_context=parameters_context,
            enable_type_mapping=self.enable_type_mapping,
            target_platforms=self.target_platforms
        )
        
        # Discover PL/SQL files (equivalent to .dtsx discovery in SSIS)
        files: List[str] = []
        if self.target_file:
            files = [self.target_file]
        else:
            for pattern in ("*.sql", "*.pks", "*.pkb"):
                files.extend(self.discover_files(pattern))

        logger.info(f"Found {len(files)} PL/SQL file(s).")
        
        # Yield connection and parameter nodes first if any exist (matching SSIS pattern)
        all_global_nodes = connection_nodes + parameter_nodes
        if all_global_nodes:
            yield all_global_nodes, []
        
        for file_path in files:
            try:
                logger.info(f"Parsing PL/SQL file: {file_path}")
                yield from parser.parse(str(file_path))
            except Exception as e:
                logger.error(f"Failed to parse {file_path}: {e}", exc_info=True)
                continue

    def _parse_oracle_connections(self) -> Dict[str, Dict[str, Any]]:
        """
        Discovers and parses Oracle connection configurations (tnsnames.ora, connection scripts).
        Returns a mapping of connection names to their properties.
        """
        connections_context = {}
        
        # Look for tnsnames.ora files
        tnsnames_files = self.discover_files("tnsnames.ora")
        for tns_file in tnsnames_files:
            try:
                logger.debug(f"Parsing tnsnames.ora: {tns_file}")
                with open(tns_file, "r", encoding="utf-8") as f:
                    content = f.read()
                
                # Parse TNS entries using regex
                tns_pattern = r'(\w+)\s*=\s*\(DESCRIPTION\s*=\s*\(ADDRESS\s*=\s*\(PROTOCOL\s*=\s*(\w+)\)\s*\(HOST\s*=\s*([^)]+)\)\s*\(PORT\s*=\s*(\d+)\)\)\s*\(CONNECT_DATA\s*=\s*\(SERVICE_NAME\s*=\s*([^)]+)\)\)\)'
                
                for match in re.finditer(tns_pattern, content, re.IGNORECASE | re.MULTILINE):
                    conn_name = match.group(1)
                    protocol = match.group(2)
                    host = match.group(3)
                    port = match.group(4)
                    service_name = match.group(5)
                    
                    connections_context[conn_name] = {
                        "connection_name": conn_name,
                        "protocol": protocol,
                        "host": host,
                        "port": port,
                        "service_name": service_name,
                        "file_path": str(tns_file),
                        "connection_type": "oracle_tns"
                    }
                    
                    logger.debug(f"Parsed TNS connection: {conn_name} -> {host}:{port}/{service_name}")
                    
            except Exception as e:
                logger.error(f"Failed to parse tnsnames.ora {tns_file}: {e}")
                continue
        
        # Look for Oracle connection scripts (connect.sql, etc.)
        conn_scripts = self.discover_files("connect*.sql") + self.discover_files("*_connect.sql")
        for script_file in conn_scripts:
            try:
                logger.debug(f"Parsing connection script: {script_file}")
                with open(script_file, "r", encoding="utf-8") as f:
                    content = f.read()
                
                # Look for CONNECT statements
                connect_pattern = r'CONNECT\s+(\w+)/([^@\s]+)@([^\s;]+)'
                for match in re.finditer(connect_pattern, content, re.IGNORECASE):
                    username = match.group(1)
                    # password = match.group(2)  # Don't store passwords
                    connection_string = match.group(3)
                    
                    conn_name = f"{username}@{connection_string}"
                    connections_context[conn_name] = {
                        "connection_name": conn_name,
                        "username": username,
                        "connection_string": connection_string,
                        "file_path": str(script_file),
                        "connection_type": "oracle_connect_script"
                    }
                    
                    logger.debug(f"Parsed connection script: {conn_name}")
                    
            except Exception as e:
                logger.error(f"Failed to parse connection script {script_file}: {e}")
                continue
                
        return connections_context

    def _parse_oracle_parameters(self) -> Dict[str, Dict[str, Any]]:
        """
        Discovers and parses Oracle parameters and variables from configuration files.
        Returns a mapping of parameter names to their properties.
        """
        parameters_context = {}
        
        # Look for Oracle parameter files (params.sql, config.sql, variables.sql)
        param_files = (self.discover_files("params*.sql") + 
                      self.discover_files("config*.sql") + 
                      self.discover_files("variables*.sql") +
                      self.discover_files("*_params.sql"))
        
        for param_file in param_files:
            try:
                logger.debug(f"Parsing parameter file: {param_file}")
                with open(param_file, "r", encoding="utf-8") as f:
                    content = f.read()
                
                # Look for DEFINE statements (Oracle substitution variables)
                define_pattern = r'DEFINE\s+(\w+)\s*=\s*([^;\n]+)'
                for match in re.finditer(define_pattern, content, re.IGNORECASE):
                    param_name = match.group(1)
                    param_value = match.group(2).strip().strip("'\"")
                    
                    parameters_context[param_name] = {
                        "parameter_name": param_name,
                        "value": param_value,
                        "file_path": str(param_file),
                        "parameter_type": "oracle_define"
                    }
                    
                    logger.debug(f"Parsed DEFINE parameter: {param_name} = {param_value}")
                
                # Look for variable declarations in PL/SQL blocks
                var_pattern = r'(\w+)\s+(?:CONSTANT\s+)?(?:VARCHAR2|NUMBER|DATE|BOOLEAN)\s*(?:\([^)]+\))?\s*(?::=|DEFAULT)\s*([^;]+)'
                for match in re.finditer(var_pattern, content, re.IGNORECASE):
                    var_name = match.group(1)
                    var_value = match.group(2).strip().strip("'\"")
                    
                    if var_name.upper() not in ['BEGIN', 'END', 'IF', 'THEN', 'ELSE']:  # Skip keywords
                        parameters_context[var_name] = {
                            "parameter_name": var_name,
                            "value": var_value,
                            "file_path": str(param_file),
                            "parameter_type": "oracle_variable"
                        }
                        
                        logger.debug(f"Parsed PL/SQL variable: {var_name} = {var_value}")
                        
            except Exception as e:
                logger.error(f"Failed to parse parameter file {param_file}: {e}")
                continue
                
        return parameters_context

    def _create_connection_nodes_from_context(
        self, connections_context: Dict[str, Dict[str, Any]]
    ) -> List[Node]:
        """
        Creates connection nodes from the parsed Oracle connection context.
        """
        connection_nodes = []
        processed_connections = set()

        for key, conn_data in connections_context.items():
            conn_name = conn_data.get("connection_name")
            
            if not conn_name or conn_name in processed_connections:
                continue

            processed_connections.add(conn_name)

            conn_id = f"connection:{conn_name}"
            properties = {
                "file_path": conn_data.get("file_path", ""),
                "technology": "ORACLE",
                "connection_type": conn_data.get("connection_type", ""),
                "host": conn_data.get("host", ""),
                "port": conn_data.get("port", ""),
                "service_name": conn_data.get("service_name", ""),
                "protocol": conn_data.get("protocol", ""),
                "username": conn_data.get("username", ""),
                "connection_string": conn_data.get("connection_string", ""),
            }

            connection_nodes.append(
                Node(
                    node_id=conn_id,
                    node_type=NodeType.CONNECTION,
                    name=conn_name,
                    properties=properties,
                )
            )

        return connection_nodes

    def _create_parameter_nodes_from_context(
        self, parameters_context: Dict[str, Dict[str, Any]]
    ) -> List[Node]:
        """
        Creates parameter nodes from the parsed Oracle parameters.
        """
        parameter_nodes = []
        processed_parameters = set()

        for key, param_data in parameters_context.items():
            param_name = param_data.get("parameter_name")
            
            if not param_name or param_name in processed_parameters:
                continue

            processed_parameters.add(param_name)

            param_id = f"parameter:{param_name}"
            properties = {
                "value": param_data.get("value", ""),
                "file_path": param_data.get("file_path", ""),
                "parameter_type": param_data.get("parameter_type", ""),
                "technology": "ORACLE",
            }

            parameter_nodes.append(
                Node(
                    node_id=param_id,
                    node_type=NodeType.PARAMETER,
                    name=param_name,
                    properties=properties,
                )
            )

        return parameter_nodes
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get enhanced processing summary with validation report and metadata quality metrics."""
        validation_report = self.parser.get_validation_report()
        
        return {
            "loader_type": "PL/SQL Enhanced Loader",
            "validation_report": validation_report,
            "metadata_capabilities": {
                "type_mapping": True,
                "column_lineage": True,
                "operation_subtypes": True,
                "oracle_specific_types": True,
                "multi_platform_conversion": True,
                "conversion_confidence_scoring": True,
                "sql_semantics_parsing": True,
                "error_handling_detection": True
            },
            "supported_target_platforms": [
                "sql_server",
                "postgresql", 
                "mysql",
                "oracle",
                "snowflake",
                "bigquery",
                "azure_synapse"
            ],
            "quality_enhancements": [
                "Oracle data type to canonical type mapping",
                "Multi-platform type conversion with confidence scoring",
                "Column-level lineage extraction with SQLGlot parsing",
                "Operation subtype categorization (DATA_FLOW, EXECUTE, etc.)",
                "Comprehensive type validation and conversion risk analysis",
                "Enhanced metadata structure matching SSIS output quality",
                "SQL semantics extraction with JOIN analysis",
                "Error handling and exception detection"
            ]
        }
