from ..ingestion_tool import IngestionTool
from .ssis_parser import CanonicalSsisParser
from typing import Generator, Tuple, List, Dict, Any
from ...models.graph import Node, Edge
from ...models.canonical_types import NodeType
import logging
from lxml import etree
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class SsisLoader(IngestionTool):
    """
    An ingestion tool for SQL Server Integration Services (SSIS) projects.
    It discovers and orchestrates the parsing of all relevant SSIS files.
    """

    def ingest(self) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Discovers and parses all .dtsx files in the project directory.
        First parses .conmgr files to build connection context for enrichment.
        """
        # Parse connection managers first to build context
        connections_context = self._parse_connection_managers()
        logger.info(
            f"Found {len(connections_context)} connection manager(s) for enrichment."
        )

        # Parse project parameters to build parameter context
        parameters_context = self._parse_project_parameters()
        logger.info(
            f"Found {len(parameters_context)} project parameter(s) for enrichment."
        )

        # Create connection nodes from .conmgr files
        connection_nodes = self._create_connection_nodes_from_context(
            connections_context
        )
        logger.info(
            f"Created {len(connection_nodes)} connection node(s) from .conmgr files."
        )

        # Create parameter nodes from project parameters
        parameter_nodes = self._create_parameter_nodes_from_context(parameters_context)
        logger.info(f"Created {len(parameter_nodes)} project parameter node(s).")

        # Create parser with connection and parameter contexts
        parser = CanonicalSsisParser(
            connections_context=connections_context,
            parameters_context=parameters_context,
        )
        ssis_files = self.discover_files("*.dtsx")
        logger.info(f"Found {len(ssis_files)} SSIS package file(s).")

        # Yield connection and parameter nodes first if any exist
        all_global_nodes = connection_nodes + parameter_nodes
        if all_global_nodes:
            yield all_global_nodes, []

        for file_path in ssis_files:
            try:
                logger.info(f"Parsing file: {file_path}")
                yield from parser.parse(str(file_path))
            except Exception as e:
                logger.error(f"Failed to parse {file_path}: {e}", exc_info=True)
                continue

    def _parse_connection_managers(self) -> Dict[str, Dict[str, Any]]:
        """
        Discovers and parses .conmgr files to extract detailed connection properties.
        Returns a mapping of connection names/GUIDs to their properties.
        """
        connections_context = {}
        conmgr_files = self.discover_files("*.conmgr")

        for conmgr_file in conmgr_files:
            try:
                logger.debug(f"Parsing connection manager: {conmgr_file}")

                # Parse the XML file
                with open(conmgr_file, "r", encoding="utf-8") as f:
                    content = f.read()
                if content.startswith("\ufeff"):
                    content = content[1:]

                root = etree.fromstring(content.encode("utf-8"))
                ns_map = {"DTS": "www.microsoft.com/SqlServer/Dts"}

                # Extract connection details
                conn_name = root.get(f"{{{ns_map['DTS']}}}ObjectName")
                conn_guid = root.get(f"{{{ns_map['DTS']}}}DTSID", "").strip("{}")
                creation_name = root.get(f"{{{ns_map['DTS']}}}CreationName")

                # Extract connection string from ObjectData
                object_data = root.find("DTS:ObjectData", ns_map)
                if object_data is not None:
                    conn_mgr = object_data.find("DTS:ConnectionManager", ns_map)
                    if conn_mgr is not None:
                        conn_string = conn_mgr.get(
                            f"{{{ns_map['DTS']}}}ConnectionString", ""
                        )

                        # Parse connection string for detailed properties
                        properties = self._parse_connection_string(conn_string)
                        properties.update(
                            {
                                "connection_name": conn_name,
                                "guid": conn_guid,
                                "creation_name": creation_name,
                                "connection_string": conn_string,
                                "file_path": str(conmgr_file),
                            }
                        )

                        # Store by both name and GUID for flexible lookup
                        if conn_name:
                            connections_context[conn_name] = properties
                        if conn_guid:
                            connections_context[conn_guid] = properties

                        logger.debug(
                            f"Parsed connection: {conn_name} (GUID: {conn_guid})"
                        )

            except Exception as e:
                logger.error(f"Failed to parse connection manager {conmgr_file}: {e}")
                continue

        return connections_context

    def _parse_connection_string(self, conn_string: str) -> Dict[str, str]:
        """
        Parses an OLEDB connection string to extract individual properties.
        """
        properties = {}

        if not conn_string:
            return properties

        # Common connection string patterns
        patterns = {
            "server": r"Data Source=([^;]+)",
            "database": r"Initial Catalog=([^;]+)",
            "provider": r"Provider=([^;]+)",
            "security": r"Integrated Security=([^;]+)",
            "application": r"Application Name=([^;]+)",
        }

        for prop_name, pattern in patterns.items():
            match = re.search(pattern, conn_string, re.IGNORECASE)
            if match:
                properties[prop_name] = match.group(1).strip()

        return properties
    
    def _analyze_connection_expression(self, connection_string: str) -> Dict[str, Any]:
        """
        Analyze connection string for parameter/variable usage.
        """
        analysis = {
            "raw_connection_string": connection_string,
            "uses_parameters": [],
            "uses_variables": [],
            "is_parameterized": False,
            "resolved_connection_string": connection_string
        }
        
        if not connection_string:
            return analysis
        
        import re
        
        # Check for parameter references in connection string
        param_patterns = [
            r"\$Project::([\w\d_]+)",  # $Project::ParameterName
            r"\$Package::([\w\d_]+)",  # $Package::ParameterName
        ]
        
        for pattern in param_patterns:
            matches = re.findall(pattern, connection_string, re.IGNORECASE)
            for match in matches:
                param_name = match.strip()
                if param_name not in analysis["uses_parameters"]:
                    analysis["uses_parameters"].append(param_name)
                    analysis["is_parameterized"] = True
        
        # Check for variable references
        var_patterns = [
            r"@\[User::([^\]]+)\]",     # @[User::VariableName]
            r"@\[System::([^\]]+)\]",   # @[System::VariableName]
        ]
        
        for pattern in var_patterns:
            matches = re.findall(pattern, connection_string, re.IGNORECASE)
            for match in matches:
                var_name = match.strip()
                if var_name not in analysis["uses_variables"]:
                    analysis["uses_variables"].append(var_name)
                    analysis["is_parameterized"] = True
        
        # Note: In a full implementation, we would resolve the parameter/variable values
        # For now, we just identify their usage
        
        return analysis

    def _create_connection_nodes_from_context(
        self, connections_context: Dict[str, Dict[str, Any]]
    ) -> List[Node]:
        """
        Creates connection nodes from the parsed .conmgr context.
        """
        connection_nodes = []
        processed_connections = set()

        for key, conn_data in connections_context.items():
            # Skip if this is a duplicate (we store by both name and GUID)
            conn_name = conn_data.get("connection_name")
            
            # Enhanced connection properties with expression resolution
            enhanced_properties = conn_data.copy()
            connection_string = conn_data.get("connection_string", "")
            
            # Analyze connection string for parameter usage
            if connection_string:
                # Check if connection string uses expressions/parameters
                expression_analysis = self._analyze_connection_expression(connection_string)
                enhanced_properties["expression_analysis"] = expression_analysis
                
                # Parse connection string components
                conn_components = self._parse_connection_string(connection_string)
                enhanced_properties.update(conn_components)
            
            conn_data = enhanced_properties
            if not conn_name or conn_name in processed_connections:
                continue

            processed_connections.add(conn_name)

            conn_id = f"connection:{conn_name}"
            properties = {
                "file_path": conn_data.get("file_path", ""),
                "technology": "SSIS",
                "guid": conn_data.get("guid", ""),
                "server": conn_data.get("server", ""),
                "database": conn_data.get("database", ""),
                "provider": conn_data.get("provider", ""),
                "security": conn_data.get("security", ""),
                "connection_string": conn_data.get("connection_string", ""),
                "creation_name": conn_data.get("creation_name", ""),
                "conmgr_file": conn_data.get("file_path", ""),
                "expression_analysis": conn_data.get("expression_analysis", {}),
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

    def _parse_project_parameters(self) -> Dict[str, Dict[str, Any]]:
        """
        Discovers and parses Project.params files to extract project parameters.
        Returns a mapping of parameter names to their properties.
        """
        parameters_context = {}
        params_files = self.discover_files("Project.params")

        for params_file in params_files:
            try:
                logger.debug(f"Parsing project parameters: {params_file}")

                # Parse the XML file
                with open(params_file, "r", encoding="utf-8") as f:
                    content = f.read()
                if content.startswith("\ufeff"):
                    content = content[1:]

                root = etree.fromstring(content.encode("utf-8"))
                ns_map = {"DTS": "www.microsoft.com/SqlServer/Dts"}

                # Extract parameter details
                param_name = root.get(f"{{{ns_map['DTS']}}}ObjectName")
                param_value = root.get(f"{{{ns_map['DTS']}}}Value")

                # Store parameter in context
                if param_name:
                    parameters_context[param_name] = {
                        "value": param_value,
                        "file_path": str(params_file),
                    }

                    logger.debug(f"Parsed parameter: {param_name}")

            except Exception as e:
                logger.error(f"Failed to parse project parameters {params_file}: {e}")
                continue

        return parameters_context

    def _create_parameter_nodes_from_context(
        self, parameters_context: Dict[str, Dict[str, Any]]
    ) -> List[Node]:
        """
        Creates parameter nodes from the parsed project parameters.
        """
        parameter_nodes = []
        processed_parameters = set()

        for key, param_data in parameters_context.items():
            # Skip if this is a duplicate
            if key in processed_parameters:
                continue

            processed_parameters.add(key)

            param_id = f"parameter:{key}"
            properties = {
                "value": param_data.get("value", ""),
                "file_path": param_data.get("file_path", ""),
            }

            parameter_nodes.append(
                Node(
                    node_id=param_id,
                    node_type=NodeType.PARAMETER,
                    name=key,
                    properties=properties,
                )
            )

        return parameter_nodes
