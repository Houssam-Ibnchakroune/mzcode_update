import os
import re
from lxml import etree
from typing import Dict, List, Tuple, Generator, Any, Optional
import logging
import json

from ...models.canonical_types import NodeType, EdgeType
from ...models.graph import Node, Edge
from ...models.traceability import SourceContext
from .type_mapping import SSISDataTypeMapper, TargetPlatform
from .sql_semantics import EnhancedSqlParser, SqlSemantics, create_join_edges_from_semantics

logger = logging.getLogger(__name__)


class CanonicalSsisParser:
    """
    A robust SSIS parser built with lxml to create a canonical representation of the project.
    It parses a single SSIS package file (.dtsx).
    """

    def __init__(
        self,
        connections_context: Optional[Dict[str, Dict[str, Any]]] = None,
        parameters_context: Optional[Dict[str, Dict[str, Any]]] = None,
        enable_schema_introspection: bool = True,
        enable_type_mapping: bool = True,
        target_platforms: Optional[List[str]] = None,
    ):
        self.ns_map = {
            "DTS": "www.microsoft.com/SqlServer/Dts",
            "SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask",
        }
        self.connections_context = connections_context or {}
        self.parameters_context = parameters_context or {}
        self.enable_schema_introspection = enable_schema_introspection
        self.schema_cache = {}  # Cache for database schema information
        
        # Initialize type mapping engine
        self.enable_type_mapping = enable_type_mapping
        self.type_mapper = SSISDataTypeMapper() if enable_type_mapping else None
        self.target_platforms = self._parse_target_platforms(target_platforms or ["sql_server", "postgresql"])
        
        # Initialize enhanced SQL parser for migration support
        self.sql_parser = EnhancedSqlParser()
        self._pending_sql_semantics = None

    def _categorize_operation_subtype(self, native_type: str) -> str:
        """
        Categorizes SSIS operations into standardized subtypes based on their native type.

        Args:
            native_type: The SSIS native operation type (e.g., 'Microsoft.Pipeline')

        Returns:
            A standardized operation subtype: CONTROL_FLOW, DATA_FLOW, EXECUTE, or SCRIPT
        """
        # Data Flow operations
        if native_type in ["Microsoft.Pipeline"]:
            return "DATA_FLOW"

        # Control Flow containers
        elif native_type in ["STOCK:FORLOOP", "STOCK:FOREACHLOOP", "STOCK:SEQUENCE"]:
            return "CONTROL_FLOW"

        # Execute operations
        elif native_type in ["Microsoft.ExecuteSQLTask", "Microsoft.FileSystemTask"]:
            return "EXECUTE"

        # Script operations
        elif native_type in ["Microsoft.ScriptTask"]:
            return "SCRIPT"

        # Default fallback for unknown types
        else:
            logger.warning(
                f"Unknown operation native_type '{native_type}', defaulting to 'EXECUTE'"
            )
            return "EXECUTE"

    def parse(
        self, file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        """
        Parses a single .dtsx file and yields the discovered nodes and edges.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Remove the BOM if it exists
            if content.startswith("\ufeff"):
                content = content[1:]
            root = etree.fromstring(content.encode("utf-8"))
            yield from self._parse_package(root, file_path)
        except Exception as e:
            logger.error(
                f"ERROR: Could not parse {file_path}. Reason: {e}", exc_info=True
            )
            return

    def _parse_package(
        self, root: etree._Element, file_path: str
    ) -> Generator[Tuple[List[Node], List[Edge]], None, None]:
        nodes: List[Node] = []
        edges: List[Edge] = []

        # Parse connection managers first (local connections within .dtsx)
        connection_nodes, connection_id_map = self._parse_connection_managers(
            root, file_path
        )
        nodes.extend(connection_nodes)

        # Add external connections from .conmgr context to the ID map
        external_connection_map = self._build_external_connection_map()
        connection_id_map.update(external_connection_map)

        # Parse package parameters
        parameter_nodes, parameter_id_map = self._parse_package_parameters(
            root, file_path
        )
        nodes.extend(parameter_nodes)

        # Parse package variables (often used in parameter mappings)
        variable_nodes, variable_id_map = self._parse_package_variables(root, file_path)
        nodes.extend(variable_nodes)

        # Combine parameter and variable mappings for reference tracking
        param_var_id_map = {**parameter_id_map, **variable_id_map}

        # Create the pipeline node
        package_name_elem = root.find(
            "DTS:Property[@DTS:Name='ObjectName']", self.ns_map
        )
        package_name = (
            package_name_elem.text
            if package_name_elem is not None
            else os.path.basename(file_path)
        )

        pipeline_id = f"pipeline:{package_name}"
        # Create enhanced properties with traceability
        pipeline_properties = {
            "technology": "SSIS",
            **SourceContext.create_node_traceability(
                source_file_path=file_path,
                source_file_type="dtsx",
                xml_path="//DTS:Executable[@DTS:ExecutableType='Package']"
            )
        }
        
        nodes.append(
            Node(
                node_id=pipeline_id,
                node_type=NodeType.PIPELINE,
                name=package_name,
                properties=pipeline_properties,
            )
        )

        # Parse tasks
        executables_container = root.find("DTS:Executables", self.ns_map)
        if executables_container is None:
            yield nodes, edges
            return

        for task_xml in executables_container.findall("DTS:Executable", self.ns_map):
            task_name = task_xml.get(f"{{{self.ns_map['DTS']}}}ObjectName")
            task_type = task_xml.get(f"{{{self.ns_map['DTS']}}}ExecutableType")

            if not task_name or not task_type:
                continue

            task_id = f"{pipeline_id}:operation:{task_name}"

            # Get operation subtype
            operation_subtype = self._categorize_operation_subtype(task_type)

            # Create operation node with enhanced traceability
            operation_properties = {
                "native_type": task_type,
                "operation_subtype": operation_subtype,
                "technology": "SSIS",
                **SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="dtsx",
                    xml_path=f"//DTS:Executable[@DTS:ObjectName='{task_name}']",
                    parent_package=package_name
                )
            }
            
            nodes.append(
                Node(
                    node_id=task_id,
                    node_type=NodeType.OPERATION,
                    name=task_name,
                    properties=operation_properties,
                )
            )
            edges.append(
                Edge(
                    source_id=pipeline_id, 
                    target_id=task_id, 
                    relation=EdgeType.CONTAINS,
                    properties=SourceContext.create_edge_traceability(
                        source_file_path=file_path,
                        derivation_method="xml_metadata",
                        xml_location=f"//DTS:Executable[@DTS:ObjectName='{task_name}']",
                        context_info=SourceContext.create_xml_derivation_context(
                            xml_element_name="DTS:Executable",
                            xml_attribute="DTS:ObjectName"
                        )
                    )
                )
            )

            # Parse task details
            object_data_xml = task_xml.find("DTS:ObjectData", self.ns_map)
            if object_data_xml is None:
                continue

            task_type_element = object_data_xml.find("*")
            if task_type_element is None:
                continue

            task_type = task_type_element.tag
            if "pipeline" in task_type.lower():
                # The task_type_element IS the pipeline element
                pipeline_xml = task_type_element
                components_xml = pipeline_xml.find("components")
                if components_xml is None:
                    continue

                for component_xml in components_xml.findall("component"):
                    self._parse_dft_component(
                        component_xml,
                        task_id,
                        nodes,
                        edges,
                        connection_id_map,
                        param_var_id_map,
                        file_path,
                    )
                    
                    # Apply pending SQL semantics if they were collected during component parsing
                    if (self._pending_sql_semantics and 
                        self._pending_sql_semantics.get("task_id") == task_id):
                        # Find the operation node and add SQL semantics
                        operation_node = next((n for n in nodes if n.node_id == task_id), None)
                        if operation_node:
                            operation_node.properties["sql_semantics"] = json.dumps(
                                self._pending_sql_semantics["sql_semantics"]
                            )
                            self._pending_sql_semantics = None  # Clear pending semantics

            elif "SqlTaskData" in task_type:
                self._parse_execute_sql_task(
                    object_data_xml,
                    task_id,
                    nodes,
                    edges,
                    connection_id_map,
                    param_var_id_map,
                )
            elif "ScriptTaskData" in task_type or "ScriptProject" in task_type:
                self._parse_script_task(
                    object_data_xml,
                    task_id,
                    nodes,
                    edges,
                    param_var_id_map,
                )
            else:
                logger.debug(
                    f"DEBUG: Unhandled task type: '{task_type}' for task: '{task_name}'"
                )

        # Parse precedence constraints (control flow)
        self._parse_precedence_constraints(root, pipeline_id, edges)

        yield nodes, edges

    def _parse_connection_managers(
        self, root: etree._Element, file_path: str
    ) -> Tuple[List[Node], Dict[str, str]]:
        connection_nodes: List[Node] = []
        id_map: Dict[str, str] = {}
        connections_container = root.find("DTS:ConnectionManagers", self.ns_map)
        if connections_container is None:
            return [], {}

        for conn_xml in connections_container.findall(
            "DTS:ConnectionManager", self.ns_map
        ):
            conn_name = conn_xml.get(f"{{{self.ns_map['DTS']}}}ObjectName")
            conn_guid = conn_xml.get(f"{{{self.ns_map['DTS']}}}ID")

            if not conn_name or not conn_guid:
                continue

            conn_id = f"connection:{conn_name}"

            # Start with basic properties including enhanced traceability
            properties = {
                "technology": "SSIS",
                "guid": conn_guid,
                **SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="dtsx",
                    xml_path=f"//DTS:ConnectionManager[@DTS:ObjectName='{conn_name}']"
                )
            }

            # Enrich with detailed properties from .conmgr files
            conn_guid_clean = conn_guid.strip("{}")
            enrichment_data = None

            # Try to find enrichment data by name or GUID
            if conn_name in self.connections_context:
                enrichment_data = self.connections_context[conn_name]
                logger.debug(
                    f"Found enrichment data for connection '{conn_name}' by name"
                )
            elif conn_guid_clean in self.connections_context:
                enrichment_data = self.connections_context[conn_guid_clean]
                logger.debug(
                    f"Found enrichment data for connection '{conn_name}' by GUID"
                )

            if enrichment_data:
                # Merge enrichment properties
                properties.update(
                    {
                        "server": enrichment_data.get("server", ""),
                        "database": enrichment_data.get("database", ""),
                        "provider": enrichment_data.get("provider", ""),
                        "security": enrichment_data.get("security", ""),
                        "connection_string": enrichment_data.get(
                            "connection_string", ""
                        ),
                        "creation_name": enrichment_data.get("creation_name", ""),
                        "conmgr_file": enrichment_data.get("file_path", ""),
                    }
                )
                
                # Add platform detection and type mapping rules
                if self.enable_type_mapping:
                    platform_type = self._detect_platform_from_connection(enrichment_data)
                    if platform_type:
                        properties["platform_type"] = platform_type.value
                        properties["type_mapping_rules"] = self._get_platform_type_rules(platform_type)
                logger.debug(
                    f"Enriched connection '{conn_name}' with server='{enrichment_data.get('server')}', database='{enrichment_data.get('database')}'"
                )
            else:
                logger.debug(
                    f"No enrichment data found for connection '{conn_name}' (GUID: {conn_guid_clean})"
                )

            connection_nodes.append(
                Node(
                    node_id=conn_id,
                    node_type=NodeType.CONNECTION,
                    name=conn_name,
                    properties=properties,
                )
            )
            id_map[conn_guid] = conn_id

        return connection_nodes, id_map

    def _build_external_connection_map(self) -> Dict[str, str]:
        """
        Builds a mapping from connection GUIDs to connection node IDs for external .conmgr connections.
        """
        external_map = {}

        for key, conn_data in self.connections_context.items():
            conn_guid = conn_data.get("guid")
            conn_name = conn_data.get("connection_name")

            if conn_guid and conn_name:
                # Map GUID to connection node ID
                conn_id = f"connection:{conn_name}"
                external_map[conn_guid] = conn_id
                logger.debug(
                    f"Mapped external connection GUID '{conn_guid}' to node '{conn_id}'"
                )

        return external_map

    def _parse_dft_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        connection_id_map: Dict[str, str],
        param_var_id_map: Dict[str, str],
        file_path: str,
    ):
        class_id = component_xml.get("componentClassID", "")
        component_name = component_xml.get("name", "")

        # Extract column lineage for all components
        column_lineage = self._extract_column_lineage(
            component_xml, component_name, task_id
        )

        # Handle different component types
        if "Microsoft.DerivedColumn" in class_id:
            self._parse_derived_column_component(
                component_xml, task_id, nodes, edges, param_var_id_map
            )
        elif "Microsoft.ConditionalSplit" in class_id:
            self._parse_conditional_split_component(
                component_xml, task_id, nodes, edges, param_var_id_map
            )
        elif "Microsoft.Lookup" in class_id:
            self._parse_lookup_component(
                component_xml, task_id, nodes, edges, param_var_id_map
            )
        elif "Microsoft.OLEDBCommand" in class_id:
            self._parse_oledb_command_component(
                component_xml, task_id, nodes, edges, connection_id_map, param_var_id_map, file_path
            )
        elif "OLEDBSource" in class_id or "OLEDBDestination" in class_id:
            self._parse_oledb_component(
                component_xml,
                task_id,
                nodes,
                edges,
                connection_id_map,
                param_var_id_map,
                file_path,
            )
        # More transformation types will be added in subsequent phases
        else:
            # Use generic component parser for unsupported types
            self._parse_generic_component(
                component_xml, task_id, nodes, edges, column_lineage
            )

        # Add column lineage to the operation node if we have any column information
        if column_lineage["input_columns"] or column_lineage["output_columns"]:
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                if "column_lineage" not in operation_node.properties:
                    operation_node.properties["column_lineage"] = []
                operation_node.properties["column_lineage"].append(column_lineage)

                logger.debug(
                    f"Added column lineage for component {component_name}: "
                    f"{len(column_lineage['input_columns'])} inputs, "
                    f"{len(column_lineage['output_columns'])} outputs, "
                    f"{len(column_lineage['column_mappings'])} mappings"
                )
        
        # Extract error handling configuration for all components
        self._extract_error_handling_config(component_xml, task_id, nodes)

    def _parse_derived_column_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse Microsoft.DerivedColumn transformation components to extract
        transformation expressions and business logic.
        """
        component_name = component_xml.get("name", "")
        logger.debug(f"Parsing derived column component: {component_name}")

        # Look for output columns with expressions
        outputs_tag = component_xml.find("outputs")
        if outputs_tag is None:
            return

        transformations = []

        for output in outputs_tag.findall("output"):
            # Skip error outputs
            if output.get("isErrorOut") == "true":
                continue

            output_columns = output.find("outputColumns")
            if output_columns is None:
                continue

            for output_column in output_columns.findall("outputColumn"):
                column_name = output_column.get("name", "")
                properties_tag = output_column.find("properties")

                if properties_tag is not None:
                    # Extract the transformation expression
                    expression_prop = properties_tag.find(
                        "property[@name='Expression']"
                    )
                    friendly_expr_prop = properties_tag.find(
                        "property[@name='FriendlyExpression']"
                    )

                    if expression_prop is not None and expression_prop.text:
                        expression = expression_prop.text
                        friendly_expression = (
                            friendly_expr_prop.text
                            if friendly_expr_prop is not None
                            else expression
                        )

                        # Enrich with type mapping properties
                        type_properties = {}
                        if self.enable_type_mapping and self.type_mapper:
                            ssis_type = output_column.get("dataType", "")
                            length = output_column.get("length", "")
                            precision = output_column.get("precision", "")
                            scale = output_column.get("scale", "")
                            
                            if ssis_type:
                                type_properties = self.type_mapper.enrich_column_properties(
                                    ssis_type=ssis_type,
                                    length=length,
                                    precision=precision, 
                                    scale=scale,
                                    target_platforms=self.target_platforms
                                )

                        transformation = {
                            "column_name": column_name,
                            "expression": expression,
                            "friendly_expression": friendly_expression,
                            "data_type": output_column.get("dataType", ""),
                            "length": output_column.get("length", ""),
                            **type_properties  # Add type mapping properties
                        }
                        transformations.append(transformation)

                        logger.debug(
                            f"Found derived column expression: {column_name} = {expression}"
                        )

                        # Parse expression for variable/parameter references
                        self._parse_expression_dependencies(
                            expression, task_id, edges, param_var_id_map
                        )

        # Store transformations in the operation node properties
        if transformations:
            # Find or create the operation node for this component
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                if "transformations" not in operation_node.properties:
                    operation_node.properties["transformations"] = []
                operation_node.properties["transformations"].extend(transformations)

                logger.debug(
                    f"Added {len(transformations)} derived column transformations to {task_id}"
                )
                
                # Store derived column expressions in business logic format
                operation_node.properties["derived_column_expressions"] = {
                    "transformation_count": len(transformations),
                    "expressions": transformations,
                    "component_name": component_xml.get("name", "")
                }

    def _parse_conditional_split_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse Microsoft.ConditionalSplit transformation components to extract
        conditional expressions and routing logic.
        """
        component_name = component_xml.get("name", "")
        logger.debug(f"Parsing conditional split component: {component_name}")

        # Look for output branches with conditions
        outputs_tag = component_xml.find("outputs")
        if outputs_tag is None:
            return

        conditions = []
        default_output = None

        for output in outputs_tag.findall("output"):
            # Skip error outputs
            if output.get("isErrorOut") == "true":
                continue

            output_name = output.get("name", "")
            properties_tag = output.find("properties")

            if properties_tag is not None:
                # Check if this is the default output
                is_default_prop = properties_tag.find("property[@name='IsDefaultOut']")
                if is_default_prop is not None and is_default_prop.text == "true":
                    default_output = {
                        "output_name": output_name,
                        "is_default": True,
                        "description": output.get("description", ""),
                    }
                    continue

                # Extract the conditional expression
                expression_prop = properties_tag.find("property[@name='Expression']")
                friendly_expr_prop = properties_tag.find(
                    "property[@name='FriendlyExpression']"
                )
                evaluation_order_prop = properties_tag.find(
                    "property[@name='EvaluationOrder']"
                )

                if expression_prop is not None and expression_prop.text:
                    expression = expression_prop.text
                    friendly_expression = (
                        friendly_expr_prop.text
                        if friendly_expr_prop is not None
                        else expression
                    )
                    evaluation_order = (
                        int(evaluation_order_prop.text)
                        if evaluation_order_prop is not None
                        else 0
                    )

                    condition = {
                        "output_name": output_name,
                        "expression": expression,
                        "friendly_expression": friendly_expression,
                        "evaluation_order": evaluation_order,
                        "description": output.get("description", ""),
                        "is_default": False,
                    }
                    conditions.append(condition)

                    logger.debug(
                        f"Found conditional split condition [{evaluation_order}]: {output_name} = {friendly_expression}"
                    )

                    # Parse expression for variable/parameter references
                    self._parse_expression_dependencies(
                        expression, task_id, edges, param_var_id_map
                    )

        # Sort conditions by evaluation order
        conditions.sort(key=lambda x: x["evaluation_order"])

        # Add default output to the end if it exists
        if default_output:
            conditions.append(default_output)

        # Store conditions in the operation node properties
        if conditions:
            # Find the operation node for this component
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                if "conditions" not in operation_node.properties:
                    operation_node.properties["conditions"] = []
                operation_node.properties["conditions"].extend(conditions)

                logger.debug(
                    f"Added {len(conditions)} conditional split conditions to {task_id}"
                )

    def _parse_expression_dependencies(
        self,
        expression: str,
        task_id: str,
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse SSIS expressions to identify variable and parameter references.

        Common SSIS expression patterns:
        - Variables: @[User::VariableName] or @VariableName
        - Parameters: $Project::ParameterName or $Package::ParameterName
        - Functions: GETDATE(), LEN(), SUBSTRING(), etc.
        - Column references: [ColumnName] or just ColumnName
        """
        if not expression:
            return

        import re

        # Pattern for variable references: @[User::VariableName] or @[System::VariableName]
        variable_pattern = r"@\[(?:User::|System::)?([^\]]+)\]"
        variable_matches = re.findall(variable_pattern, expression, re.IGNORECASE)

        for var_name in variable_matches:
            # Look for variable in our mapping
            var_id = f"variable:{var_name}"
            if var_id in param_var_id_map.values():
                edges.append(
                    Edge(
                        source_id=task_id,
                        target_id=var_id,
                        relation=EdgeType.USES_VARIABLE,
                    )
                )
                logger.debug(f"Found variable reference in expression: {var_name}")

        # Pattern for parameter references: $Project::ParamName or $Package::ParamName
        param_pattern = r"\$(?:Project::|Package::)([^\s\)]+)"
        param_matches = re.findall(param_pattern, expression, re.IGNORECASE)

        for param_name in param_matches:
            param_id = f"parameter:{param_name}"
            if param_id in param_var_id_map.values():
                edges.append(
                    Edge(
                        source_id=task_id,
                        target_id=param_id,
                        relation=EdgeType.USES_PARAMETER,
                    )
                )
                logger.debug(f"Found parameter reference in expression: {param_name}")

    def _parse_oledb_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        connection_id_map: Dict[str, str],
        param_var_id_map: Dict[str, str],
        file_path: str,
    ):
        """
        Parse OLE DB Source and Destination components (extracted from original method).
        """
        class_id = component_xml.get("componentClassID", "")
        is_source = "OLEDBSource" in class_id
        is_destination = "OLEDBDestination" in class_id

        if not is_source and not is_destination:
            return

        # Parse connection
        conn_guid = None
        connections_tag = component_xml.find("connections")
        if connections_tag is not None:
            connection_tag = connections_tag.find("connection")
            if connection_tag is not None:
                conn_mgr_id_attr = connection_tag.get("connectionManagerID")
                if conn_mgr_id_attr:
                    conn_guid = conn_mgr_id_attr.split(":")[0].strip("{}")

        if conn_guid and conn_guid in connection_id_map:
            edges.append(
                Edge(
                    source_id=task_id,
                    target_id=connection_id_map[conn_guid],
                    relation=EdgeType.USES_CONNECTION,
                    properties=SourceContext.create_edge_traceability(
                        source_file_path=file_path,
                        derivation_method="xml_metadata",
                        xml_location="//connections/connection[@connectionManagerID]",
                        context_info=SourceContext.create_xml_derivation_context(
                            xml_element_name="connection",
                            xml_attribute="connectionManagerID"
                        ),
                        confidence_level="high"
                    )
                )
            )

        # Parse parameter mappings
        properties_tag = component_xml.find("properties")
        if properties_tag is not None:
            param_mapping_prop = properties_tag.find(
                "property[@name='ParameterMapping']"
            )
            if param_mapping_prop is not None and param_mapping_prop.text:
                self._parse_parameter_mapping(
                    param_mapping_prop.text, task_id, edges, param_var_id_map
                )

        # Parse table information and SQL semantics
        table_name = None
        sql_tables_processed = False
        sql_semantics = None
        
        if properties_tag is not None:
            # First, check for SQL commands and process ALL tables from them
            sql_command_prop = properties_tag.find("property[@name='SqlCommand']")
            if sql_command_prop is not None and sql_command_prop.text and "SELECT" in sql_command_prop.text.upper():
                # Parse complete SQL semantics for migration support
                sql_semantics = self.sql_parser.parse_sql_semantics(sql_command_prop.text)
                
                # Use tables from SQL semantics instead of old regex method
                if sql_semantics and sql_semantics.tables:
                    # Create table nodes for ALL tables found in SQL semantics
                    for table_ref in sql_semantics.tables:
                        sql_table_name = table_ref.name
                        table_name_clean = sql_table_name.strip("[]")
                        table_id = f"table:{table_name_clean}"
                        
                        # Create table node if it doesn't exist
                        if not any(n.node_id == table_id for n in nodes):
                            table_properties = {
                                "technology": "SSIS",
                                "table_name": table_name_clean,
                                **SourceContext.create_node_traceability(
                                    source_file_path=file_path,
                                    source_file_type="dtsx",
                                    xml_path=f"//SqlTask[contains(SqlCommand, '{table_name_clean}')]",
                                    parent_package=os.path.basename(file_path).replace('.dtsx', '')
                                )
                            }
                            
                            # Add platform type mapping support
                            if self.enable_type_mapping and self.target_platforms:
                                table_properties["supported_platforms"] = [p.value for p in self.target_platforms]
                                table_properties["type_mapping_enabled"] = True
                            
                            # Add schema introspection if connection context is available
                            if conn_guid and conn_guid in self.connections_context:
                                conn_info = self.connections_context[conn_guid]
                                connection_string = conn_info.get("connection_string")
                                if connection_string:
                                    schema_info = self._introspect_table_schema(connection_string, table_name_clean)
                                    table_properties["schema_details"] = schema_info
                                    
                                    # Add connection reference
                                    table_properties["connection_id"] = conn_guid
                                    table_properties["database"] = conn_info.get("database")
                                    table_properties["server"] = conn_info.get("server")
                            
                            nodes.append(Node(
                                node_id=table_id,
                                node_type=NodeType.DATA_ASSET,
                                name=table_name_clean,
                                properties=table_properties,
                            ))
                        
                        # Create edge to the table with SQL parsing traceability
                        relation = EdgeType.READS_FROM if is_source else EdgeType.WRITES_TO
                        edges.append(Edge(
                            source_id=task_id, 
                            target_id=table_id, 
                            relation=relation,
                            properties=SourceContext.create_edge_traceability(
                                source_file_path=file_path,
                                derivation_method="sql_parsing",
                                xml_location="//property[@name='SqlCommand']",
                                context_info=SourceContext.create_sql_derivation_context(
                                    sql_statement=sql_command_prop.text,
                                    component_type="Execute SQL Task",
                                    property_name="SqlCommand"
                                ),
                                confidence_level="high"
                            )
                        ))
                    
                    # Store SQL semantics for later application to operation node
                    # (Operation node may not be in nodes list yet at this point)
                    if sql_semantics:
                        # Return or store the SQL semantics to be applied to the operation node later
                        self._pending_sql_semantics = {
                            "task_id": task_id,
                            "sql_semantics": sql_semantics.to_dict()
                        }
                    
                    # Create JOIN relationship edges
                    if sql_semantics and sql_semantics.joins:
                        join_edges = create_join_edges_from_semantics(sql_semantics)
                        for join_edge_data in join_edges:
                            try:
                                edges.append(Edge(
                                    source_id=join_edge_data["source_id"],
                                    target_id=join_edge_data["target_id"],
                                    relation=EdgeType(join_edge_data["edge_type"]),
                                    properties=join_edge_data["properties"]
                                ))
                            except ValueError:
                                # Fallback to REFERENCES if new edge type not available
                                edges.append(Edge(
                                    source_id=join_edge_data["source_id"],
                                    target_id=join_edge_data["target_id"],
                                    relation=EdgeType.REFERENCES,
                                    properties=join_edge_data["properties"]
                                ))
                    
                    sql_tables_processed = True
            
            # Then, check for simple table references (OpenRowset, TableName) only if no SQL was processed
            if not sql_tables_processed:
                prop_names_to_check = ["OpenRowset", "TableName"]
                for prop_name in prop_names_to_check:
                    prop = properties_tag.find(f"property[@name='{prop_name}']")
                    if prop is not None and prop.text:
                        table_name = prop.text
                        break

        if table_name:
            table_name = table_name.strip("[]")
            table_id = f"table:{table_name}"
            if not any(n.node_id == table_id for n in nodes):
                # Enhanced table properties with schema introspection and platform mapping
                table_properties = {
                    "technology": "SSIS",
                    "table_name": table_name,
                    **SourceContext.create_node_traceability(
                        source_file_path=file_path,
                        source_file_type="dtsx",
                        xml_path=f"//DataFlowComponent[contains(@name, '{table_name}')]",
                        parent_package=os.path.basename(file_path).replace('.dtsx', '')
                    )
                }
                
                # Add platform type mapping support for table-level operations
                if self.enable_type_mapping and self.target_platforms:
                    table_properties["supported_platforms"] = [p.value for p in self.target_platforms]
                    table_properties["type_mapping_enabled"] = True
                
                # Add schema introspection if connection context is available
                if conn_guid and conn_guid in self.connections_context:
                    conn_info = self.connections_context[conn_guid]
                    connection_string = conn_info.get("connection_string")
                    if connection_string:
                        schema_info = self._introspect_table_schema(connection_string, table_name)
                        table_properties["schema_details"] = schema_info
                        
                        # Add connection reference
                        table_properties["connection_id"] = conn_guid
                        table_properties["database"] = conn_info.get("database")
                        table_properties["server"] = conn_info.get("server")
                
                nodes.append(
                    Node(
                        node_id=table_id,
                        node_type=NodeType.TABLE,
                        name=table_name,
                        properties=table_properties,
                    )
                )

            relation = EdgeType.READS_FROM if is_source else EdgeType.WRITES_TO
            # Determine which property was used for the table reference
            prop_name_used = "OpenRowset" if "OpenRowset" in str(properties_tag) else "TableName"
            edges.append(Edge(
                source_id=task_id, 
                target_id=table_id, 
                relation=relation,
                properties=SourceContext.create_edge_traceability(
                    source_file_path=file_path,
                    derivation_method="xml_metadata",
                    xml_location=f"//property[@name='{prop_name_used}']",
                    context_info=SourceContext.create_xml_derivation_context(
                        xml_element_name="property",
                        xml_attribute="name",
                        xml_property=prop_name_used
                    ),
                    confidence_level="high"
                )
            ))

    def _parse_parameter_mapping(
        self,
        mapping_text: str,
        task_id: str,
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parses parameter mapping strings like:
        "Parameter0:Input",{6A1D4288-D951-460C-A98D-17C167544F57};
        """
        if not mapping_text:
            return

        # Extract parameter GUID from mapping text
        import re

        guid_pattern = r"\{([A-F0-9\-]+)\}"
        matches = re.findall(guid_pattern, mapping_text, re.IGNORECASE)

        for guid in matches:
            if guid in param_var_id_map:
                target_id = param_var_id_map[guid]

                # Determine if it's a parameter or variable based on node ID prefix
                if target_id.startswith("parameter:"):
                    relation = EdgeType.USES_PARAMETER
                    edge_type = "USES_PARAMETER"
                elif target_id.startswith("variable:"):
                    relation = EdgeType.USES_VARIABLE
                    edge_type = "USES_VARIABLE"
                else:
                    relation = EdgeType.USES_PARAMETER  # Default fallback
                    edge_type = "USES_PARAMETER"

                edges.append(
                    Edge(
                        source_id=task_id,
                        target_id=target_id,
                        relation=relation,
                    )
                )
                logger.debug(f"Created {edge_type} edge: {task_id} -> {target_id}")
            else:
                logger.debug(f"Parameter/Variable GUID {guid} not found in mapping")

    def _parse_execute_sql_task(
        self,
        object_data_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        connection_id_map: Dict[str, str],
        param_var_id_map: Dict[str, str],
    ):
        sql_task_data_xml = object_data_xml.find("SQLTask:SqlTaskData", self.ns_map)
        if sql_task_data_xml is None:
            return

        # Parse connection
        connection_ref = sql_task_data_xml.get(
            f"{{{self.ns_map['SQLTask']}}}Connection"
        )
        if connection_ref and connection_ref in connection_id_map:
            edges.append(
                Edge(
                    source_id=task_id,
                    target_id=connection_id_map[connection_ref],
                    relation=EdgeType.USES_CONNECTION,
                    properties=SourceContext.create_edge_traceability(
                        source_file_path=file_path,
                        derivation_method="xml_metadata",
                        xml_location="//SqlTaskData[@Connection]",
                        context_info=SourceContext.create_xml_derivation_context(
                            xml_element_name="SqlTaskData",
                            xml_attribute="Connection"
                        ),
                        confidence_level="high"
                    )
                )
            )

        # Parse SQL statement - this is the embedded script content
        sql_statement = sql_task_data_xml.get(
            f"{{{self.ns_map['SQLTask']}}}SqlStatementSource"
        )
        if sql_statement:
            # Store the embedded SQL in the operation node properties
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                # Enhanced SQL transformation logic extraction
                sql_info = {
                    "sql_query": sql_statement,
                    "connection_ref": connection_ref,
                    "query_type": self._determine_sql_type(sql_statement),
                    "parameters": self._extract_sql_parameters(sql_statement),
                    "affected_tables": self._extract_table_references(sql_statement),
                    "has_placeholders": "?" in sql_statement
                }
                operation_node.properties["sql_transformation"] = sql_info

                logger.debug(
                    f"Added SQL transformation logic to {task_id}: {sql_statement[:100]}..."
                )

            # Check for parameter placeholders (? marks) in SQL
            if "?" in sql_statement:
                logger.debug(
                    f"Found parameterized SQL in task {task_id}: {sql_statement[:100]}..."
                )

                # Look for parameter mapping properties
                param_mapping_prop = sql_task_data_xml.find(
                    "DTS:Property[@DTS:Name='ParameterMapping']", self.ns_map
                )
                if param_mapping_prop is not None and param_mapping_prop.text:
                    self._parse_parameter_mapping(
                        param_mapping_prop.text, task_id, edges, param_var_id_map
                    )

            # Extract table references from SQL for lineage
            found_tables = re.findall(
                r"(?:FROM|JOIN|UPDATE|INTO)\s+\[?(\w+)\]?\.\[?(\w+)\]?",
                sql_statement,
                re.IGNORECASE,
            )
            for schema, table in found_tables:
                table_name = f"{schema}.{table}"
                table_id = f"table:{table_name}"
                if not any(n.node_id == table_id for n in nodes):
                    nodes.append(
                        Node(
                            node_id=table_id,
                            node_type=NodeType.TABLE,
                            name=table_name,
                            properties={"technology": "SSIS"},
                        )
                    )

                relation = (
                    EdgeType.WRITES_TO
                    if "UPDATE" in sql_statement.upper()
                    or "INSERT" in sql_statement.upper()
                    else EdgeType.READS_FROM
                )
                edges.append(
                    Edge(source_id=task_id, target_id=table_id, relation=relation)
                )

            # Parse expression for variable/parameter references
            self._parse_expression_dependencies(
                sql_statement, task_id, edges, param_var_id_map
            )

    def _determine_sql_type(self, sql_statement: str) -> str:
        """Determine the type of SQL operation."""
        sql_upper = sql_statement.upper().strip()
        if sql_upper.startswith('SELECT'):
            return 'SELECT'
        elif sql_upper.startswith('INSERT'):
            return 'INSERT'
        elif sql_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif sql_upper.startswith('DELETE'):
            return 'DELETE'
        elif sql_upper.startswith('EXEC'):
            return 'EXECUTE'
        elif sql_upper.startswith('CREATE'):
            return 'CREATE'
        elif sql_upper.startswith('DROP'):
            return 'DROP'
        else:
            return 'UNKNOWN'

    def _extract_sql_parameters(self, sql_statement: str) -> List[Dict[str, Any]]:
        """Extract parameter placeholders and their positions."""
        parameters = []
        param_count = sql_statement.count('?')
        for i in range(param_count):
            parameters.append({
                "position": i,
                "placeholder": "?",
                "description": f"Parameter {i+1}"
            })
        return parameters

    def _extract_table_references(self, sql_statement: str) -> List[Dict[str, str]]:
        """Extract table references from SQL statement."""
        tables = []
        # Enhanced regex to capture various SQL patterns
        patterns = [
            r"FROM\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?",
            r"JOIN\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?",
            r"UPDATE\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?",
            r"INSERT\s+INTO\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?",
            r"DELETE\s+FROM\s+\[?([\w\d_]+)\]?\.\[?([\w\d_]+)\]?"
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql_statement, re.IGNORECASE)
            for schema, table in matches:
                tables.append({
                    "schema": schema,
                    "table": table,
                    "full_name": f"{schema}.{table}"
                })
        
        return tables

    def _parse_oledb_command_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        connection_id_map: Dict[str, str],
        param_var_id_map: Dict[str, str],
        file_path: str,
    ):
        """
        Parse Microsoft.OLEDBCommand components to extract SQL command logic.
        This handles embedded SQL commands within data flows.
        """
        component_name = component_xml.get("name", "")
        logger.debug(f"Parsing OLE DB Command component: {component_name}")
        
        # Extract SQL command from properties
        properties_tag = component_xml.find("properties")
        if properties_tag is not None:
            sql_command_prop = properties_tag.find("property[@name='SqlCommand']")
            if sql_command_prop is not None and sql_command_prop.text:
                sql_command = sql_command_prop.text.strip()
                
                # Find the operation node and add SQL transformation logic
                operation_node = next((n for n in nodes if n.node_id == task_id), None)
                if operation_node:
                    # Extract connection information
                    conn_ref = None
                    connections_tag = component_xml.find("connections")
                    if connections_tag is not None:
                        connection_tag = connections_tag.find("connection")
                        if connection_tag is not None:
                            conn_mgr_id = connection_tag.get("connectionManagerID")
                            if conn_mgr_id:
                                conn_guid = conn_mgr_id.split(":")[0].strip("{}")
                                if conn_guid in connection_id_map:
                                    conn_ref = connection_id_map[conn_guid]
                    
                    # Create SQL transformation info
                    sql_info = {
                        "sql_query": sql_command,
                        "connection_ref": conn_ref,
                        "query_type": self._determine_sql_type(sql_command),
                        "parameters": self._extract_sql_parameters(sql_command),
                        "affected_tables": self._extract_table_references(sql_command),
                        "has_placeholders": "?" in sql_command,
                        "component_type": "OLE DB Command",
                        "component_name": component_name
                    }
                    operation_node.properties["sql_transformation"] = sql_info
                    
                    logger.debug(
                        f"Added OLE DB Command SQL to {task_id}: {sql_command[:100]}..."
                    )
                    
                    # Parse expression for dependencies
                    self._parse_expression_dependencies(
                        sql_command, task_id, edges, param_var_id_map
                    )

    def _parse_script_task(
        self,
        object_data_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse Microsoft.ScriptTask components to extract script code and logic.
        This handles VB.NET and C# code embedded in Script Tasks.
        """
        # Script tasks can have different XML structures: ScriptTaskData or ScriptProject
        script_task_data_xml = None
        
        # Try direct element names first
        script_task_data_xml = object_data_xml.find("ScriptTaskData")
        if script_task_data_xml is None:
            script_task_data_xml = object_data_xml.find("ScriptProject")
        
        if script_task_data_xml is None:
            # Try scanning all children for script-related elements
            for child in object_data_xml:
                child_tag = child.tag.lower()
                if "scripttaskdata" in child_tag or "scriptproject" in child_tag:
                    script_task_data_xml = child
                    break
        
        if script_task_data_xml is None:
            logger.debug(f"No ScriptTaskData or ScriptProject found in Script Task {task_id}")
            return

        logger.debug(f"Parsing Script Task: {task_id}")

        # Extract script information
        script_info = {
            "task_name": task_id.split(":")[-1],
            "script_language": "VB.NET",  # Default, will be updated if found
            "script_code": "",
            "referenced_variables": [],
            "readonly_variables": [],
            "readwrite_variables": [],
            "referenced_assemblies": [],
            "imports": [],
            "has_custom_code": False,
            "script_project_name": "",
            "entry_point": "Main"
        }

        # Extract script language - handle both ScriptTaskData and ScriptProject formats
        script_language = (script_task_data_xml.get("ScriptLanguage") or 
                          script_task_data_xml.get("scriptLanguage") or
                          script_task_data_xml.get("Language"))
        if script_language:
            # Map language names
            language_map = {
                "VisualBasic": "VB.NET",
                "CSharp": "C#",
                "VB": "VB.NET",
                "CS": "C#"
            }
            script_info["script_language"] = language_map.get(script_language, script_language)
            logger.debug(f"Script language: {script_info['script_language']}")

        # Extract ReadOnlyVariables
        readonly_vars = script_task_data_xml.get("ReadOnlyVariables") or script_task_data_xml.get("readOnlyVariables")
        if readonly_vars:
            script_info["readonly_variables"] = [var.strip() for var in readonly_vars.split(",")]
            logger.debug(f"ReadOnly variables: {script_info['readonly_variables']}")

        # Extract ReadWriteVariables
        readwrite_vars = script_task_data_xml.get("ReadWriteVariables") or script_task_data_xml.get("readWriteVariables")
        if readwrite_vars:
            script_info["readwrite_variables"] = [var.strip() for var in readwrite_vars.split(",")]
            logger.debug(f"ReadWrite variables: {script_info['readwrite_variables']}")

        # Extract EntryPoint
        entry_point = script_task_data_xml.get("EntryPoint") or script_task_data_xml.get("entryPoint")
        if entry_point:
            script_info["entry_point"] = entry_point

        # Extract ScriptProjectName - handle both formats
        script_project = (script_task_data_xml.get("ScriptProjectName") or 
                         script_task_data_xml.get("scriptProjectName") or
                         script_task_data_xml.get("Name"))
        if script_project:
            script_info["script_project_name"] = script_project

        # Extract VSTA version information if available
        vsta_major = script_task_data_xml.get("VSTAMajorVersion")
        vsta_minor = script_task_data_xml.get("VSTAMinorVersion")
        if vsta_major and vsta_minor:
            script_info["vsta_version"] = f"{vsta_major}.{vsta_minor}"
            logger.debug(f"VSTA Version: {script_info['vsta_version']}")

        # Enhanced script code extraction with comprehensive location scanning
        script_info["script_code"] = self._extract_script_source_code(script_task_data_xml, task_id)
        if script_info["script_code"]:
            script_info["has_custom_code"] = True
            logger.debug(f"Extracted {len(script_info['script_code'])} characters of script code")
            
            # Analyze script dependencies and complexity
            script_analysis = self._analyze_script_content(script_info["script_code"], script_info["script_language"])
            script_info.update(script_analysis)

        # Extract referenced assemblies from script project
        assemblies_elem = script_task_data_xml.find(".//References") or script_task_data_xml.find(".//AssemblyReferences")
        if assemblies_elem is not None:
            for assembly_elem in assemblies_elem.findall(".//Reference"):
                assembly_name = assembly_elem.get("AssemblyName") or assembly_elem.text
                if assembly_name:
                    script_info["referenced_assemblies"].append(assembly_name)

        # Combine all variable references for dependency tracking
        all_variables = script_info["readonly_variables"] + script_info["readwrite_variables"]
        script_info["referenced_variables"] = all_variables

        # Create dependency edges for referenced variables
        for var_name in all_variables:
            if var_name and var_name.strip():
                # Clean variable name (remove User:: prefix if present)
                clean_var_name = var_name.replace("User::", "").strip()
                var_id = f"variable:{clean_var_name}"
                
                # Check if variable exists in our mapping
                if var_id in param_var_id_map.values():
                    edges.append(
                        Edge(
                            source_id=task_id,
                            target_id=var_id,
                            relation=EdgeType.USES_VARIABLE,
                        )
                    )
                    logger.debug(f"Created variable dependency: {task_id} -> {var_id}")

        # Parse any expressions in the script code for additional dependencies
        if script_info["script_code"]:
            self._parse_expression_dependencies(
                script_info["script_code"], task_id, edges, param_var_id_map
            )

        # Store script information in the operation node properties
        operation_node = next((n for n in nodes if n.node_id == task_id), None)
        if operation_node:
            operation_node.properties["custom_script"] = script_info
            
            # Also store a summary for quick access
            operation_node.properties["script_summary"] = {
                "language": script_info["script_language"],
                "has_code": script_info["has_custom_code"],
                "variable_count": len(script_info["referenced_variables"]),
                "assembly_count": len(script_info["referenced_assemblies"]),
                "code_length": len(script_info["script_code"]) if script_info["script_code"] else 0
            }

            logger.debug(
                f"Added Script Task logic to {task_id}: {script_info['script_language']} "
                f"with {len(script_info['referenced_variables'])} variables, "
                f"{len(script_info['script_code'])} chars of code"
            )

    def _extract_error_handling_config(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node]
    ):
        """
        Extract error handling configuration from SSIS components.
        This captures error output redirections, failure handling logic, and retry policies.
        """
        error_config = {
            "has_error_output": False,
            "error_outputs": [],
            "input_error_configs": [],
            "output_error_configs": []
        }
        
        # Check for error outputs
        outputs_tag = component_xml.find("outputs")
        if outputs_tag is not None:
            for output in outputs_tag.findall("output"):
                if output.get("isErrorOut") == "true":
                    error_config["has_error_output"] = True
                    error_output = {
                        "name": output.get("name", ""),
                        "description": output.get("description", ""),
                        "ref_id": output.get("refId", "")
                    }
                    error_config["error_outputs"].append(error_output)
        
        # Check for input error handling configurations
        inputs_tag = component_xml.find("inputs")
        if inputs_tag is not None:
            for input_elem in inputs_tag.findall("input"):
                error_disposition = input_elem.get("errorRowDisposition")
                truncation_disposition = input_elem.get("truncationRowDisposition")
                
                if error_disposition or truncation_disposition:
                    input_error_config = {
                        "input_name": input_elem.get("name", ""),
                        "error_row_disposition": error_disposition,
                        "truncation_row_disposition": truncation_disposition,
                        "error_operation": input_elem.get("errorOrTruncationOperation", "")
                    }
                    error_config["input_error_configs"].append(input_error_config)
        
        # Add error configuration to operation node if any error handling is found
        if (error_config["has_error_output"] or 
            error_config["input_error_configs"] or 
            error_config["output_error_configs"]):
            
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                operation_node.properties["error_handling"] = error_config
                logger.debug(
                    f"Added error handling config to {task_id}: "
                    f"{len(error_config['error_outputs'])} error outputs, "
                    f"{len(error_config['input_error_configs'])} input configs"
                )

    def _parse_precedence_constraints(
        self, root: etree._Element, pipeline_id: str, edges: List[Edge]
    ):
        """
        Parse precedence constraints to capture control flow between tasks.
        Creates PRECEDES edges between operation nodes.
        """
        precedence_constraints = root.findall(
            ".//DTS:PrecedenceConstraint", self.ns_map
        )

        logger.debug(f"Found {len(precedence_constraints)} precedence constraints")

        # Get list of existing task IDs from current edges to validate references
        existing_task_ids = set()
        for edge in edges:
            if edge.relation == EdgeType.CONTAINS and edge.source_id == pipeline_id:
                existing_task_ids.add(edge.target_id)

        for constraint in precedence_constraints:
            from_attr = constraint.get(f"{{{self.ns_map['DTS']}}}From")
            to_attr = constraint.get(f"{{{self.ns_map['DTS']}}}To")

            logger.debug(f"Constraint: From='{from_attr}', To='{to_attr}'")

            if not from_attr or not to_attr:
                continue

            # Extract task names from the format "Package\TaskName"
            from_task = self._extract_task_name_from_ref(from_attr)
            to_task = self._extract_task_name_from_ref(to_attr)

            logger.debug(f"Extracted tasks: From='{from_task}', To='{to_task}'")

            if from_task and to_task:
                from_task_id = f"{pipeline_id}:operation:{from_task}"
                to_task_id = f"{pipeline_id}:operation:{to_task}"

                logger.debug(f"Task IDs: From='{from_task_id}', To='{to_task_id}'")

                # Only create edge if both tasks exist
                if (
                    from_task_id in existing_task_ids
                    and to_task_id in existing_task_ids
                ):
                    edges.append(
                        Edge(
                            source_id=from_task_id,
                            target_id=to_task_id,
                            relation=EdgeType.PRECEDES,
                        )
                    )
                    logger.debug(
                        f"Created PRECEDES edge: {from_task_id} -> {to_task_id}"
                    )
                else:
                    logger.debug(
                        f"Skipping constraint - tasks not found in graph: {from_task_id} or {to_task_id}"
                    )

    def _extract_task_name_from_ref(self, task_ref: str) -> str:
        """
        Extract task name from SSIS task reference format.
        Handles formats like "Package\TaskName" or just "TaskName"
        """
        if "\\" in task_ref:
            return task_ref.split("\\")[-1]
        return task_ref

    def _parse_package_parameters(
        self, root: etree._Element, file_path: str
    ) -> Tuple[List[Node], Dict[str, str]]:
        r"""
        Parses package parameters from DTS:PackageParameters section.
        Returns parameter nodes and a mapping from parameter GUIDs to node IDs.
        """
        parameter_nodes: List[Node] = []
        id_map: Dict[str, str] = {}

        # Find the PackageParameters container
        parameters_container = root.find("DTS:PackageParameters", self.ns_map)
        if parameters_container is None:
            return [], {}

        for param_xml in parameters_container.findall(
            "DTS:PackageParameter", self.ns_map
        ):
            param_name = param_xml.get(f"{{{self.ns_map['DTS']}}}ObjectName")
            param_guid = param_xml.get(f"{{{self.ns_map['DTS']}}}DTSID")
            param_data_type = param_xml.get(f"{{{self.ns_map['DTS']}}}DataType")
            param_required = param_xml.get(f"{{{self.ns_map['DTS']}}}Required", "False")

            if not param_name or not param_guid:
                continue

            # Extract parameter value
            param_value = ""
            param_value_elem = param_xml.find(
                "DTS:Property[@DTS:Name='ParameterValue']", self.ns_map
            )
            if param_value_elem is not None and param_value_elem.text:
                param_value = param_value_elem.text.strip('"')

            param_id = f"parameter:{param_name}"

            # Build properties with enhanced traceability
            properties = {
                "technology": "SSIS",
                "guid": param_guid.strip("{}"),
                "data_type": param_data_type or "unknown",
                "required": param_required.lower() == "true",
                "value": param_value,
                "scope": "package",
                **SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="dtsx",
                    xml_path=f"//DTS:PackageParameter[@DTS:ObjectName='{param_name}']"
                )
            }

            parameter_nodes.append(
                Node(
                    node_id=param_id,
                    node_type=NodeType.PARAMETER,
                    name=param_name,
                    properties=properties,
                )
            )

            # Map GUID to parameter node ID for reference tracking
            id_map[param_guid.strip("{}")] = param_id

            logger.debug(
                f"Parsed package parameter: {param_name} (Type: {param_data_type}, Required: {param_required})"
            )

        return parameter_nodes, id_map

    def _parse_package_variables(
        self, root: etree._Element, file_path: str
    ) -> Tuple[List[Node], Dict[str, str]]:
        """
        Parses package variables from DTS:Variables section.
        Returns variable nodes and a mapping from variable GUIDs to node IDs.
        """
        variable_nodes: List[Node] = []
        id_map: Dict[str, str] = {}

        # Find the Variables container
        variables_container = root.find("DTS:Variables", self.ns_map)
        if variables_container is None:
            return [], {}

        for var_xml in variables_container.findall("DTS:Variable", self.ns_map):
            var_name = var_xml.get(f"{{{self.ns_map['DTS']}}}ObjectName")
            var_guid = var_xml.get(f"{{{self.ns_map['DTS']}}}DTSID")
            var_namespace = var_xml.get(f"{{{self.ns_map['DTS']}}}Namespace", "User")

            if not var_name or not var_guid:
                continue

            # Extract variable value from DTS:VariableValue element
            var_value = ""
            var_value_elem = var_xml.find("DTS:VariableValue", self.ns_map)
            if var_value_elem is not None and var_value_elem.text:
                var_value = var_value_elem.text

            # Extract data type from VariableValue element
            var_data_type = "unknown"
            if var_value_elem is not None:
                var_data_type = var_value_elem.get(
                    f"{{{self.ns_map['DTS']}}}DataType", "unknown"
                )

            var_id = f"variable:{var_namespace}.{var_name}"

            # Build properties with enhanced traceability
            properties = {
                "technology": "SSIS",
                "guid": var_guid.strip("{}"),
                "data_type": var_data_type,
                "value": var_value,
                "namespace": var_namespace,
                "scope": "package",
                **SourceContext.create_node_traceability(
                    source_file_path=file_path,
                    source_file_type="dtsx",
                    xml_path=f"//DTS:Variable[@DTS:ObjectName='{var_name}']"
                )
            }

            variable_nodes.append(
                Node(
                    node_id=var_id,
                    node_type=NodeType.VARIABLE,
                    name=f"{var_namespace}.{var_name}",
                    properties=properties,
                )
            )

            # Map GUID to variable node ID for reference tracking
            id_map[var_guid.strip("{}")] = var_id

            logger.debug(
                f"Parsed package variable: {var_namespace}.{var_name} (Type: {var_data_type}, Value: {var_value[:50]}...)"
            )

        return variable_nodes, id_map

    def _parse_lookup_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        param_var_id_map: Dict[str, str],
    ):
        """
        Parse Microsoft.Lookup transformation components to extract
        join conditions and reference table mappings.
        """
        component_name = component_xml.get("name", "")
        logger.debug(f"Parsing lookup component: {component_name}")

        properties_tag = component_xml.find("properties")
        if properties_tag is None:
            return

        # Extract SQL commands
        sql_command_prop = properties_tag.find("property[@name='SqlCommand']")
        sql_command_param_prop = properties_tag.find(
            "property[@name='SqlCommandParam']"
        )
        parameter_map_prop = properties_tag.find("property[@name='ParameterMap']")
        no_match_behavior_prop = properties_tag.find(
            "property[@name='NoMatchBehavior']"
        )

        lookup_info = {
            "lookup_name": component_name,
            "sql_command": sql_command_prop.text
            if sql_command_prop is not None
            else "",
            "sql_command_param": sql_command_param_prop.text
            if sql_command_param_prop is not None
            else "",
            "parameter_map": parameter_map_prop.text
            if parameter_map_prop is not None
            else "",
            "no_match_behavior": int(no_match_behavior_prop.text)
            if no_match_behavior_prop is not None
            else 0,
            "join_conditions": [],
            "output_columns": [],
        }

        # Extract join conditions from input columns
        inputs_tag = component_xml.find("inputs")
        if inputs_tag is not None:
            for input_elem in inputs_tag.findall("input"):
                input_columns_tag = input_elem.find("inputColumns")
                if input_columns_tag is not None:
                    for input_column in input_columns_tag.findall("inputColumn"):
                        column_name = input_column.get("cachedName", "")
                        column_properties = input_column.find("properties")

                        if column_properties is not None:
                            join_to_ref_prop = column_properties.find(
                                "property[@name='JoinToReferenceColumn']"
                            )
                            if join_to_ref_prop is not None and join_to_ref_prop.text:
                                # Enrich join condition with type mapping
                                ssis_type = input_column.get("cachedDataType", "")
                                length = input_column.get("cachedLength", "")
                                type_properties = {}
                                
                                if self.enable_type_mapping and self.type_mapper and ssis_type:
                                    type_properties = self.type_mapper.enrich_column_properties(
                                        ssis_type=ssis_type,
                                        length=length,
                                        target_platforms=self.target_platforms
                                    )

                                join_condition = {
                                    "input_column": column_name,
                                    "reference_column": join_to_ref_prop.text,
                                    "data_type": ssis_type,
                                    "length": length,
                                    **type_properties  # Add type mapping properties
                                }
                                lookup_info["join_conditions"].append(join_condition)

                                logger.debug(
                                    f"Found lookup join: {column_name} -> {join_to_ref_prop.text}"
                                )

        # Extract output columns
        outputs_tag = component_xml.find("outputs")
        if outputs_tag is not None:
            for output in outputs_tag.findall("output"):
                # Skip error outputs and no-match outputs for now
                if output.get(
                    "isErrorOut"
                ) == "true" or "sans correspondance" in output.get("name", ""):
                    continue

                output_columns_tag = output.find("outputColumns")
                if output_columns_tag is not None:
                    for output_column in output_columns_tag.findall("outputColumn"):
                        column_name = output_column.get("name", "")
                        column_properties = output_column.find("properties")

                        if column_properties is not None:
                            copy_from_ref_prop = column_properties.find(
                                "property[@name='CopyFromReferenceColumn']"
                            )
                            if (
                                copy_from_ref_prop is not None
                                and copy_from_ref_prop.text
                            ):
                                # Enrich output column with type mapping
                                ssis_type = output_column.get("dataType", "")
                                type_properties = {}
                                
                                if self.enable_type_mapping and self.type_mapper and ssis_type:
                                    type_properties = self.type_mapper.enrich_column_properties(
                                        ssis_type=ssis_type,
                                        target_platforms=self.target_platforms
                                    )

                                output_col = {
                                    "output_column": column_name,
                                    "reference_column": copy_from_ref_prop.text,
                                    "data_type": ssis_type,
                                    **type_properties  # Add type mapping properties
                                }
                                lookup_info["output_columns"].append(output_col)

                                logger.debug(
                                    f"Found lookup output: {copy_from_ref_prop.text} -> {column_name}"
                                )

        # Extract reference table name from SQL command
        if lookup_info["sql_command"]:
            import re

            # Try to extract table name from "select * from [dbo].[TableName]"
            table_match = re.search(
                r"from\s+\[?([^\[\]\s]+)\]?\.\[?([^\[\]\s]+)\]?",
                lookup_info["sql_command"],
                re.IGNORECASE,
            )
            if table_match:
                lookup_info["reference_schema"] = table_match.group(1)
                lookup_info["reference_table"] = table_match.group(2)
            else:
                # Fallback: try simpler pattern
                table_match = re.search(
                    r"from\s+\[?([^\[\]\s]+)\]?",
                    lookup_info["sql_command"],
                    re.IGNORECASE,
                )
                if table_match:
                    lookup_info["reference_table"] = table_match.group(1)

        # Store lookup info in the operation node properties
        if lookup_info["join_conditions"] or lookup_info["output_columns"]:
            # Find the operation node for this component
            operation_node = next((n for n in nodes if n.node_id == task_id), None)
            if operation_node:
                if "lookups" not in operation_node.properties:
                    operation_node.properties["lookups"] = []
                operation_node.properties["lookups"].append(lookup_info)

                logger.debug(
                    f"Added lookup with {len(lookup_info['join_conditions'])} join conditions "
                    f"and {len(lookup_info['output_columns'])} output columns to {task_id}"
                )

    def _extract_column_lineage(
        self,
        component_xml: etree._Element,
        component_name: str,
        task_id: str,
    ) -> Dict[str, Any]:
        """
        Extracts column-level lineage mappings from SSIS components.

        Args:
            component_xml: The component XML element
            component_name: Name of the component
            task_id: Parent task ID

        Returns:
            Dictionary containing column lineage information
        """
        column_lineage = {
            "component_name": component_name,
            "input_columns": [],
            "output_columns": [],
            "column_mappings": [],
        }

        # Extract input columns with their lineage IDs
        inputs_xml = component_xml.find("inputs")
        if inputs_xml is not None:
            for input_xml in inputs_xml.findall("input"):
                input_name = input_xml.get("name", "")
                input_columns_xml = input_xml.find("inputColumns")

                if input_columns_xml is not None:
                    for input_col_xml in input_columns_xml.findall("inputColumn"):
                        col_name = input_col_xml.get("cachedName") or input_col_xml.get(
                            "name"
                        )
                        lineage_id = input_col_xml.get("lineageId")
                        data_type = input_col_xml.get("cachedDataType")
                        length = input_col_xml.get("cachedLength")

                        # Extract output lineage ID from properties
                        output_lineage_id = None
                        properties_xml = input_col_xml.find("properties")
                        if properties_xml is not None:
                            for prop_xml in properties_xml.findall("property"):
                                if prop_xml.get("name") == "OutputColumnLineageID":
                                    output_lineage_id = prop_xml.text
                                    # Remove the #{} wrapper if present
                                    if (
                                        output_lineage_id
                                        and output_lineage_id.startswith("#{")
                                        and output_lineage_id.endswith("}")
                                    ):
                                        output_lineage_id = output_lineage_id[2:-1]
                                    break

                        # Enrich input column with type mapping
                        type_properties = {}
                        if self.enable_type_mapping and self.type_mapper and data_type:
                            type_properties = self.type_mapper.enrich_column_properties(
                                ssis_type=data_type,
                                length=length,
                                target_platforms=self.target_platforms
                            )

                        input_column = {
                            "column_name": col_name,
                            "input_name": input_name,
                            "lineage_id": lineage_id,
                            "output_lineage_id": output_lineage_id,
                            "data_type": data_type,
                            "length": length,
                            **type_properties  # Add type mapping properties
                        }
                        column_lineage["input_columns"].append(input_column)

                        # Create column mapping if we have both input and output lineage
                        if lineage_id and output_lineage_id:
                            column_lineage["column_mappings"].append(
                                {
                                    "source_column": col_name,
                                    "source_lineage_id": lineage_id,
                                    "target_lineage_id": output_lineage_id,
                                    "transformation_type": "pass_through",
                                }
                            )

        # Extract output columns
        outputs_xml = component_xml.find("outputs")
        if outputs_xml is not None:
            for output_xml in outputs_xml.findall("output"):
                output_name = output_xml.get("name", "")
                output_columns_xml = output_xml.find("outputColumns")

                if output_columns_xml is not None:
                    for output_col_xml in output_columns_xml.findall("outputColumn"):
                        col_name = output_col_xml.get("name")
                        lineage_id = output_col_xml.get("lineageId")
                        data_type = output_col_xml.get("dataType")
                        length = output_col_xml.get("length")

                        # Check if this is a derived column with expression
                        expression = None
                        properties_xml = output_col_xml.find("properties")
                        if properties_xml is not None:
                            for prop_xml in properties_xml.findall("property"):
                                if prop_xml.get("name") == "Expression":
                                    expression = prop_xml.text
                                    break

                        # Enrich output column with type mapping
                        type_properties = {}
                        if self.enable_type_mapping and self.type_mapper and data_type:
                            type_properties = self.type_mapper.enrich_column_properties(
                                ssis_type=data_type,
                                length=length,
                                target_platforms=self.target_platforms
                            )

                        output_column = {
                            "column_name": col_name,
                            "output_name": output_name,
                            "lineage_id": lineage_id,
                            "data_type": data_type,
                            "length": length,
                            "expression": expression,
                            **type_properties  # Add type mapping properties
                        }
                        column_lineage["output_columns"].append(output_column)

                        # If there's an expression, this is a transformation
                        if expression:
                            column_lineage["column_mappings"].append(
                                {
                                    "target_column": col_name,
                                    "target_lineage_id": lineage_id,
                                    "expression": expression,
                                    "transformation_type": "derived_column",
                                }
                            )

        return column_lineage

    def _resolve_expression_with_parameters(self, expression: str, param_var_id_map: Dict[str, str]) -> Dict[str, Any]:
        """
        Enhanced expression resolution that tracks parameter usage and provides resolved values.
        
        Args:
            expression: The raw SSIS expression
            param_var_id_map: Mapping of parameter/variable names to their IDs
            
        Returns:
            Dictionary containing expression analysis and resolved values
        """
        if not expression:
            return {
                "raw_expression": expression,
                "uses_parameters": [],
                "uses_variables": [],
                "resolved_expression": expression,
                "is_parameterized": False
            }
            
        import re
        
        result = {
            "raw_expression": expression,
            "uses_parameters": [],
            "uses_variables": [],
            "resolved_expression": expression,
            "is_parameterized": False
        }
        
        # Enhanced parameter pattern matching
        param_patterns = [
            r"\$Project::([\w\d_]+)",  # $Project::ParameterName
            r"\$Package::([\w\d_]+)",  # $Package::ParameterName
            r"@\[\$Project::([^\]]+)\]",  # @[$Project::ParameterName]
            r"@\[\$Package::([^\]]+)\]"   # @[$Package::ParameterName]
        ]
        
        for pattern in param_patterns:
            matches = re.findall(pattern, expression, re.IGNORECASE)
            for match in matches:
                param_name = match.strip()
                if param_name not in result["uses_parameters"]:
                    result["uses_parameters"].append(param_name)
                    result["is_parameterized"] = True
                    
                    # Try to resolve parameter value from context
                    param_key = f"parameter:{param_name}"
                    if param_key in param_var_id_map:
                        param_value = self._get_parameter_value(param_name)
                        if param_value:
                            # Replace parameter reference with actual value in resolved expression
                            param_ref_patterns = [
                                f"\\$Project::{param_name}",
                                f"\\$Package::{param_name}",
                                f"@[\\$Project::{param_name}]",
                                f"@[\\$Package::{param_name}]"
                            ]
                            for ref_pattern in param_ref_patterns:
                                result["resolved_expression"] = result["resolved_expression"].replace(
                                    ref_pattern, str(param_value)
                                )
        
        # Enhanced variable pattern matching
        variable_patterns = [
            r"@\[User::([^\]]+)\]",     # @[User::VariableName]
            r"@\[System::([^\]]+)\]",   # @[System::VariableName]
            r"@([\w\d_]+)"              # @VariableName (simple form)
        ]
        
        for pattern in variable_patterns:
            matches = re.findall(pattern, expression, re.IGNORECASE)
            for match in matches:
                var_name = match.strip()
                if var_name not in result["uses_variables"]:
                    result["uses_variables"].append(var_name)
                    result["is_parameterized"] = True
                    
                    # Try to resolve variable value from context
                    var_key = f"variable:{var_name}"
                    if var_key in param_var_id_map:
                        var_value = self._get_variable_value(var_name)
                        if var_value:
                            # Replace variable reference with actual value
                            var_ref_patterns = [
                                f"@[User::{var_name}]",
                                f"@[System::{var_name}]",
                                f"@{var_name}"
                            ]
                            for ref_pattern in var_ref_patterns:
                                result["resolved_expression"] = result["resolved_expression"].replace(
                                    ref_pattern, str(var_value)
                                )
        
        return result
    
    def _get_parameter_value(self, param_name: str) -> Optional[str]:
        """
        Retrieve parameter value from parameters context.
        """
        for param_info in self.parameters_context.values():
            if param_info.get("name") == param_name:
                return param_info.get("value")
        return None
    
    def _get_variable_value(self, var_name: str) -> Optional[str]:
        """
        Retrieve variable value from the parsed variables.
        """
        # This would need to be implemented based on how variables are stored
        # For now, return None as we'd need access to the variable context
        return None
    
    def _introspect_table_schema(self, connection_string: str, table_name: str) -> Dict[str, Any]:
        """
        Introspect database table schema to get detailed metadata.
        
        Args:
            connection_string: Database connection string
            table_name: Name of the table to introspect
            
        Returns:
            Dictionary containing detailed schema information
        """
        # Create cache key
        cache_key = f"{connection_string}::{table_name}"
        if cache_key in self.schema_cache:
            return self.schema_cache[cache_key]
            
        schema_info = {
            "table_name": table_name,
            "schema_introspected": False,
            "columns": [],
            "indexes": [],
            "constraints": [],
            "table_type": "TABLE",
            "row_count_estimate": None,
            "introspection_error": None
        }
        
        if not self.enable_schema_introspection:
            schema_info["introspection_error"] = "Schema introspection disabled"
            self.schema_cache[cache_key] = schema_info
            return schema_info
        
        try:
            # For now, we'll skip actual database connection due to environment limitations
            # In a real implementation, this would connect to the database and introspect
            schema_info["introspection_error"] = "Database introspection not implemented in this demo"
            logger.debug(f"Schema introspection requested for {table_name} but not implemented")
                
        except Exception as e:
            schema_info["introspection_error"] = f"Schema introspection error: {str(e)}"
            logger.debug(f"Unexpected error during schema introspection: {e}")
        
        # Cache the result
        self.schema_cache[cache_key] = schema_info
        return schema_info
    
    def _parse_connection_string(self, connection_string: str) -> Optional[Dict[str, str]]:
        """
        Parse SQL Server connection string into components.
        """
        if not connection_string:
            return None
            
        parts = {}
        for part in connection_string.split(";"):
            if "=" in part:
                key, value = part.split("=", 1)
                parts[key.strip().lower()] = value.strip()
        
        return parts if parts else None

    def _parse_generic_component(
        self,
        component_xml: etree._Element,
        task_id: str,
        nodes: List[Node],
        edges: List[Edge],
        column_lineage: Dict[str, Any],
    ):
        """
        Parse generic SSIS components that don't have specialized parsers.
        This ensures we still capture column lineage for all component types.
        """
        component_name = component_xml.get("name", "")
        class_id = component_xml.get("componentClassID", "")

        logger.debug(f"Parsing generic component: {component_name} (type: {class_id})")

        # Log column lineage information for debugging
        if column_lineage["column_mappings"]:
            logger.debug(
                f"Generic component {component_name} has {len(column_lineage['column_mappings'])} column mappings"
            )

    def _extract_script_source_code(self, script_task_data_xml: etree._Element, task_id: str) -> str:
        """
        Enhanced script code extraction with comprehensive location and namespace scanning.
        Handles multiple SSIS script storage formats including SQLTask namespace variants.
        """
        script_code = ""
        
        # Strategy 1: Look for SQLTask namespace elements (reviewer's format)
        try:
            # Check for SQLTask:ScriptCode elements
            sqltask_script_code = script_task_data_xml.find("SQLTask:ScriptCode", self.ns_map)
            if sqltask_script_code is not None and sqltask_script_code.text:
                script_code = sqltask_script_code.text.strip()
                logger.debug(f"Found script code in SQLTask:ScriptCode: {len(script_code)} chars")
                return script_code
                
            # Check for nested SQLTask:ScriptProject/SQLTask:ScriptCode
            sqltask_project = script_task_data_xml.find("SQLTask:ScriptProject", self.ns_map)
            if sqltask_project is not None:
                sqltask_script_code = sqltask_project.find("SQLTask:ScriptCode", self.ns_map)
                if sqltask_script_code is not None and sqltask_script_code.text:
                    script_code = sqltask_script_code.text.strip()
                    logger.debug(f"Found script code in SQLTask:ScriptProject/ScriptCode: {len(script_code)} chars")
                    return script_code
        except Exception as e:
            logger.debug(f"SQLTask namespace search failed: {e}")
        
        # Strategy 2: Look for standard script storage locations
        script_code_locations = [
            # Direct code elements
            "ScriptCode",
            "SourceCode", 
            "Code",
            # Storage containers
            "VSTAScriptProjectStorage",
            "ScriptStorage", 
            "BinaryCodeStorage",
            "ProjectStorage",
            # Nested paths
            ".//ScriptCode",
            ".//SourceCode",
            ".//Code"
        ]
        
        for location in script_code_locations:
            try:
                script_storage = script_task_data_xml.find(location)
                if script_storage is not None:
                    # Try to extract direct text content
                    if script_storage.text and script_storage.text.strip():
                        script_code = script_storage.text.strip()
                        logger.debug(f"Found script code in {location}: {len(script_code)} chars")
                        return script_code
                    
                    # Look for nested elements that might contain code
                    for child in script_storage:
                        if child.text and len(child.text.strip()) > 20:  # Likely code content
                            script_code = child.text.strip()
                            logger.debug(f"Found script code in {location}/{child.tag}: {len(script_code)} chars")
                            return script_code
            except Exception as e:
                logger.debug(f"Script location {location} search failed: {e}")
        
        # Strategy 3: Look for base64 encoded or CDATA script content
        try:
            # Check for CDATA sections that might contain encoded script
            for element in script_task_data_xml.iter():
                if element.text and ("Sub Main" in element.text or "void Main" in element.text or 
                                   "Public Sub" in element.text or "public void" in element.text):
                    script_code = element.text.strip()
                    logger.debug(f"Found script code in CDATA/text content: {len(script_code)} chars")
                    return script_code
        except Exception as e:
            logger.debug(f"CDATA search failed: {e}")
        
        # Strategy 4: Look for any text content that looks like code
        try:
            all_text = etree.tostring(script_task_data_xml, encoding='unicode', method='text')
            if all_text:
                # Look for common code patterns
                code_patterns = [
                    r'Public Sub Main\(\).*?End Sub',
                    r'public void Main\(\).*?\}',
                    r'Sub Main\(\).*?End Sub',
                    r'void Main\(\).*?\}'
                ]
                
                for pattern in code_patterns:
                    matches = re.findall(pattern, all_text, re.DOTALL | re.IGNORECASE)
                    if matches:
                        script_code = matches[0].strip()
                        logger.debug(f"Found script code via pattern matching: {len(script_code)} chars")
                        return script_code
        except Exception as e:
            logger.debug(f"Pattern matching search failed: {e}")
        
        if not script_code:
            logger.debug(f"No script code found in Script Task {task_id}")
        
        return script_code

    def _analyze_script_content(self, script_code: str, language: str) -> Dict[str, Any]:
        """
        Analyze script content to extract dependencies, complexity, and business logic patterns.
        """
        analysis = {
            "script_dependencies": [],
            "entry_points": [],
            "code_complexity": "unknown",
            "has_error_handling": False,
            "uses_database": False,
            "uses_file_system": False,
            "uses_web_services": False,
            "custom_functions": [],
            "exception_types": [],
            "code_lines": 0,
            "comment_lines": 0
        }
        
        if not script_code:
            return analysis
        
        lines = script_code.split('\n')
        analysis["code_lines"] = len(lines)
        
        # Language-specific analysis
        if language.upper() in ["VB.NET", "VB", "VISUALBASIC"]:
            analysis.update(self._analyze_vb_script(script_code, lines))
        elif language.upper() in ["C#", "CSHARP"]:
            analysis.update(self._analyze_csharp_script(script_code, lines))
        
        # Common dependency patterns across languages
        dependency_patterns = {
            "Dts.Variables": "SSIS Variables",
            "Dts.Connections": "SSIS Connections", 
            "Dts.Events": "SSIS Events",
            "Dts.Log": "SSIS Logging",
            "Dts.TaskResult": "Task Result",
            "System.Data": "Database Operations",
            "System.IO": "File System Operations",
            "System.Net": "Network Operations",
            "System.Web": "Web Services",
            "SqlConnection": "SQL Server Connection",
            "OleDbConnection": "OLE DB Connection",
            "FileStream": "File Operations",
            "HttpClient": "HTTP Client"
        }
        
        for pattern, description in dependency_patterns.items():
            if pattern in script_code:
                analysis["script_dependencies"].append({
                    "pattern": pattern,
                    "description": description,
                    "type": "framework_dependency"
                })
        
        # Complexity analysis
        complexity_indicators = {
            "low": ["ScriptResults.Success", "ScriptResults.Failure"],
            "medium": ["Try", "Catch", "For", "While", "If"],
            "high": ["Class", "Function", "Sub", "Method", "Threading", "Async"]
        }
        
        complexity_score = 0
        for level, indicators in complexity_indicators.items():
            for indicator in indicators:
                if indicator in script_code:
                    if level == "low":
                        complexity_score += 1
                    elif level == "medium":
                        complexity_score += 3
                    elif level == "high":
                        complexity_score += 5
        
        if complexity_score <= 3:
            analysis["code_complexity"] = "low"
        elif complexity_score <= 10:
            analysis["code_complexity"] = "medium"
        else:
            analysis["code_complexity"] = "high"
        
        # Feature detection
        analysis["has_error_handling"] = any(pattern in script_code.upper() 
                                           for pattern in ["TRY", "CATCH", "ON ERROR", "ERROR HANDLING"])
        analysis["uses_database"] = any(pattern in script_code 
                                      for pattern in ["SqlConnection", "OleDbConnection", "System.Data"])
        analysis["uses_file_system"] = any(pattern in script_code 
                                         for pattern in ["System.IO", "FileStream", "File."])
        analysis["uses_web_services"] = any(pattern in script_code 
                                          for pattern in ["HttpClient", "WebRequest", "System.Net"])
        
        logger.debug(f"Script analysis complete: {analysis['code_complexity']} complexity, "
                    f"{len(analysis['script_dependencies'])} dependencies")
        
        return analysis

    def _analyze_vb_script(self, script_code: str, lines: List[str]) -> Dict[str, Any]:
        """VB.NET specific script analysis."""
        analysis = {}
        
        # Find entry points
        entry_points = []
        for line in lines:
            line_clean = line.strip()
            if (line_clean.startswith("Public Sub ") or line_clean.startswith("Sub ")) and "(" in line_clean:
                method_name = line_clean.split("Sub ")[1].split("(")[0].strip()
                entry_points.append(method_name)
        
        analysis["entry_points"] = entry_points
        
        # Count comment lines
        comment_lines = sum(1 for line in lines if line.strip().startswith("'"))
        analysis["comment_lines"] = comment_lines
        
        # Find custom functions
        custom_functions = []
        for line in lines:
            line_clean = line.strip()
            if (line_clean.startswith("Public Function ") or line_clean.startswith("Function ")) and "(" in line_clean:
                func_name = line_clean.split("Function ")[1].split("(")[0].strip()
                custom_functions.append(func_name)
        
        analysis["custom_functions"] = custom_functions
        
        return analysis

    def _analyze_csharp_script(self, script_code: str, lines: List[str]) -> Dict[str, Any]:
        """C# specific script analysis."""
        analysis = {}
        
        # Find entry points
        entry_points = []
        for line in lines:
            line_clean = line.strip()
            if ("public void " in line_clean or "void " in line_clean) and "(" in line_clean:
                parts = line_clean.split("void ")[1].split("(")
                if parts:
                    method_name = parts[0].strip()
                    entry_points.append(method_name)
        
        analysis["entry_points"] = entry_points
        
        # Count comment lines
        comment_lines = sum(1 for line in lines if line.strip().startswith("//"))
        analysis["comment_lines"] = comment_lines
        
        # Find custom functions/methods
        custom_functions = []
        for line in lines:
            line_clean = line.strip()
            if ("public " in line_clean and "(" in line_clean and 
                not "void Main(" in line_clean and "{" not in line_clean):
                # Extract method name
                parts = line_clean.split("(")[0].split()
                if len(parts) >= 2:
                    method_name = parts[-1].strip()
                    custom_functions.append(method_name)
        
        analysis["custom_functions"] = custom_functions
        
        return analysis
    
    def _parse_target_platforms(self, platform_names: List[str]) -> List[TargetPlatform]:
        """Parse target platform names to enum values."""
        platforms = []
        platform_mapping = {
            "sql_server": TargetPlatform.SQL_SERVER,
            "postgresql": TargetPlatform.POSTGRESQL,
            "mysql": TargetPlatform.MYSQL,
            "oracle": TargetPlatform.ORACLE,
            "snowflake": TargetPlatform.SNOWFLAKE,
            "bigquery": TargetPlatform.BIGQUERY,
            "azure_synapse": TargetPlatform.AZURE_SYNAPSE
        }
        
        for name in platform_names:
            platform = platform_mapping.get(name.lower())
            if platform:
                platforms.append(platform)
            else:
                logger.warning(f"Unknown target platform: {name}")
        
        return platforms or [TargetPlatform.SQL_SERVER]  # Default fallback
    
    def _detect_platform_from_connection(self, connection_data: Dict[str, Any]) -> Optional[TargetPlatform]:
        """
        Detect target platform from connection information.
        """
        provider = connection_data.get("provider", "").lower()
        creation_name = connection_data.get("creation_name", "").lower()
        connection_string = connection_data.get("connection_string", "").lower()
        
        # SQL Server detection
        if any(keyword in provider for keyword in ["sqloledb", "sqlncli", "msoledbsql"]) or \
           any(keyword in creation_name for keyword in ["oledb", "sql"]) or \
           any(keyword in connection_string for keyword in ["sqlserver", "sql server"]):
            return TargetPlatform.SQL_SERVER
            
        # PostgreSQL detection
        if any(keyword in provider for keyword in ["postgresql", "npgsql"]) or \
           any(keyword in connection_string for keyword in ["postgresql", "postgres"]):
            return TargetPlatform.POSTGRESQL
            
        # MySQL detection
        if any(keyword in provider for keyword in ["mysql"]) or \
           any(keyword in connection_string for keyword in ["mysql"]):
            return TargetPlatform.MYSQL
            
        # Oracle detection
        if any(keyword in provider for keyword in ["oracle", "oraoledb"]) or \
           any(keyword in connection_string for keyword in ["oracle"]):
            return TargetPlatform.ORACLE
            
        # Default to SQL Server if uncertain
        return TargetPlatform.SQL_SERVER
    
    def _extract_tables_from_sql(self, sql_text: str) -> List[str]:
        """
        Extract table names from SQL commands in SSIS components.
        
        This replaces the broken regex that only handled schema.table format.
        Now handles:
        - Simple tables: Products, Categories  
        - Bracketed tables: [Products], [Categories]
        - Schema-qualified: dbo.Products, [dbo].[Products]
        - Table aliases: Products AS p, Categories c
        - All JOIN types: INNER JOIN, LEFT JOIN, etc.
        
        Args:
            sql_text: SQL command text from SSIS component
            
        Returns:
            List of table names found in the SQL
        """
        if not sql_text or not isinstance(sql_text, str):
            return []
        
        tables = set()
        
        # Pattern 1: Handle [schema].[table] format
        bracketed_schema_pattern = r'(?:FROM|JOIN)\s+\[([^\]]+)\]\.\[([^\]]+)\](?:\s+(?:AS\s+)?\w+)?'
        matches = re.findall(bracketed_schema_pattern, sql_text, re.IGNORECASE)
        for schema, table in matches:
            tables.add(f"{schema}.{table}")
        
        # Pattern 2: Handle schema.table format (no brackets)
        plain_schema_pattern = r'(?:FROM|JOIN)\s+([^\s\[\]\.]+)\.([^\s\[\]\.]+)(?:\s+(?:AS\s+)?\w+)?'
        matches = re.findall(plain_schema_pattern, sql_text, re.IGNORECASE)
        for schema, table in matches:
            if not any(existing == f"{schema}.{table}" for existing in tables):
                tables.add(f"{schema}.{table}")
        
        # Pattern 3: Handle simple table names (only if not already captured above)
        simple_pattern = r'(?:FROM|JOIN)\s+\[?([^\s\[\]\.]+)\]?(?:\s+(?:AS\s+)?\w+)?'
        matches = re.findall(simple_pattern, sql_text, re.IGNORECASE)
        for table in matches:
            # Only add if this table isn't already part of a schema.table entry
            if not any(existing.endswith(f".{table}") for existing in tables):
                tables.add(table)
        
        return sorted(list(tables))
    
    def _get_platform_type_rules(self, platform: TargetPlatform) -> Dict[str, str]:
        """
        Get simplified type mapping rules for a platform.
        """
        if not self.type_mapper:
            return {}
            
        # Common SSIS types and their platform mappings
        common_ssis_types = [
            "DT_I4", "DT_I8", "DT_WSTR", "DT_STR", "DT_DBTIMESTAMP", 
            "DT_BOOL", "DT_R8", "DT_DECIMAL", "DT_GUID"
        ]
        
        rules = {}
        for ssis_type in common_ssis_types:
            canonical_type = self.type_mapper.get_canonical_type(ssis_type)
            platform_type = self.type_mapper.get_platform_type(canonical_type, platform)
            rules[ssis_type] = platform_type
            
        return rules
