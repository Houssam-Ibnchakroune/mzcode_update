"""
Cross-Package Dependency Analyzer

Analyzes the graph after ingestion to identify dependencies between packages,
shared resources, and execution order requirements.
"""

import logging
from typing import Dict, List, Set, Tuple, Any
from collections import defaultdict
import networkx as nx

from ..models.graph import Node, Edge
from ..models.canonical_types import NodeType, EdgeType
from ..graph.graph_client_interface import GraphClientInterface

logger = logging.getLogger(__name__)


class CrossPackageAnalyzer:
    """
    Analyzes cross-package dependencies in an SSIS project graph.
    
    Identifies:
    1. Shared resources (tables, connections, parameters)
    2. Data flow dependencies between packages
    3. Execution order requirements
    4. Resource contention risks
    """
    
    def __init__(self, graph_client: GraphClientInterface):
        self.graph_client = graph_client
        self.graph = self._get_networkx_graph()
        
    def analyze(self) -> Dict[str, Any]:
        """
        Performs comprehensive cross-package dependency analysis.
        
        Returns:
            Analysis results including shared resources, dependencies, and execution order
        """
        logger.info("Starting cross-package dependency analysis...")
        
        # Get all packages (pipelines)
        packages = self._get_all_packages()
        logger.info(f"Found {len(packages)} packages to analyze")
        
        # Analyze shared resources
        shared_tables = self._analyze_shared_tables(packages)
        shared_connections = self._analyze_shared_connections(packages)
        shared_parameters = self._analyze_shared_parameters(packages)
        
        # Analyze data flow dependencies
        data_dependencies = self._analyze_data_flow_dependencies(packages, shared_tables)
        
        # Determine execution order
        execution_order = self._determine_execution_order(packages, data_dependencies)
        
        # Identify resource contention risks
        contention_risks = self._analyze_resource_contention(shared_connections, shared_tables)
        
        # Create cross-package edges
        cross_package_edges = self._create_cross_package_edges(data_dependencies, shared_connections, shared_parameters)
        
        # Add cross-package edges to the graph
        self._add_cross_package_edges_to_graph(cross_package_edges)
        
        # Update package properties with dependency information
        self._update_package_properties(packages, data_dependencies, execution_order, shared_tables, shared_connections)
        
        analysis_results = {
            "packages_analyzed": len(packages),
            "shared_resources": {
                "tables": len(shared_tables),
                "connections": len(shared_connections), 
                "parameters": len(shared_parameters)
            },
            "data_dependencies": len(data_dependencies),
            "execution_chains": len(execution_order),
            "cross_package_edges_added": len(cross_package_edges),
            "contention_risks": contention_risks,
            "detailed_analysis": {
                "shared_tables": shared_tables,
                "shared_connections": shared_connections,
                "shared_parameters": shared_parameters,
                "data_dependencies": data_dependencies,
                "execution_order": execution_order
            }
        }
        
        logger.info(f"Cross-package analysis complete: {analysis_results['packages_analyzed']} packages, "
                   f"{analysis_results['shared_resources']['tables']} shared tables, "
                   f"{analysis_results['data_dependencies']} dependencies")
        
        return analysis_results
    
    def _get_all_packages(self) -> List[Dict[str, Any]]:
        """Get all pipeline (package) nodes from the graph."""
        packages = []
        
        # For Memgraph, we need to handle the case where cross-package analysis is limited
        raw_graph = self.graph_client.get_graph()
        if not isinstance(raw_graph, nx.DiGraph):
            # Limited support for Memgraph - return empty packages for now
            logger.warning("Limited cross-package analysis support for Memgraph backend")
            return packages
        
        # Use NetworkX approach for full support
        for node_id, node_data in raw_graph.nodes(data=True):
            if node_data.get('node_type') == 'pipeline':
                packages.append({
                    'id': node_id,
                    'name': node_data.get('name', node_id),
                    'properties': node_data.get('properties', {})
                })
        return packages
    
    def _get_networkx_graph(self) -> nx.DiGraph:
        """Get a NetworkX graph representation from the graph client."""
        # Check if it's already a NetworkX graph
        raw_graph = self.graph_client.get_graph()
        if isinstance(raw_graph, nx.DiGraph):
            return raw_graph
        
        # For Memgraph, create NetworkX graph from stored data
        logger.warning("Limited cross-package analysis support for Memgraph backend")
        
        # Create NetworkX graph from Memgraph data
        graph = nx.DiGraph()
        
        try:
            # Get all nodes
            all_nodes = self.graph_client.get_all_nodes()
            for node in all_nodes:
                node_dict = node.to_dict()
                graph.add_node(node_dict['node_id'], **node_dict)
            
            # Get all edges via Cypher query
            connection = self.graph_client.get_graph()
            cursor = connection.cursor()
            cursor.execute("""
                MATCH (source)-[r]->(target)
                RETURN source.id as source_id, target.id as target_id, 
                       type(r) as relation_type, properties(r) as properties
            """)
            edge_results = cursor.fetchall()
            
            for source_id, target_id, relation_type, properties in edge_results:
                edge_attrs = properties.copy() if properties else {}
                edge_attrs['relation'] = relation_type
                graph.add_edge(source_id, target_id, **edge_attrs)
                
        except Exception as e:
            logger.error(f"Failed to convert Memgraph data to NetworkX: {e}")
            return nx.DiGraph()
        
        return graph
    
    def _analyze_shared_tables(self, packages: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Identify tables that are used by multiple packages."""
        table_usage = defaultdict(lambda: {'readers': set(), 'writers': set(), 'packages': set()})
        
        for package in packages:
            package_id = package['id']
            
            # Find all operations in this package
            package_operations = []
            for source, target, edge_data in self.graph.edges(data=True):
                if (source == package_id and 
                    edge_data.get('relation') == 'contains' and 
                    self.graph.nodes[target].get('node_type') == 'operation'):
                    package_operations.append(target)
            
            # Analyze table usage for each operation
            for operation_id in package_operations:
                for source, target, edge_data in self.graph.edges(data=True):
                    if (source == operation_id and 
                        self.graph.nodes[target].get('node_type') == 'table'):
                        relation = edge_data.get('relation')
                        if relation == 'writes_to':
                            table_usage[target]['writers'].add(operation_id)
                            table_usage[target]['packages'].add(package_id)
                        elif relation == 'reads_from':
                            table_usage[target]['readers'].add(operation_id)
                            table_usage[target]['packages'].add(package_id)
        
        # Filter to only shared tables (used by multiple packages)
        shared_tables = {}
        for table_id, usage in table_usage.items():
            if len(usage['packages']) > 1:
                table_node = self.graph.nodes[table_id]
                shared_tables[table_id] = {
                    'table_name': table_node.get('name', table_id),
                    'packages': list(usage['packages']),
                    'readers': list(usage['readers']),
                    'writers': list(usage['writers']),
                    'is_integration_point': len(usage['writers']) > 0 and len(usage['readers']) > 0,
                    'package_count': len(usage['packages']),
                    'properties': table_node.get('properties', {})
                }
        
        return shared_tables
    
    def _analyze_shared_connections(self, packages: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Identify connections that are used by multiple packages."""
        connection_usage = defaultdict(set)
        
        for package in packages:
            package_id = package['id']
            
            # Find all operations in this package that use connections  
            package_operations = []
            for source, target, edge_data in self.graph.edges(data=True):
                if (source == package_id and 
                    edge_data.get('relation') == 'contains' and 
                    self.graph.nodes[target].get('node_type') == 'operation'):
                    package_operations.append(target)
            
            # Check if operations use any connections
            for operation_id in package_operations:
                for source, target, edge_data in self.graph.edges(data=True):
                    if (source == operation_id and 
                        self.graph.nodes[target].get('node_type') == 'connection' and 
                        edge_data.get('relation') == 'uses_connection'):
                        connection_usage[target].add(package_id)
        
        # Filter to only shared connections
        shared_connections = {}
        for connection_id, packages_using in connection_usage.items():
            if len(packages_using) > 1:
                connection_node = self.graph.nodes[connection_id]
                shared_connections[connection_id] = {
                    'connection_name': connection_node.get('name', connection_id),
                    'packages': list(packages_using),
                    'package_count': len(packages_using),
                    'concurrent_usage_risk': 'HIGH' if len(packages_using) > 3 else 'MEDIUM',
                    'properties': connection_node.get('properties', {})
                }
        
        return shared_connections
    
    def _analyze_shared_parameters(self, packages: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Identify parameters that are used by multiple packages."""
        parameter_usage = defaultdict(set)
        
        for package in packages:
            package_id = package['id']
            
            # Find all operations in this package that use parameters
            package_operations = []
            for source, target, edge_data in self.graph.edges(data=True):
                if (source == package_id and 
                    edge_data.get('relation') == 'contains' and 
                    self.graph.nodes[target].get('node_type') == 'operation'):
                    package_operations.append(target)
            
            # Check if operations use any parameters
            for operation_id in package_operations:
                for source, target, edge_data in self.graph.edges(data=True):
                    if (source == operation_id and 
                        self.graph.nodes[target].get('node_type') == 'parameter' and 
                        edge_data.get('relation') == 'uses_parameter'):
                        parameter_usage[target].add(package_id)
        
        # Filter to only shared parameters
        shared_parameters = {}
        for param_id, packages_using in parameter_usage.items():
            if len(packages_using) > 1:
                param_node = self.graph.nodes[param_id]
                shared_parameters[param_id] = {
                    'parameter_name': param_node.get('name', param_id),
                    'packages': list(packages_using),
                    'package_count': len(packages_using),
                    'properties': param_node.get('properties', {})
                }
        
        return shared_parameters
    
    def _analyze_data_flow_dependencies(self, packages: List[Dict[str, Any]], 
                                      shared_tables: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze data flow dependencies between packages via shared tables."""
        dependencies = []
        
        for table_id, table_info in shared_tables.items():
            if table_info['is_integration_point']:
                # This table has both writers and readers across packages
                writer_packages = set()
                reader_packages = set()
                
                # Determine which packages write vs read this table
                for writer_op in table_info['writers']:
                    for source, target, edge_data in self.graph.edges(data=True):
                        if (target == writer_op and 
                            edge_data.get('relation') == 'contains'):
                            writer_packages.add(source)
                            break
                
                for reader_op in table_info['readers']:
                    for source, target, edge_data in self.graph.edges(data=True):
                        if (target == reader_op and 
                            edge_data.get('relation') == 'contains'):
                            reader_packages.add(source)
                            break
                
                # Create dependencies: writers must complete before readers
                for writer_pkg in writer_packages:
                    for reader_pkg in reader_packages:
                        if writer_pkg != reader_pkg:
                            dependencies.append({
                                'source_package': writer_pkg,
                                'target_package': reader_pkg,
                                'dependency_type': 'data_flow',
                                'shared_resource': table_id,
                                'shared_resource_name': table_info['table_name'],
                                'description': f"{writer_pkg} must complete before {reader_pkg} (via {table_info['table_name']})"
                            })
        
        return dependencies
    
    def _determine_execution_order(self, packages: List[Dict[str, Any]], 
                                 dependencies: List[Dict[str, Any]]) -> List[List[str]]:
        """Determine execution order based on dependencies using topological sort."""
        # Create dependency graph
        dep_graph = nx.DiGraph()
        
        # Add all packages as nodes
        for package in packages:
            dep_graph.add_node(package['id'])
        
        # Add dependency edges
        for dep in dependencies:
            dep_graph.add_edge(dep['source_package'], dep['target_package'])
        
        # Perform topological sort to get execution order
        try:
            execution_order = []
            if dep_graph.number_of_edges() > 0:
                # Group packages by execution level
                levels = []
                temp_graph = dep_graph.copy()
                
                while temp_graph.nodes():
                    # Find nodes with no incoming edges (can execute now)
                    current_level = [node for node in temp_graph.nodes() 
                                   if temp_graph.in_degree(node) == 0]
                    
                    if not current_level:
                        # Circular dependency detected
                        logger.warning("Circular dependency detected in package execution order")
                        remaining_nodes = list(temp_graph.nodes())
                        levels.append(remaining_nodes)
                        break
                    
                    levels.append(current_level)
                    temp_graph.remove_nodes_from(current_level)
                
                execution_order = levels
            else:
                # No dependencies, all packages can run in parallel
                execution_order = [[pkg['id'] for pkg in packages]]
                
        except nx.NetworkXError as e:
            logger.error(f"Error determining execution order: {e}")
            execution_order = [[pkg['id'] for pkg in packages]]  # Fallback to all parallel
        
        return execution_order
    
    def _analyze_resource_contention(self, shared_connections: Dict[str, Dict[str, Any]], 
                                   shared_tables: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze potential resource contention issues."""
        contention_risks = {
            'high_risk_connections': [],
            'high_contention_tables': [],
            'risk_summary': {}
        }
        
        # Identify high-risk connections (used by many packages)
        for conn_id, conn_info in shared_connections.items():
            if conn_info['package_count'] > 3:
                contention_risks['high_risk_connections'].append({
                    'connection': conn_info['connection_name'],
                    'package_count': conn_info['package_count'],
                    'packages': conn_info['packages']
                })
        
        # Identify high-contention tables
        for table_id, table_info in shared_tables.items():
            if table_info['package_count'] > 2 and table_info['is_integration_point']:
                contention_risks['high_contention_tables'].append({
                    'table': table_info['table_name'],
                    'package_count': table_info['package_count'],
                    'packages': table_info['packages']
                })
        
        contention_risks['risk_summary'] = {
            'total_shared_connections': len(shared_connections),
            'high_risk_connections': len(contention_risks['high_risk_connections']),
            'total_shared_tables': len(shared_tables),
            'high_contention_tables': len(contention_risks['high_contention_tables'])
        }
        
        return contention_risks
    
    def _create_cross_package_edges(self, data_dependencies: List[Dict[str, Any]], 
                                  shared_connections: Dict[str, Dict[str, Any]],
                                  shared_parameters: Dict[str, Dict[str, Any]]) -> List[Edge]:
        """Create cross-package edges for the graph."""
        cross_package_edges = []
        
        # Create data dependency edges
        for dep in data_dependencies:
            edge = Edge(
                source_id=dep['target_package'],  # Reader depends on writer
                target_id=dep['source_package'],  # Writer is the dependency
                relation=EdgeType.DEPENDS_ON,
                properties={
                    'dependency_type': dep['dependency_type'],
                    'shared_resource': dep['shared_resource'],
                    'shared_resource_name': dep['shared_resource_name'],
                    'description': dep['description']
                }
            )
            cross_package_edges.append(edge)
        
        # Create shared connection edges (informational)
        for conn_id, conn_info in shared_connections.items():
            packages = conn_info['packages']
            for i, pkg1 in enumerate(packages):
                for pkg2 in packages[i+1:]:
                    edge = Edge(
                        source_id=pkg1,
                        target_id=pkg2,
                        relation=EdgeType.SHARES_RESOURCE,
                        properties={
                            'resource_type': 'connection',
                            'shared_resource': conn_id,
                            'resource_name': conn_info['connection_name'],
                            'contention_risk': conn_info['concurrent_usage_risk']
                        }
                    )
                    cross_package_edges.append(edge)
        
        # Create shared parameter edges (informational)
        for param_id, param_info in shared_parameters.items():
            packages = param_info['packages']
            for i, pkg1 in enumerate(packages):
                for pkg2 in packages[i+1:]:
                    edge = Edge(
                        source_id=pkg1,
                        target_id=pkg2,
                        relation=EdgeType.SHARES_RESOURCE,
                        properties={
                            'resource_type': 'parameter',
                            'shared_resource': param_id,
                            'resource_name': param_info['parameter_name']
                        }
                    )
                    cross_package_edges.append(edge)
        
        return cross_package_edges
    
    def _add_cross_package_edges_to_graph(self, cross_package_edges: List[Edge]):
        """Add cross-package edges to the graph."""
        for edge in cross_package_edges:
            try:
                self.graph_client.write_edge(edge)
            except Exception as e:
                logger.warning(f"Failed to add cross-package edge {edge.source_id} -> {edge.target_id}: {e}")
    
    def _update_package_properties(self, packages: List[Dict[str, Any]], 
                                 data_dependencies: List[Dict[str, Any]],
                                 execution_order: List[List[str]],
                                 shared_tables: Dict[str, Dict[str, Any]],
                                 shared_connections: Dict[str, Dict[str, Any]]):
        """Update package node properties with cross-package analysis results."""
        
        # Calculate execution priorities
        execution_priority_map = {}
        for level, package_group in enumerate(execution_order):
            for package_id in package_group:
                execution_priority_map[package_id] = level + 1
        
        # Calculate dependencies for each package
        upstream_deps = defaultdict(set)
        downstream_deps = defaultdict(set)
        
        for dep in data_dependencies:
            upstream_deps[dep['target_package']].add(dep['source_package'])
            downstream_deps[dep['source_package']].add(dep['target_package'])
        
        # Update each package's properties
        for package in packages:
            package_id = package['id']
            
            # Get current node and update properties
            if self.graph.has_node(package_id):
                current_properties = self.graph.nodes[package_id].get('properties', {})
                
                # Add cross-package analysis results
                current_properties.update({
                    'execution_priority': execution_priority_map.get(package_id, 1),
                    'upstream_dependencies': list(upstream_deps.get(package_id, set())),
                    'downstream_dependencies': list(downstream_deps.get(package_id, set())),
                    'shared_tables_used': [table_id for table_id, table_info in shared_tables.items() 
                                         if package_id in table_info['packages']],
                    'shared_connections_used': [conn_id for conn_id, conn_info in shared_connections.items() 
                                              if package_id in conn_info['packages']],
                    'cross_package_analysis_complete': True
                })
                
                # Update the node in the graph
                nx.set_node_attributes(self.graph, {package_id: {'properties': current_properties}})