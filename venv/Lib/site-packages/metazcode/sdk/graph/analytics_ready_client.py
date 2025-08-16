"""
Analytics-Ready Memgraph Client

Extends the base Memgraph client to create analytics-ready graphs optimized for
downstream application consumption including migration, compliance, and governance tools.
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from .client_memgraph import MemgraphClient
from ..models.config import DatabaseConfig

logger = logging.getLogger(__name__)


class AnalyticsReadyMemgraphClient(MemgraphClient):
    """
    Analytics-ready Memgraph client optimized for downstream application consumption.
    
    This client extends the base Memgraph functionality to automatically create
    performance indexes, materialized views, and metadata structures that enable
    fast queries for various analytical applications.
    
    Supported use cases:
    - Migration planning and analysis
    - Compliance and audit reporting  
    - Data governance and lineage tracking
    - Impact analysis and risk assessment
    """
    
    def __init__(self, config: DatabaseConfig):
        """Initialize analytics-ready client with automatic optimization."""
        super().__init__(config)
        self._optimization_completed = False
        logger.info("Initialized analytics-ready Memgraph client")
    
    def prepare_for_applications(self):
        """
        Prepare the graph for downstream application consumption.
        
        This method should be called after ingestion is complete to create
        all necessary indexes, views, and metadata for optimal application performance.
        """
        if self._optimization_completed:
            logger.info("Graph already prepared for applications")
            return
            
        logger.info("Preparing graph for downstream applications...")
        
        try:
            # Step 1: Create performance indexes
            self._create_performance_indexes()
            
            # Step 2: Create materialized views for applications
            self._create_application_views()
            
            # Step 3: Add graph metadata for readiness verification
            self._add_application_metadata()
            
            self._optimization_completed = True
            logger.info("✅ Graph prepared for applications successfully")
            
        except Exception as e:
            logger.error(f"Failed to prepare graph for applications: {e}")
            raise
    
    def _create_performance_indexes(self):
        """Create performance indexes optimized for analytical queries."""
        logger.info("Creating performance indexes...")
        
        indexes = [
            # Basic lookup performance
            "CREATE INDEX ON :Node(id);",
            "CREATE INDEX ON :Node(node_type);", 
            "CREATE INDEX ON :Node(name);",
            
            # Application-specific performance indexes
            "CREATE TEXT INDEX application_search ON :Node(properties);",
            "CREATE INDEX ON :Node(technology);",
            
            # Cross-package analysis indexes  
            "CREATE INDEX ON :Node(execution_priority);",
            "CREATE INDEX ON :Node(shared_across_packages);",
        ]
        
        created_count = 0
        for index_query in indexes:
            try:
                self._execute_query(index_query)
                created_count += 1
                logger.debug(f"Created index: {index_query}")
            except Exception as e:
                logger.debug(f"Index creation skipped (may exist): {e}")
        
        logger.info(f"✅ Created {created_count} performance indexes")
    
    def _create_application_views(self):
        """Create materialized views optimized for various downstream applications."""
        logger.info("Creating materialized views for applications...")
        
        views = {
            # For Migration Applications
            "sql_operations_catalog": self._build_sql_operations_view(),
            "cross_package_dependencies": self._build_dependencies_view(),
            "shared_resources_analysis": self._build_shared_resources_view(),
            
            # For Compliance Applications  
            "data_lineage_catalog": self._build_lineage_view(),
            "business_rules_catalog": self._build_business_rules_view(),
            
            # For Governance Applications
            "graph_summary_stats": self._build_summary_stats_view(),
            "complexity_metrics": self._build_complexity_metrics_view()
        }
        
        created_count = 0
        for view_name, view_data in views.items():
            try:
                self._store_materialized_view(view_name, view_data)
                created_count += 1
            except Exception as e:
                logger.error(f"Failed to create view '{view_name}': {e}")
        
        logger.info(f"✅ Created {created_count} materialized views")
    
    def _build_sql_operations_view(self) -> List[Dict[str, Any]]:
        """Build catalog of SQL operations for migration and analysis applications."""
        query = """
            MATCH (n:Node) 
            WHERE n.node_type = 'operation' 
            AND n.properties CONTAINS 'sql_transformation'
            RETURN n.id as operation_id,
                   n.name as operation_name,
                   n.properties as operation_details
        """
        results = self._execute_query(query)
        
        catalog = []
        for result in results:
            try:
                op_details = json.loads(result[2]) if isinstance(result[2], str) else result[2]
                sql_info = op_details.get('sql_transformation', {})
                
                catalog.append({
                    'operation_id': result[0],
                    'operation_name': result[1], 
                    'sql_type': sql_info.get('query_type', 'unknown'),
                    'affected_tables': sql_info.get('affected_tables', []),
                    'has_parameters': bool(sql_info.get('parameters', [])),
                    'complexity_indicators': {
                        'table_count': len(sql_info.get('affected_tables', [])),
                        'has_joins': 'JOIN' in str(sql_info.get('sql_query', '')).upper(),
                        'has_subqueries': str(sql_info.get('sql_query', '')).upper().count('SELECT') > 1,
                        'parameter_count': len(sql_info.get('parameters', []))
                    },
                    'raw_sql': sql_info.get('sql_query', ''),
                    'technology': op_details.get('technology', 'SSIS')
                })
            except Exception as e:
                logger.warning(f"Failed to process SQL operation {result[0]}: {e}")
                continue
        
        logger.info(f"Built SQL operations catalog with {len(catalog)} operations")
        return catalog
    
    def _build_dependencies_view(self) -> List[Dict[str, Any]]:
        """Build cross-package dependencies view for migration planning."""
        query = """
            MATCH (p1:Node {node_type: 'pipeline'})-[r:DEPENDS_ON]->(p2:Node {node_type: 'pipeline'})
            RETURN p1.name as source_package,
                   p2.name as target_package,
                   r.dependency_type as dependency_type,
                   r.shared_resources as shared_resources
        """
        results = self._execute_query(query)
        
        dependencies = []
        for result in results:
            dependencies.append({
                'source_package': result[0],
                'target_package': result[1],
                'dependency_type': result[2] if result[2] else 'package_dependency',
                'shared_resources': result[3] if result[3] else [],
                'risk_level': self._calculate_dependency_risk(result[0], result[1])
            })
        
        logger.info(f"Built dependencies view with {len(dependencies)} dependencies")
        return dependencies
    
    def _build_shared_resources_view(self) -> List[Dict[str, Any]]:
        """Build shared resources analysis for impact assessment."""
        query = """
            MATCH (t:Node {node_type: 'table'})
            MATCH (t)<-[:READS_FROM|WRITES_TO]-(op:Node)
            MATCH (op)-[:CONTAINS*]-(p:Node {node_type: 'pipeline'})
            WITH t, collect(DISTINCT p.name) as packages, collect(DISTINCT op.name) as operations
            WHERE size(packages) > 1
            RETURN t.id as resource_id,
                   t.name as resource_name,
                   packages as sharing_packages,
                   operations as accessing_operations,
                   size(packages) as package_count
        """
        results = self._execute_query(query)
        
        shared_resources = []
        for result in results:
            shared_resources.append({
                'resource_id': result[0],
                'resource_name': result[1],
                'sharing_packages': result[2],
                'accessing_operations': result[3],
                'package_count': result[4],
                'contention_risk': 'HIGH' if result[4] > 3 else 'MEDIUM' if result[4] > 1 else 'LOW',
                'resource_type': 'table'  # Could be extended for other resource types
            })
        
        logger.info(f"Built shared resources view with {len(shared_resources)} shared resources")
        return shared_resources
    
    def _build_lineage_view(self) -> List[Dict[str, Any]]:
        """Build data lineage catalog for compliance applications."""
        query = """
            MATCH (source:Node)-[r:READS_FROM|WRITES_TO]->(target:Node)
            RETURN source.id as source_id,
                   source.name as source_name,
                   source.node_type as source_type,
                   type(r) as relationship_type,
                   target.id as target_id,
                   target.name as target_name,
                   target.node_type as target_type
        """
        results = self._execute_query(query)
        
        lineage = []
        for result in results:
            lineage.append({
                'source_id': result[0],
                'source_name': result[1],
                'source_type': result[2],
                'relationship_type': result[3],
                'target_id': result[4],
                'target_name': result[5],
                'target_type': result[6],
                'lineage_direction': 'downstream' if result[3] == 'WRITES_TO' else 'upstream'
            })
        
        logger.info(f"Built lineage view with {len(lineage)} lineage relationships")
        return lineage
    
    def _build_business_rules_view(self) -> List[Dict[str, Any]]:
        """Build business rules catalog from conditional logic and transformations."""
        query = """
            MATCH (n:Node) 
            WHERE n.node_type = 'operation' 
            AND (n.properties CONTAINS 'conditional_split' 
                 OR n.properties CONTAINS 'derived_column_expressions'
                 OR n.properties CONTAINS 'lookups')
            RETURN n.id as operation_id,
                   n.name as operation_name,
                   n.properties as operation_details
        """
        results = self._execute_query(query)
        
        business_rules = []
        for result in results:
            try:
                op_details = json.loads(result[2]) if isinstance(result[2], str) else result[2]
                
                # Extract business rules from different operation types
                rules = []
                
                # Conditional split rules
                if 'conditional_split' in op_details:
                    conditions = op_details['conditional_split'].get('conditions', [])
                    for condition in conditions:
                        rules.append({
                            'rule_type': 'conditional_split',
                            'expression': condition.get('expression', ''),
                            'output_name': condition.get('output_name', ''),
                            'description': f"Route data to {condition.get('output_name', 'output')} when {condition.get('expression', 'condition met')}"
                        })
                
                # Derived column rules
                if 'derived_column_expressions' in op_details:
                    expressions = op_details['derived_column_expressions'].get('expressions', [])
                    for expr in expressions:
                        rules.append({
                            'rule_type': 'derived_column',
                            'expression': expr.get('expression', ''),
                            'column_name': expr.get('column_name', ''),
                            'description': f"Calculate {expr.get('column_name', 'column')} as {expr.get('expression', 'expression')}"
                        })
                
                if rules:
                    business_rules.append({
                        'operation_id': result[0],
                        'operation_name': result[1],
                        'rules': rules,
                        'rule_count': len(rules)
                    })
                    
            except Exception as e:
                logger.warning(f"Failed to process business rules for {result[0]}: {e}")
                continue
        
        logger.info(f"Built business rules view with {len(business_rules)} rule-containing operations")
        return business_rules
    
    def _build_summary_stats_view(self) -> List[Dict[str, Any]]:
        """Build graph summary statistics for governance dashboards."""
        stats_queries = {
            'total_nodes': "MATCH (n:Node) RETURN count(n)",
            'total_edges': "MATCH ()-[r]->() RETURN count(r)",
            'pipelines': "MATCH (n:Node {node_type: 'pipeline'}) RETURN count(n)",
            'operations': "MATCH (n:Node {node_type: 'operation'}) RETURN count(n)",
            'tables': "MATCH (n:Node {node_type: 'table'}) RETURN count(n)",
            'connections': "MATCH (n:Node {node_type: 'connection'}) RETURN count(n)"
        }
        
        stats = {}
        for stat_name, query in stats_queries.items():
            try:
                result = self._execute_query(query)
                stats[stat_name] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Failed to calculate {stat_name}: {e}")
                stats[stat_name] = 0
        
        summary = [{
            'metric_name': 'graph_summary',
            'statistics': stats,
            'calculated_at': datetime.now().isoformat(),
            'version': '1.0'
        }]
        
        logger.info("Built summary statistics view")
        return summary
    
    def _build_complexity_metrics_view(self) -> List[Dict[str, Any]]:
        """Build complexity metrics for migration planning."""
        # This is a simplified version - could be expanded with more sophisticated metrics
        complexity_metrics = [{
            'metric_name': 'system_complexity',
            'package_count': self._get_package_count(),
            'operation_count': self._get_operation_count(),
            'cross_package_dependencies': self._get_dependency_count(),
            'shared_resource_count': self._get_shared_resource_count(),
            'complexity_score': self._calculate_overall_complexity()
        }]
        
        logger.info("Built complexity metrics view")
        return complexity_metrics
    
    def _store_materialized_view(self, view_name: str, view_data: List[Dict[str, Any]]):
        """Store materialized view as a special node for fast access by applications."""
        # Delete existing view
        delete_query = "MATCH (v:Node {id: $view_id, node_type: 'materialized_view'}) DELETE v"
        view_id = f"view:{view_name}"
        self._execute_query(delete_query, {"view_id": view_id})
        
        # Create new view node with proper Node structure
        create_query = """
            CREATE (v:Node {
                id: $id,
                node_id: $id,
                node_type: 'materialized_view',
                name: $name,
                properties: $properties,
                context: $context
            })
        """
        
        # Package the view data in the properties field
        properties = {
            "data": json.dumps(view_data),
            "created_at": datetime.now().isoformat(),
            "record_count": len(view_data),
            "version": "1.0",
            "view_type": "analytics_ready"
        }
        
        self._execute_query(create_query, {
            "id": view_id,
            "name": view_name,
            "properties": json.dumps(properties),
            "context": json.dumps({"application_ready": True})
        })
        
        logger.debug(f"Stored materialized view '{view_name}' with {len(view_data)} records")
    
    def _add_application_metadata(self):
        """Add metadata indicating the graph is ready for application consumption."""
        logger.info("Adding application readiness metadata...")
        
        # Get graph statistics
        node_count = self.get_node_count()
        edge_count = self.get_edge_count()
        
        # Count by node types
        type_counts = {}
        for node_type in ['pipeline', 'operation', 'table', 'connection', 'parameter', 'variable']:
            query = f"MATCH (n:Node) WHERE n.node_type = '{node_type}' RETURN count(n)"
            try:
                result = self._execute_query(query)
                type_counts[node_type] = result[0][0] if result else 0
            except Exception as e:
                logger.warning(f"Failed to count nodes of type '{node_type}': {e}")
                type_counts[node_type] = 0
        
        # Store comprehensive metadata as a proper Node
        metadata_id = "metadata:graph_readiness"
        
        # Delete existing metadata
        delete_query = "MATCH (m:Node {id: $metadata_id, node_type: 'graph_metadata'}) DELETE m"
        self._execute_query(delete_query, {"metadata_id": metadata_id})
        
        # Create metadata with proper Node structure
        metadata_query = """
            CREATE (m:Node {
                id: $id,
                node_id: $id,
                node_type: 'graph_metadata',
                name: 'graph_readiness_metadata',
                properties: $properties,
                context: $context
            })
        """
        
        # Package metadata in properties
        metadata_properties = {
            "total_nodes": node_count,
            "total_edges": edge_count,
            "node_type_counts": type_counts,
            "analysis_timestamp": datetime.now().isoformat(),
            "metazcode_version": "1.0",
            "ready_for_applications": True,
            "optimization_level": "analytics_ready",
            "supported_use_cases": [
                "migration_planning",
                "compliance_analysis", 
                "governance_reporting",
                "data_lineage_tracking",
                "impact_analysis",
                "business_rules_analysis"
            ],
            "materialized_views": [
                "sql_operations_catalog",
                "cross_package_dependencies",
                "shared_resources_analysis", 
                "data_lineage_catalog",
                "business_rules_catalog",
                "graph_summary_stats",
                "complexity_metrics"
            ],
            "performance_indexes": [
                "node_id_index",
                "node_type_index",
                "node_name_index",
                "application_search_text_index",
                "technology_index",
                "execution_priority_index",
                "shared_resources_index"
            ]
        }
        
        self._execute_query(metadata_query, {
            "id": metadata_id,
            "properties": json.dumps(metadata_properties),
            "context": json.dumps({"system_metadata": True, "application_ready": True})
        })
        
        logger.info("✅ Added application readiness metadata")
    
    # Helper methods for metrics calculation
    def _calculate_dependency_risk(self, source_package: str, target_package: str) -> str:
        """Calculate risk level for package dependency."""
        # Simplified risk calculation - could be enhanced
        return "MEDIUM"  # Default risk level
    
    def _get_package_count(self) -> int:
        """Get total number of packages."""
        query = "MATCH (n:Node {node_type: 'pipeline'}) RETURN count(n)"
        result = self._execute_query(query)
        return result[0][0] if result else 0
    
    def _get_operation_count(self) -> int:
        """Get total number of operations."""
        query = "MATCH (n:Node {node_type: 'operation'}) RETURN count(n)"
        result = self._execute_query(query)
        return result[0][0] if result else 0
    
    def _get_dependency_count(self) -> int:
        """Get total number of cross-package dependencies."""
        query = "MATCH ()-[r:DEPENDS_ON]->() RETURN count(r)"
        result = self._execute_query(query)
        return result[0][0] if result else 0
    
    def _get_shared_resource_count(self) -> int:
        """Get number of shared resources."""
        query = """
            MATCH (t:Node {node_type: 'table'})
            MATCH (t)<-[:READS_FROM|WRITES_TO]-(op:Node)
            MATCH (op)-[:CONTAINS*]-(p:Node {node_type: 'pipeline'})
            WITH t, collect(DISTINCT p.name) as packages
            WHERE size(packages) > 1
            RETURN count(t)
        """
        result = self._execute_query(query)
        return result[0][0] if result else 0
    
    def _calculate_overall_complexity(self) -> float:
        """Calculate overall system complexity score."""
        # Simplified complexity calculation
        package_count = self._get_package_count()
        operation_count = self._get_operation_count()
        dependency_count = self._get_dependency_count()
        
        if package_count == 0:
            return 0.0
        
        # Simple complexity formula - could be enhanced
        complexity = (operation_count / package_count) + (dependency_count * 0.5)
        return round(complexity, 2)