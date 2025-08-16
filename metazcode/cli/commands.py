import click
import os
from pathlib import Path
from typing import Optional
import json
import networkx as nx
import logging

from metazcode.sdk.graph.graph_constructor import GraphClientBuilder
from metazcode.cli.orchestrator import Orchestrator
from metazcode.sdk.models.canonical_types import NodeType
from metazcode.sdk.integration.index_integration import IndexIntegration
from metazcode.sdk.models.config import DatabaseConfig, MetaZenseConfig
from metazcode.sdk.ingestion.plsql import PlsqlLoader


def database_option(f):
    """Decorator to add database configuration options to CLI commands."""
    f = click.option(
        "--database",
        type=click.Choice(["networkx", "memgraph"]),
        default=None,
        help="Graph database backend to use (overrides environment variable)",
    )(f)
    f = click.option(
        "--memgraph-host",
        default=None,
        help="Memgraph server host (default: localhost)",
    )(f)
    f = click.option(
        "--memgraph-port",
        type=int,
        default=None,
        help="Memgraph server port (default: 7687)",
    )(f)
    f = click.option(
        "--memgraph-username",
        default=None,
        help="Memgraph username (default: from environment)",
    )(f)
    f = click.option(
        "--memgraph-password",
        default=None,
        help="Memgraph password (default: from environment)",
    )(f)
    return f


def get_database_config(
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
) -> DatabaseConfig:
    """Create database configuration from CLI options and environment."""
    config = DatabaseConfig.from_environment()

    # Override with CLI options if provided
    if database:
        config.backend = database
    if memgraph_host:
        config.host = memgraph_host
    if memgraph_port:
        config.port = memgraph_port
    if memgraph_username:
        config.username = memgraph_username
    if memgraph_password:
        config.password = memgraph_password

    return config


@click.group()
def cli():
    """A command-line tool for Metazcode."""
    pass


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory or a specific file to ingest.",
)
@database_option
def ingest(
    path: str,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Discovers and runs all available ingestion tools on the specified path.
    Can target a whole directory or a single supported file.
    """
    click.echo(f"Starting ingestion for: {os.path.abspath(path)}")

    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(
            f"Error: The path '{path}' does not exist or is not a valid file or directory.",
            err=True,
        )
        return

    # 1. Build the graph client with configuration
    db_config = get_database_config(
        database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
    )

    # Test connection and show status
    if not GraphClientBuilder.validate_connection(db_config):
        click.echo(
            f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
            err=True,
        )

    graph_client = GraphClientBuilder.get_client(db_config)

    # 2. Initialize and run the orchestrator
    orchestrator = Orchestrator(
        graph_client=graph_client, root_path=root_path, target_file=target_file
    )
    orchestrator.run()

    click.echo("[SUCCESS] Ingestion finished successfully.")


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory to ingest for visualization.",
)
@click.option(
    "--output-path", default="graph.png", help="Path to save the visualization."
)
@database_option
def visualize(
    path: str,
    output_path: str,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Generates a visual representation of the graph.
    NOTE: This is a basic visualization and may be unreadable for large graphs.
    """
    click.echo(
        f"Generating graph visualization for '{path}' and saving to '{output_path}'..."
    )

    # Build the graph client with configuration
    db_config = get_database_config(
        database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
    )

    # Test connection and show status
    if not GraphClientBuilder.validate_connection(db_config):
        click.echo(
            f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
            err=True,
        )

    graph_client = GraphClientBuilder.get_client(db_config)

    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(
            f"Error: The path '{path}' does not exist or is not a valid file or directory.",
            err=True,
        )
        return

    orchestrator = Orchestrator(
        graph_client=graph_client, root_path=root_path, target_file=target_file
    )
    orchestrator.run()  # This will populate the in-memory graph

    try:
        from metazcode.sdk.graph.visualizer import visualize_graph

        graph = graph_client.get_graph()
        visualize_graph(graph, output_path)
        click.echo(f"[SUCCESS] Visualization saved to {output_path}")
    except ImportError:
        click.echo(
            "Please install matplotlib and pygraphviz to use this command.", err=True
        )
    except Exception as e:
        click.echo(f"Error during visualization: {e}", err=True)


@cli.command()
@click.option(
    "--path", default=".", help="The path to the project directory to ingest."
)
@click.option(
    "--output",
    default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Path to save the output JSON file.",
)
@database_option
def dump(
    path: str,
    output: Optional[str],
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Ingests the project and prints the nodes and edges to the console or saves to a JSON file.
    """
    click.echo("==================== Starting Ingestion ====================")

    # Build the graph client with configuration
    db_config = get_database_config(
        database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
    )

    # Test connection and show status
    if not GraphClientBuilder.validate_connection(db_config):
        click.echo(
            f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
            err=True,
        )

    graph_client = GraphClientBuilder.get_client(db_config)

    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(f"Error: Path '{path}' not found.", err=True)
        return

    orchestrator = Orchestrator(
        graph_client=graph_client, root_path=root_path, target_file=target_file
    )
    orchestrator.run()
    click.echo("====================== Ingestion Complete ======================")
    click.echo("")

    graph = graph_client.get_graph()

    if output:
        # Serialize and write to JSON file
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert graph to a JSON-serializable format
        graph_data = nx.node_link_data(graph, edges="links")

        # Custom JSON serialization for complex types if needed
        def default_serializer(o):
            if isinstance(o, (set, tuple)):
                return list(o)
            if hasattr(o, "to_dict"):
                return o.to_dict()
            try:
                return str(o)  # Fallback to string representation
            except TypeError:
                return repr(o)

        with open(output_path, "w") as f:
            json.dump(graph_data, f, indent=2, default=default_serializer)

        click.echo(f"[SUCCESS] Graph data saved to {output_path.resolve()}")
    else:
        # Print to console (existing behavior)
        click.echo("--- NODES ---")
        for node_id, data in graph.nodes(data=True):
            click.echo(f"ID: {node_id}")
            for key, value in data.items():
                click.echo(f"  {key}: {value}")
            click.echo("----------")

        click.echo("\n--- EDGES ---")
        for source, target, data in graph.edges(data=True):
            click.echo(f"FROM: {source}")
            click.echo(f"  TO: {target}")
            click.echo(f"  RELATION: {data.get('relation', 'N/A')}")
            click.echo("----------")


@cli.command()
@click.option(
    "--path", default=".", help="The path to the project directory to analyze."
)
@click.option(
    "--output",
    default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Path to save the cross-package analysis results JSON file.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging to see detailed progress.",
)
@database_option
def analyze(
    path: str,
    output: Optional[str],
    verbose: bool,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Perform cross-package dependency analysis on an existing graph.

    This command:
    1. Loads the existing graph from the specified path
    2. Identifies shared resources (tables, connections, parameters)
    3. Maps data flow dependencies between packages
    4. Determines execution order requirements
    5. Adds cross-package edges to the graph
    6. Updates package properties with dependency information

    Example: metazcode analyze --path ./data/ssis/project --output analysis.json
    """
    # Setup logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    click.echo(
        f"[START] Starting cross-package dependency analysis for: {os.path.abspath(path)}"
    )

    # Validate path
    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(f"[ERROR] Error: Path '{path}' not found.", err=True)
        return

    try:
        # 1. Build the graph client with configuration
        db_config = get_database_config(
            database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
        )

        # Test connection and show status
        if not GraphClientBuilder.validate_connection(db_config):
            click.echo(
                f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
                err=True,
            )

        graph_client = GraphClientBuilder.get_client(db_config)

        # 2. Load the graph by running ingestion first
        click.echo("[INFO] Loading existing graph...")
        orchestrator = Orchestrator(
            graph_client=graph_client, root_path=root_path, target_file=target_file
        )
        orchestrator.run()
        click.echo("[INFO] Graph loaded successfully")

        # 3. Initialize cross-package analyzer
        from metazcode.sdk.analysis.cross_package_analyzer import CrossPackageAnalyzer

        analyzer = CrossPackageAnalyzer(graph_client)

        # 4. Perform the analysis
        click.echo("[ANALYZE] Performing cross-package dependency analysis...")
        analysis_results = analyzer.analyze()

        # 5. Display results
        click.echo(f"[SUCCESS] Cross-package analysis completed!")
        click.echo(f"   Packages analyzed: {analysis_results['packages_analyzed']}")
        click.echo(
            f"   Shared tables: {analysis_results['shared_resources']['tables']}"
        )
        click.echo(
            f"   Shared connections: {analysis_results['shared_resources']['connections']}"
        )
        click.echo(f"   Data dependencies: {analysis_results['data_dependencies']}")
        click.echo(
            f"   Cross-package edges added: {analysis_results['cross_package_edges_added']}"
        )

        # Show execution chains
        execution_order = analysis_results["detailed_analysis"]["execution_order"]
        if len(execution_order) > 1:
            click.echo(f"\n[INFO] Execution order (sequential levels):")
            for level, packages in enumerate(execution_order, 1):
                package_names = [
                    pkg.split(":")[1] if ":" in pkg else pkg for pkg in packages
                ]
                click.echo(f"   Level {level}: {', '.join(package_names)}")
        else:
            click.echo(
                f"\n[INFO] All packages can execute in parallel (no dependencies)"
            )

        # Show high-risk resources
        contention_risks = analysis_results["contention_risks"]
        if contention_risks["high_risk_connections"]:
            click.echo(f"\n[WARNING] High-risk connections (resource contention):")
            for risk in contention_risks["high_risk_connections"]:
                click.echo(f"   {risk['connection']}: {risk['package_count']} packages")

        # 6. Save analysis results if output specified
        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w") as f:
                json.dump(analysis_results, f, indent=2, default=str)

            click.echo(f"[SUCCESS] Analysis results saved to {output_path.resolve()}")

        # 7. Optionally save enhanced graph
        enhanced_graph_path = Path("enhanced_graph_with_dependencies.json")
        graph = graph_client.get_graph()
        graph_data = nx.node_link_data(graph, edges="links")

        def default_serializer(o):
            if isinstance(o, (set, tuple)):
                return list(o)
            if hasattr(o, "to_dict"):
                return o.to_dict()
            try:
                return str(o)
            except TypeError:
                return repr(o)

        with open(enhanced_graph_path, "w") as f:
            json.dump(graph_data, f, indent=2, default=default_serializer)

        click.echo(
            f"[SUCCESS] Enhanced graph with cross-package dependencies saved to {enhanced_graph_path.resolve()}"
        )

    except Exception as e:
        click.echo(f"[ERROR] Error during cross-package analysis: {e}", err=True)
        if verbose:
            import traceback

            click.echo(traceback.format_exc(), err=True)


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory or a specific file to ingest and index.",
)
@click.option(
    "--index-output",
    default=None,
    help="Path to save the index file (optional).",
)
@click.option(
    "--project-id",
    default=None,
    help="Project identifier for the index (optional).",
)
@database_option
def ingest_n_index(
    path: str,
    index_output: Optional[str],
    project_id: Optional[str],
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Run both ingestion and indexing together.

    This command performs the complete workflow of data ingestion followed by
    enhanced SSIS indexing for fast search and analysis capabilities.
    """
    click.echo(
        f"Starting integrated ingestion and indexing for: {os.path.abspath(path)}"
    )

    try:
        # Initialize integration component
        integration = IndexIntegration()

        # Run integrated workflow
        results = integration.ingest_and_index(
            path=path, index_output=index_output, project_id=project_id
        )

        # Display results
        ingestion_stats = results["ingestion_results"]
        index_stats = results["index_results"]

        click.echo(f"\n[SUCCESS] Integrated workflow completed successfully!")
        click.echo(f"")
        click.echo(f"Ingestion Results:")
        click.echo(f"  - Nodes: {ingestion_stats['node_count']}")
        click.echo(f"  - Edges: {ingestion_stats['edge_count']}")
        click.echo(f"")
        click.echo(f"Index Results:")
        click.echo(f"  - Indexed nodes: {index_stats['node_count']}")
        click.echo(f"  - Unique names: {index_stats['unique_names']}")
        click.echo(f"  - BM25 metadata ready: {index_stats['bm25_metadata_ready']}")
        click.echo(f"  - BM25 content ready: {index_stats['bm25_content_ready']}")

        # Show SSIS enhancements if available
        if "ssis_enhancements" in index_stats:
            ssis_stats = index_stats["ssis_enhancements"]
            click.echo(f"")
            click.echo(f"SSIS Business Logic Indexing:")
            click.echo(
                f"  - SQL operations indexed: {ssis_stats['sql_operations_indexed']}"
            )
            click.echo(
                f"  - Shared tables indexed: {ssis_stats['shared_tables_indexed']}"
            )
            click.echo(
                f"  - Parameterized connections indexed: {ssis_stats['parameterized_connections_indexed']}"
            )
            click.echo(
                f"  - Cross-package pipelines indexed: {ssis_stats['cross_package_pipelines_indexed']}"
            )

        # Show file outputs
        files_created = results["files_created"]
        if files_created["index_file"]:
            click.echo(f"")
            click.echo(f"Files Created:")
            click.echo(f"  - Index file: {files_created['index_file']}")
            click.echo(f"  - Metadata file: {files_created['metadata_file']}")

        click.echo(f"")
        click.echo(
            f"[SUCCESS] Enhanced SSIS indexing complete. Graph is now searchable!"
        )

    except Exception as e:
        click.echo(
            f"[ERROR] Error during integrated ingestion and indexing: {e}", err=True
        )
        import traceback

        click.echo(traceback.format_exc(), err=True)


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory or a specific file to process.",
)
@click.option(
    "--output",
    default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Path to save the analysis results JSON file.",
)
@click.option(
    "--index-output",
    default=None,
    help="Path to save the index file (optional).",
)
@click.option(
    "--project-id",
    default=None,
    help="Project identifier for the index (optional).",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging to see detailed progress.",
)
@database_option
def full(
    path: str,
    output: Optional[str],
    index_output: Optional[str],
    project_id: Optional[str],
    verbose: bool,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Complete SSIS analysis: ingest + analyze + index in one command.

    This command performs the full MetaZenseCode workflow:
    1. Ingests SSIS packages and extracts business logic
    2. Performs cross-package dependency analysis
    3. Creates enhanced searchable index
    4. Exports rich graph with enterprise intelligence

    Perfect for users who want complete SSIS analysis without running separate commands.

    Example: metazcode full --path ./data/ssis/project --output analysis.json
    """
    # Setup logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    click.echo("Starting Complete SSIS Analysis (Ingest + Analyze + Index)")
    click.echo("=" * 70)
    click.echo(f"Project path: {os.path.abspath(path)}")

    # Validate path
    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(f"[ERROR] Path '{path}' not found.", err=True)
        return

    try:
        # Phase 1: Ingestion
        click.echo("")
        click.echo("Phase 1: SSIS Ingestion (Building Graph)")
        click.echo("-" * 50)

        # Build the graph client with configuration
        db_config = get_database_config(
            database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
        )

        # Test connection and show status
        if not GraphClientBuilder.validate_connection(db_config):
            click.echo(
                f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
                err=True,
            )

        graph_client = GraphClientBuilder.get_client(db_config)
        orchestrator = Orchestrator(
            graph_client=graph_client, root_path=root_path, target_file=target_file
        )
        orchestrator.run()

        initial_nodes = graph_client.get_node_count()
        initial_edges = graph_client.get_edge_count()
        click.echo(f"Ingestion complete: {initial_nodes} nodes, {initial_edges} edges")

        # Phase 2: Cross-Package Analysis
        click.echo("")
        click.echo("Phase 2: Cross-Package Analysis (Enterprise Intelligence)")
        click.echo("-" * 50)

        from metazcode.sdk.analysis.cross_package_analyzer import CrossPackageAnalyzer

        analyzer = CrossPackageAnalyzer(graph_client)
        analysis_results = analyzer.analyze()

        final_nodes = graph_client.get_node_count()
        final_edges = graph_client.get_edge_count()
        new_edges = final_edges - initial_edges

        click.echo(f"Analysis complete:")
        click.echo(f"   Packages analyzed: {analysis_results['packages_analyzed']}")
        click.echo(f"   Cross-package edges added: {new_edges}")
        click.echo(
            f"   Shared tables: {analysis_results['shared_resources']['tables']}"
        )
        click.echo(
            f"   Shared connections: {analysis_results['shared_resources']['connections']}"
        )

        # Show execution order
        execution_order = analysis_results["detailed_analysis"]["execution_order"]
        if len(execution_order) > 1:
            click.echo(f"")
            click.echo(f"Execution Order:")
            for level, packages in enumerate(execution_order, 1):
                package_names = [
                    pkg.split(":")[1] if ":" in pkg else pkg for pkg in packages
                ]
                click.echo(f"   Level {level}: {', '.join(package_names)}")
        else:
            click.echo(f"All packages can execute in parallel")

        # Show warnings
        contention_risks = analysis_results["contention_risks"]
        if contention_risks["high_risk_connections"]:
            click.echo(f"")
            click.echo(f"High-Risk Resources:")
            for risk in contention_risks["high_risk_connections"]:
                click.echo(f"   {risk['connection']}: {risk['package_count']} packages")

        # Phase 3: Enhanced Indexing
        click.echo("")
        click.echo("Phase 3: Enhanced Indexing (Search Capabilities)")
        click.echo("-" * 50)

        integration = IndexIntegration()

        # Run indexing on the already enhanced graph
        index = integration._build_enhanced_index(graph_client, project_id)

        # Save index if requested
        index_path = None
        metadata_path = None
        if index_output:
            index_path, metadata_path = integration._save_index(index, index_output)

        # Generate index results summary
        integration_results = integration._generate_results_summary(
            graph_client, index, index_path, metadata_path
        )
        index_results = integration_results["index_results"]

        click.echo(f"Indexing complete:")
        click.echo(f"   Indexed nodes: {index_results['node_count']}")
        click.echo(f"   Unique names: {index_results['unique_names']}")
        click.echo(f"   BM25 metadata ready: {index_results['bm25_metadata_ready']}")
        click.echo(f"   BM25 content ready: {index_results['bm25_content_ready']}")

        # Check if SSIS enhanced index has additional stats
        if hasattr(index, "get_ssis_enhancement_stats"):
            ssis_stats = index.get_ssis_enhancement_stats()
            if ssis_stats:
                click.echo(f"")
                click.echo(f"SSIS Business Logic Indexing:")
                click.echo(
                    f"   SQL operations: {ssis_stats.get('sql_operations_indexed', 0)}"
                )
                click.echo(
                    f"   Shared tables: {ssis_stats.get('shared_tables_indexed', 0)}"
                )
                click.echo(
                    f"   Parameterized connections: {ssis_stats.get('parameterized_connections_indexed', 0)}"
                )
                click.echo(
                    f"   Cross-package pipelines: {ssis_stats.get('cross_package_pipelines_indexed', 0)}"
                )

        # Phase 4: Analytics Preparation (Application Readiness)
        if db_config.backend == "memgraph":
            click.echo("")
            click.echo("Phase 4: Analytics Preparation (Application Readiness)")
            click.echo("-" * 50)
            
            # Trigger analytics preparation for downstream applications
            try:
                from metazcode.sdk.graph.analytics_ready_client import AnalyticsReadyMemgraphClient
                if isinstance(graph_client, AnalyticsReadyMemgraphClient):
                    graph_client.prepare_for_applications()
                    
                    click.echo("Analytics preparation complete:")
                    click.echo("   ✅ Performance indexes created")
                    click.echo("   ✅ Materialized views for applications created")
                    click.echo("   ✅ Graph metadata for readiness verification added")
                    click.echo("   ✅ Ready for downstream applications:")
                    click.echo("      • Migration planning tools")
                    click.echo("      • Compliance analysis tools") 
                    click.echo("      • Governance reporting tools")
                    click.echo("      • Data lineage tracking tools")
                else:
                    click.echo("Analytics preparation skipped (client type not supported)")
            except Exception as e:
                click.echo(f"Warning: Analytics preparation failed: {e}")

        # Save outputs
        click.echo("")
        click.echo("Saving Results")
        click.echo("-" * 50)

        # Save analysis results if requested
        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(analysis_results, f, indent=2, default=str)
            click.echo(f"Analysis results: {output_path.resolve()}")

        # Always save enhanced graph
        enhanced_graph_path = Path("enhanced_graph_full_analysis.json")
        
        # Handle different graph client types
        try:
            graph = graph_client.get_graph()
            # Check if it's a NetworkX graph
            if hasattr(graph, 'is_multigraph'):
                graph_data = nx.node_link_data(graph, edges="links")
            else:
                # For Memgraph, create a custom export
                all_nodes = graph_client.get_all_nodes()
                nodes_data = [node.to_dict() for node in all_nodes]
                
                # Get edges via custom query
                connection = graph_client.get_graph()
                cursor = connection.cursor()
                cursor.execute("""
                    MATCH (source)-[r]->(target)
                    RETURN source.id as source_id, target.id as target_id, 
                           type(r) as relation_type, properties(r) as properties
                """)
                edge_results = cursor.fetchall()
                
                edges_data = []
                for source_id, target_id, relation_type, properties in edge_results:
                    edge_dict = {
                        "source": source_id,
                        "target": target_id,
                        "relation": relation_type,
                        "properties": properties if properties else {}
                    }
                    edges_data.append(edge_dict)
                
                graph_data = {
                    "nodes": nodes_data,
                    "links": edges_data,
                    "metadata": {
                        "node_count": len(nodes_data),
                        "edge_count": len(edges_data),
                        "graph_type": "memgraph"
                    }
                }
        except Exception as e:
            click.echo(f"Warning: Could not export graph data: {e}")
            graph_data = {"error": str(e), "nodes": [], "links": []}

        def default_serializer(o):
            if isinstance(o, (set, tuple)):
                return list(o)
            if hasattr(o, "to_dict"):
                return o.to_dict()
            try:
                return str(o)
            except TypeError:
                return repr(o)

        with open(enhanced_graph_path, "w") as f:
            json.dump(graph_data, f, indent=2, default=default_serializer)

        click.echo(f"Enhanced graph: {enhanced_graph_path.resolve()}")

        # Show index files if created
        if index_path:
            click.echo(f"Index file: {index_path}")
        if metadata_path:
            click.echo(f"Metadata file: {metadata_path}")

        # Final summary
        click.echo("")
        click.echo("COMPLETE ANALYSIS FINISHED!")
        click.echo("=" * 70)
        click.echo(
            f"Graph: {final_nodes} nodes, {final_edges} edges ({new_edges} cross-package)"
        )
        click.echo(f"Intelligence: Business logic + Dependencies + Search ready")
        click.echo(f"Ready for: Migration planning, AI agents, Enterprise coordination")
        click.echo("")
        click.echo("Next steps:")
        click.echo("  Review analysis results in the generated JSON files")
        click.echo("  Use the enhanced graph for migration planning")
        click.echo("  Feed the rich metadata to AI migration agents")

    except Exception as e:
        click.echo(f"[ERROR] Error during complete analysis: {e}", err=True)
        if verbose:
            import traceback

            click.echo(traceback.format_exc(), err=True)


# Alias command for shorter usage
@cli.command()
@click.option("--path", default=".", help="The path to the project directory.")
@click.option("--output", default=None, help="Path to save analysis results.")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging.")
@database_option
def complete(
    path: str,
    output: Optional[str],
    verbose: bool,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Alias for 'full' command - complete SSIS analysis in one shot.

    Shorter command for: ingest + analyze + index

    Example: metazcode complete --path ./data/ssis/project
    """
    # Call the full command with default parameters
    from click import Context

    ctx = Context(full)
    ctx.invoke(
        full,
        path=path,
        output=output,
        index_output=None,
        project_id=None,
        verbose=verbose,
        database=database,
        memgraph_host=memgraph_host,
        memgraph_port=memgraph_port,
        memgraph_username=memgraph_username,
        memgraph_password=memgraph_password,
    )


if __name__ == "__main__":
    cli()


# --------------------------
# PL/SQL specific commands
# --------------------------

@cli.command(name="plsql-ingest")
@click.option(
    "--path",
    default=".",
    help="The path to the PL/SQL project directory or a specific file.",
)
@database_option
def plsql_ingest(
    path: str,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """Ingest only PL/SQL ETL artifacts and populate the graph."""
    click.echo(f"[PLSQL] Ingesting PL/SQL from: {os.path.abspath(path)}")

    p = Path(path)
    root_path: str
    target_file: Optional[str] = None
    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(f"[ERROR] Path '{path}' not found.", err=True)
        return

    # Build the graph client with configuration
    db_config = get_database_config(
        database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
    )
    if not GraphClientBuilder.validate_connection(db_config):
        click.echo(
            f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
            err=True,
        )
    graph_client = GraphClientBuilder.get_client(db_config)

    # Run only the PL/SQL loader
    loader = PlsqlLoader(root_path=root_path, target_file=target_file)
    total_nodes = 0
    total_edges = 0
    for nodes, edges in loader.ingest():
        if nodes:
            graph_client.add_nodes(nodes)
            total_nodes += len(nodes)
        if edges:
            graph_client.add_edges(edges)
            total_edges += len(edges)

    click.echo(f"[PLSQL] Ingestion complete: added {total_nodes} nodes, {total_edges} edges")


@cli.command(name="plsql-full")
@click.option("--path", default=".", help="Path to PL/SQL project or file.")
@click.option(
    "--output",
    default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Path to save analysis results (JSON).",
)
@database_option
def plsql_full(
    path: str,
    output: Optional[str],
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """PL/SQL analysis: ingest PL/SQL only, run cross-package analysis, export graph."""
    click.echo("Starting PL/SQL Analysis (Ingest + Analyze)")
    click.echo("=" * 70)
    click.echo(f"Project path: {os.path.abspath(path)}")

    p = Path(path)
    root_path: str
    target_file: Optional[str] = None
    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(f"[ERROR] Path '{path}' not found.", err=True)
        return

    # Build graph client
    db_config = get_database_config(
        database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
    )
    if not GraphClientBuilder.validate_connection(db_config):
        click.echo(
            f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
            err=True,
        )
    graph_client = GraphClientBuilder.get_client(db_config)

    # Ingestion (PL/SQL only)
    loader = PlsqlLoader(root_path=root_path, target_file=target_file)
    for nodes, edges in loader.ingest():
        if nodes:
            graph_client.add_nodes(nodes)
        if edges:
            graph_client.add_edges(edges)

    initial_nodes = graph_client.get_node_count()
    initial_edges = graph_client.get_edge_count()
    click.echo(f"Ingestion complete: {initial_nodes} nodes, {initial_edges} edges")

    # Cross-package analysis (generic)
    from metazcode.sdk.analysis.cross_package_analyzer import CrossPackageAnalyzer

    analyzer = CrossPackageAnalyzer(graph_client)
    analysis_results = analyzer.analyze()

    final_nodes = graph_client.get_node_count()
    final_edges = graph_client.get_edge_count()
    new_edges = final_edges - initial_edges
    click.echo(f"Analysis complete: +{new_edges} cross-package edges")

    # Export graph (JSON)
    enhanced_graph_path = Path("enhanced_graph_plsql.json")
    try:
        graph = graph_client.get_graph()
        if hasattr(graph, 'is_multigraph'):
            graph_data = nx.node_link_data(graph, edges="links")
        else:
            all_nodes = graph_client.get_all_nodes()
            nodes_data = [node.to_dict() for node in all_nodes]
            connection = graph_client.get_graph()
            cursor = connection.cursor()
            cursor.execute(
                """
                    MATCH (source)-[r]->(target)
                    RETURN source.id as source_id, target.id as target_id, 
                           type(r) as relation_type, properties(r) as properties
                """
            )
            edge_results = cursor.fetchall()
            edges_data = []
            for source_id, target_id, relation_type, properties in edge_results:
                edges_data.append({
                    "source": source_id,
                    "target": target_id,
                    "relation": relation_type,
                    "properties": properties if properties else {}
                })
            graph_data = {
                "nodes": nodes_data,
                "links": edges_data,
                "metadata": {
                    "node_count": len(nodes_data),
                    "edge_count": len(edges_data),
                    "graph_type": "memgraph"
                }
            }
    except Exception as e:
        click.echo(f"Warning: Could not export graph data: {e}")
        graph_data = {"error": str(e), "nodes": [], "links": []}

    def default_serializer(o):
        if isinstance(o, (set, tuple)):
            return list(o)
        if hasattr(o, "to_dict"):
            return o.to_dict()
        try:
            return str(o)
        except TypeError:
            return repr(o)

    with open(enhanced_graph_path, "w") as f:
        json.dump(graph_data, f, indent=2, default=default_serializer)
    click.echo(f"Enhanced graph: {enhanced_graph_path.resolve()}")

    if output:
        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(analysis_results, f, indent=2, default=str)
        click.echo(f"Analysis results: {out_path.resolve()}")
