import networkx as nx
import matplotlib.pyplot as plt


def visualize_graph(graph: nx.DiGraph, output_path: str):
    """
    Generates and saves a visualization of the graph.
    """
    plt.figure(figsize=(20, 20))

    # Use a layout that spreads nodes out
    pos = nx.spring_layout(graph, k=0.5, iterations=50)

    # Color nodes by their type
    node_colors = []
    for node in graph.nodes(data=True):
        node_type = node[1].get("node_type", "unknown")
        if node_type == "pipeline":
            node_colors.append("skyblue")
        elif node_type == "operation":
            node_colors.append("lightgreen")
        elif node_type == "table":
            node_colors.append("salmon")
        elif node_type == "connection":
            node_colors.append("gold")
        else:
            node_colors.append("gray")

    nx.draw_networkx_nodes(graph, pos, node_color=node_colors, node_size=2000)

    # Draw edges
    nx.draw_networkx_edges(
        graph, pos, arrowstyle="->", arrowsize=20, edge_color="gray", width=1
    )

    # Draw labels
    labels = {
        node: f"{data.get('node_type', '')}\\n{data.get('name', '')}"
        for node, data in graph.nodes(data=True)
    }
    nx.draw_networkx_labels(graph, pos, labels=labels, font_size=8)

    plt.title("Codebase Knowledge Graph", size=20)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(output_path, format="PNG")
    plt.close()
