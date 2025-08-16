from neo4j import GraphDatabase
import networkx as nx
import matplotlib.pyplot as plt

driver = GraphDatabase.driver("bolt://localhost:7687")
G = nx.DiGraph()

with driver.session() as session:
    result = session.run("MATCH (a)-[r]->(b) RETURN a, r, b")
    for record in result:
        a = record["a"]["id"]
        b = record["b"]["id"]
        G.add_edge(a, b)

nx.draw(G, with_labels=True)
plt.show()