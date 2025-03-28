from neo4j import GraphDatabase
import os

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://44.213.127.135")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "inventions-hit-telephone")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")


class Neo4jConnection:
    def __init__(self, uri, user, password, database="neo4j"):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._database = database

    def close(self):
        self._driver.close()

    def query(self, query, parameters=None):
        with self._driver.session(database=self._database) as session:
            result = session.run(query, parameters)
            return [record for record in result]


# Connexion à Neo4j
conn = Neo4jConnection(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE)

# Liste des requêtes
queries = {
    "Nodes Count": "MATCH (n) RETURN count(n) AS TotalNodes;",
    "Relationships Count": "MATCH ()-[r]->() RETURN count(r) AS TotalRelationships;",
    "Node Labels and their Count": """
        MATCH (n) RETURN labels(n) AS NodeLabel, count(n) AS Count ORDER BY Count DESC;
    """,
    "Relationship Types and their Count": """
        MATCH ()-[r]->() RETURN type(r) AS RelationshipType, count(r) AS Count ORDER BY Count DESC;
    """,
}

# Exécution et affichage des résultats
for title, query in queries.items():
    print(f"\n{'-' * 50}")
    print(f"{title}")
    print(f"{'-' * 50}")
    try:
        result = conn.query(query)
        for record in result:
            print(record)
    except Exception as e:
        print(f"Erreur lors de l'exécution de la requête '{title}': {e}")

# Fermeture de la connexion
conn.close()
