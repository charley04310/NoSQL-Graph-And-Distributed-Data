from neo4j import GraphDatabase
import os 

NEO4J_URI=os.getenv("NEO4J_URI", "neo4j://localhost:7687")
NEO4J_USERNAME=os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD=os.getenv("NEO4J_PASSWORD", "adminadmin")

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]  # Stocker les résultats dans une liste

# Connexion à Neo4j
conn = Neo4jConnection(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)

# Requête pour récupérer tous les films
query = """
MATCH (m:Movie)
RETURN m.title AS title, m.released AS released, m.tagline AS tagline
"""

# Exécution et stockage des résultats
result = conn.query(query)  # Converti en liste immédiatement

# Affichage des films
print("Liste des films :")
for record in result:
    print(f"- {record['title']} ({record['released']}): {record['tagline']}")

# Fermeture de la connexion
conn.close()