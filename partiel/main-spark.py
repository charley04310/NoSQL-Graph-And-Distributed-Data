from pyspark.sql import SparkSession

# Neo4j connection details
NEO4J_URI = "bolt://44.213.127.135:7687"  # Ensure the port is included
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "inventions-hit-telephone"

spark = (
    SparkSession.builder.config("neo4j.url", NEO4J_URI)
    .config("neo4j.authentication.basic.username", NEO4J_USERNAME)
    .config("neo4j.authentication.basic.password", NEO4J_PASSWORD)
    .config("neo4j.database", "neo4j")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

queries = {
    "Nodes Count": "MATCH (n) RETURN count(n) AS TotalNodes",
    "Relationships Count": "MATCH ()-[r]->() RETURN count(r) AS TotalRelationships",
    "Node Labels and their Count": """
        MATCH (n) RETURN labels(n) AS NodeLabel, count(n) AS Count ORDER BY Count DESC
    """,
    "Relationship Types and their Count": """
        MATCH ()-[r]->() RETURN type(r) AS RelationshipType, count(r) AS Count ORDER BY Count DESC
    """,
}

for title, query in queries.items():
    print(f"\n{'-' * 50}")
    print(f"{title}")
    print(f"{'-' * 50}")
    df = spark.read.format("org.neo4j.spark.DataSource").option("query", query
    ).load()
    df.show()
    
spark.stop()
