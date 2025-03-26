from pyspark.sql import SparkSession

# Configuration Neo4j
NEO4J_URI =  "bolt://localhost:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "adminadmin"
from pyspark.sql import SparkSession


# Create a Spark session and connect it to Neo4j
spark = (
    SparkSession.builder.config("neo4j.url", NEO4J_URI)
    .config("neo4j.authentication.basic.username", NEO4J_USERNAME)
    .config("neo4j.authentication.basic.password", NEO4J_PASSWORD)
    .config("neo4j.database", "neo4j")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

# Lecture des données depuis Neo4j
query = """
MATCH (m:Movie)
RETURN m.title AS title, m.released AS released, m.tagline AS tagline
"""
movies_df = (
    spark.read.format("org.neo4j.spark.DataSource").option("query", query).load()
)

# Affichage des données
print("Liste des films depuis Neo4j :")
movies_df.show(truncate=False)

spark.stop()
