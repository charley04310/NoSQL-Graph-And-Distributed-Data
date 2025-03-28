

from pyspark.sql import SparkSession

NEO4J_URI = "bolt://44.213.127.135:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "inventions-hit-telephone"

spark = (
    SparkSession.builder.config("neo4j.url", NEO4J_URI)
    .config("neo4j.authentication.basic.username", NEO4J_USERNAME)
    .config("neo4j.authentication.basic.password", NEO4J_PASSWORD)
    .config("neo4j.database", "neo4j")
    .getOrCreate()
)


df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("query", """
        MATCH (:Client:FirstPartyFraudster)-[]-(txn:Transaction)-[]-(c:Client)
        WHERE NOT c:FirstPartyFraudster
        UNWIND labels(txn) AS transactionType
        RETURN transactionType, count(*) AS frequency
        ORDER BY frequency DESC
    """)
    .load()
)

df.show(truncate=False)

    
spark.stop()
