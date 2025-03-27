from pyspark.sql import SparkSession

# Création de la session Spark
spark = (
    SparkSession.builder.appName("KafkaSparkConsumer")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
    .getOrCreate()
)

# Configuration du topic et du broker
kafka_bootstrap_servers = "my-cluster-kafka-bootstrap:9092"
kafka_topic = "inputstream-test-newapplication"

# Lecture du flux Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
)

# Extraction des valeurs du message
messages = df.selectExpr("CAST(value AS STRING) as message")

# Affichage des messages en streaming
query = messages.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
