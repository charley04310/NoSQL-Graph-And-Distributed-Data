from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

# MinIO configurations
s3_endpoint = "http://minio.minio.svc.cluster.local:9000"
s3_access_key = "JAEfMra4rwN5kfAelupW"
s3_secret_key = "h5CxbuAv93e49TOt2ign3Cncqdbatj1nNrw4UdLG"
s3_bucket = "spark"
s3_prefix = ""
s3_uri = f"s3a://{s3_bucket}/{s3_prefix}"

# Create Spark session
spark = (
    SparkSession.builder.appName("BucketMonitorStreamApp")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Monitor raw text files in MinIO (new arrivals only)
df = (
    spark.readStream.format("text")
    .option("path", s3_uri)
    .option("maxFilesPerTrigger", 1)
    .load()
    .withColumn("filename", input_file_name())
    .withColumn("detected_at", current_timestamp())
)

# Log new file arrivals
query = (
    df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()

spark.stop()
