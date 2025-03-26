from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def main():
    # Configuration S3
    s3_endpoint = "http://minio.minio.svc.cluster.local:9000"
    s3_access_key = "JAEfMra4rwN5kfAelupW"
    s3_secret_key = "h5CxbuAv93e49TOt2ign3Cncqdbatj1nNrw4UdLG"
    s3_bucket = "spark"
    s3_users = "users.csv"
    s3_stackoverflow = "stackoverflow.csv"

    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("SimpleApp")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Set S3 configurations
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3_endpoint)
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    # Construct S3 URIs
    s3_uri = f"s3a://{s3_bucket}/{s3_users}"
    s3_stackoverflow_uri = f"s3a://{s3_bucket}/{s3_stackoverflow}"

    # Process users.csv
    print("\n=== Processing users.csv ===")
    users_df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(s3_uri)
    )

    result = (
        users_df.filter(col("age") >= 25)
        .groupBy("city")
        .agg(collect_list("name").alias("names"))
        .select("city", "names")
    )

    print("\nUsers grouped by city (age >= 25):")
    result.show(truncate=False)

    # Define schema for stackoverflow.csv
    schema = StructType(
        [
            StructField("postTypeId", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("acceptedAnswer", StringType(), True),
            StructField("parentId", IntegerType(), True),
            StructField("score", IntegerType(), True),
            StructField("tag", StringType(), True),
        ]
    )

    # Process stackoverflow.csv
    print("\n=== Processing stackoverflow.csv ===")
    stackoverflow_df = (
        spark.read.option("header", "false")
        .schema(schema)
        .csv(s3_stackoverflow_uri)
        .drop("acceptedAnswer")
    )

    print(f"\nTotal records in stackoverflow.csv: {stackoverflow_df.count()}")
    print("\nSchema of stackoverflow.csv:")
    stackoverflow_df.printSchema()

    print("\nFirst 5 records in stackoverflow.csv:")
    stackoverflow_df.show(5)

    print("\nCount of null values in specific columns:")
    print(f" - tag: {stackoverflow_df.filter(col('tag').isNull()).count()}")
    print(f" - parentId: {stackoverflow_df.filter(col('parentId').isNull()).count()}")

    # Register the DataFrame as a temporary SQL table
    stackoverflow_df.createOrReplaceTempView("stackoverflow")

    # Query posts with a score greater than 20
    print("\n=== Posts with score > 20 ===")
    high_score_posts_sql = spark.sql("SELECT * FROM stackoverflow WHERE score > 20")
    high_score_posts_sql.show(5)

    # Top 5 posts with the highest scores and non-null tags
    print("\n=== Top 5 posts with highest scores and non-null tags ===")
    top5ScoresWithTag = spark.sql("""
        SELECT id, score, tag
        FROM stackoverflow
        WHERE tag IS NOT NULL
        ORDER BY score DESC
        LIMIT 5
    """)
    top5ScoresWithTag.show()

    # Top 10 most popular tags
    print("\n=== Top 10 most popular tags ===")
    popularTags = spark.sql("""
        SELECT tag, COUNT(*) as frequency
        FROM stackoverflow
        WHERE tag IS NOT NULL
        GROUP BY tag
        ORDER BY frequency DESC
        LIMIT 10
    """)
    popularTags.show()

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
