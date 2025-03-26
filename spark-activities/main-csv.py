from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list


def main():
    # Configuration S3
    s3_endpoint = "http://minio.minio.svc.cluster.local:9000"
    s3_access_key = "JAEfMra4rwN5kfAelupW"
    s3_secret_key = "h5CxbuAv93e49TOt2ign3Cncqdbatj1nNrw4UdLG"
    s3_bucket = "spark"
    s3_file_path = "users.csv"

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

    # Set S3 configurations
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3_endpoint)
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    # Construct S3 URI
    s3_uri = f"s3a://{s3_bucket}/{s3_file_path}"

    # Read CSV file from S3
    users_df = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(s3_uri)
    )

    # Process the data
    result = (
        users_df.filter(col("age") >= 25)
        .groupBy("city")
        .agg(collect_list("name").alias("names"))
        .select("city", "names")
    )

    # Collect and print the results
    result_collected = result.collect()
    for row in result_collected:
        city = row["city"]
        names = row["names"]
        print(f"Users in {city}: {', '.join(names)}")

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
