from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, count
import re

spark = SparkSession.builder.appName("Wikipedia Data Processing with RDD").getOrCreate()
sc = spark.sparkContext

sc.setLogLevel("WARN")

input_file = "./wikipedia.dat"
text_rdd = sc.textFile(input_file)

title_regex = re.compile(r"<title>(.*?)</title>")
text_regex = re.compile(r"<text.*?>(.*?)</text>")
category_regex = re.compile(r"\[\[Category:(.*?)\]\]")
reference_regex = re.compile(r"<ref>(.*?)</ref>")
redirect_regex = re.compile(r"#REDIRECT \[\[(.*?)\]\]")


def extract_fields(line):
    title = title_regex.search(line)
    text = text_regex.search(line)
    category = category_regex.search(line)
    reference = reference_regex.search(line)
    redirect = redirect_regex.search(line)
    return Row(
        Title=title.group(1) if title else None,
        Text=text.group(1) if text else None,
        Category=category.group(1) if category else None,
        Reference=reference.group(1) if reference else None,
        Redirect=redirect.group(1) if redirect else None,
    )


structured_rdd = text_rdd.map(extract_fields)

processed_df = spark.createDataFrame(structured_rdd)

filtered_df = processed_df.filter(
    col("Title").isNotNull()
    | col("Text").isNotNull()
    | col("Category").isNotNull()
    | col("Reference").isNotNull()
    | col("Redirect").isNotNull()
)

print("\n=== Top 50 catégories les plus fréquentes ===")
top_categories = (
    filtered_df.filter(col("Category").isNotNull())
    .groupBy("Category")
    .agg(count("Category").alias("Frequency"))
    .orderBy(col("Frequency").desc())
    .limit(50)
)

top_categories.show(n=50, truncate=False)

spark.stop()
