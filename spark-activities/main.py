from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

input_file = "/opt/spark/python/README.md"
text_rdd = spark.sparkContext.textFile(input_file)

word_counts = (
    text_rdd.flatMap(lambda line: line.split())
    .map(lambda word: (word.lower(), 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
)

print(f"{'Word':<20}{'Count':<10}")
print("-" * 30)
for word, count in word_counts.collect():
    print(f"{word:<20}{count:<10}")

spark.stop()
