from pyspark.sql import SparkSession
from prettytable import PrettyTable

# Initialize Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read input file
input_file = "README.md"
text_rdd = spark.sparkContext.textFile(input_file)

# Word count logic
word_counts = (
    text_rdd.flatMap(lambda line: line.split())  # Split lines into words
    .map(lambda word: (word.lower(), 1))         # Map each word to (word, 1) and convert to lowercase
    .reduceByKey(lambda a, b: a + b)             # Reduce by key (word)
    .sortBy(lambda x: x[1], ascending=False)     # Sort by count in descending order
)

# Create a PrettyTable for clean output
table = PrettyTable()
table.field_names = ["Word", "Count"]

for word, count in word_counts.collect():
    table.add_row([word, count])

# Print the table
print(table)

# Stop Spark session
spark.stop()