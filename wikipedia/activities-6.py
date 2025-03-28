from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, udf, desc
import re
import time

start_time = time.time()
spark = (
    SparkSession.builder.appName("WikipediaAnalysis").master("local[*]").getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

print(f"\nThis is Spark Version: {spark.version}")

input_file = "./wikipedia.dat"
raw_data = sc.textFile(input_file)


class WikiPage:
    def __init__(self, title, text, is_redirect):
        self.title = title
        self.text = text
        self.is_redirect = is_redirect


def parse_wiki_page(page_string):
    title_match = re.search(r"<title>(.*?)</title>", page_string)
    text_match = re.search(r"<text>(.*?)</text>", page_string, re.DOTALL)

    title = title_match.group(1) if title_match else ""
    text = text_match.group(1) if text_match else ""
    is_redirect = "#REDIRECT" in text  # Vérifie si le texte contient "#REDIRECT"

    return WikiPage(title, text, is_redirect)


wiki_page_rdd = raw_data.map(parse_wiki_page)

wiki_page_df = wiki_page_rdd.map(
    lambda page: Row(title=page.title, text=page.text, is_redirect=page.is_redirect)
).toDF()

print("\n=== Aperçu des pages Wikipedia ===")
wiki_page_df.show(5, truncate=False)

print("\n=== Comptage des redirections ===")
wiki_page_df.groupBy("is_redirect").count().show()

# Fonction pour extraire les catégories
category_pattern = re.compile(r"\[\[Category:(.*?)\]\]")


def extract_categories(text):
    return category_pattern.findall(text) if text else []  # Retourne une liste


extract_categories_udf = udf(extract_categories)

wiki_with_categories = wiki_page_df.withColumn(
    "categories", extract_categories_udf(col("text"))
)

print("\n=== Titres et catégories ===")
wiki_with_categories.select("title", "categories").show(truncate=False)

exploded_df = wiki_with_categories.withColumn("category", explode(col("categories")))

print("\n=== Top 50 catégories les plus fréquentes ===")
exploded_df.groupBy("category").count().orderBy(desc("count")).show(50, truncate=False)

elapsed_time = time.time() - start_time
print(f"\nProgram execution time: {elapsed_time:.2f} seconds")
print(".......Program *****Completed***** Successfully.....!\n")

spark.stop()
