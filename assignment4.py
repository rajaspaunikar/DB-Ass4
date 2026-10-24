import sys
import re
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


def _fmt(v):
    """Convert one cell value to the text that is printed."""
    if v is None:
        return "NULL"
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v)

def print_df(task_name, df, sort_cols):
    """Print a deterministically sorted DataFrame for autograding."""
    ordered = df.orderBy(*sort_cols)
    print(f"BEGIN_{task_name}")
    print("\t".join(ordered.columns))
    for row in ordered.toLocalIterator():
        print("\t".join(_fmt(v) for v in row))
    print(f"END_{task_name}")
    print()  # empty line between blocks, for readability


PLACES = [
    "delhi", "mumbai", "kolkata", "chennai", "bengaluru",
    "hyderabad", "lucknow", "srinagar", "kashmir", "punjab",
    "gujarat", "maharashtra", "kerala", "karnataka", "bengal",
    "assam", "bihar", "rajasthan", "pakistan", "china",
    "noida", "mathura",
]
PLACE_SET = set(PLACES)


def parse_article(content):
    """Return [(article_id_as_string, body)] or [] if the file is unusable."""
    try:
        obj = json.loads(content)
    except Exception:
        return []
    if not isinstance(obj, dict) or obj.get("article_id") is None:
        return []
    body = obj.get("article_body") or ""
    return [(str(obj["article_id"]), body)]


def places_in(body):
    """Distinct places mentioned in body, using the mandated tokenization."""
    return set(re.findall(r'[a-z]+', body.lower())) & PLACE_SET


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: spark-submit assignment4.py <data_dir>\n")
        sys.exit(1)
    data_path = sys.argv[1]

    spark = SparkSession.builder.appName("NewsroomGeoCoverage").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    sc._jsc.hadoopConfiguration().set(
        "mapreduce.input.fileinputformat.input.dir.recursive", "true")

    # (article_id, article_body), article_id kept as a string
    articles = (sc.wholeTextFiles(data_path)
                .filter(lambda kv: kv[0].endswith(".json") and "__MACOSX" not in kv[0])
                .flatMap(lambda kv: parse_article(kv[1]))
                .cache())

    # ---------------- Task 1: (place, article_id) pairs, RDD ----------------
    pairs_rdd = (articles
                 .flatMap(lambda a: [(p, a[0]) for p in places_in(a[1])])
                 .distinct()
                 .cache())
    total = pairs_rdd.count()
    first_rows = pairs_rdd.sortBy(lambda x: (x[0], x[1])).take(20) if total > 0 else []

    print("BEGIN_TASK1_RDD")
    print(total)
    for place, aid in first_rows:
        print(f"{place}\t{aid}")
    print("END_TASK1_RDD")
    print()

    # ---------------- Task 2: convert to DataFrame ----------------
    pair_schema = StructType([
        StructField("place", StringType(), False),
        StructField("article_id", StringType(), False),
    ])
    pairs_df = spark.createDataFrame(pairs_rdd, pair_schema).cache()

    print_df('TASK2_PAIRS',
             pairs_df.orderBy('place', 'article_id').limit(20),
             [F.col('place').asc(), F.col('article_id').asc()])

    # ---------------- Task 3: term frequency and dominant place ----------------
    article_schema = StructType([
        StructField("article_id", StringType(), False),
        StructField("article_body", StringType(), True),
    ])
    articles_df = spark.createDataFrame(articles, article_schema)

    occurrences_df = (articles_df
                      .select('article_id',
                              F.explode(F.split(F.lower(F.col('article_body')),
                                                '[^a-z]+')).alias('word'))
                      .filter(F.col('word').isin(PLACES))   # drops empty strings
                      .select('article_id', F.col('word').alias('place')))

    freq_df = (occurrences_df
               .groupBy('article_id', 'place')
               .agg(F.count(F.lit(1)).alias('term_frequency'))
               .select('article_id', 'place', 'term_frequency')
               .cache())

    print_df('TASK3_FREQUENCIES', freq_df,
             [F.col('article_id').asc(), F.col('place').asc()])

    w = Window.partitionBy('article_id')
    dominant_df = (freq_df
                   .withColumn('max_tf', F.max('term_frequency').over(w))
                   .filter(F.col('term_frequency') == F.col('max_tf'))
                   .select('article_id', 'place', 'term_frequency'))

    print_df('TASK3_DOMINANT', dominant_df,
             [F.col('article_id').asc(), F.col('place').asc()])

    # ---------------- Task 4: blind spots ----------------
    doc_freq_df = (pairs_df
                   .groupBy('place')
                   .agg(F.count('article_id').alias('doc_count'))
                   .cache())

    blind_df = (doc_freq_df
                .filter(F.col('doc_count') == 1)
                .join(pairs_df, on='place', how='inner')
                .select('place', 'article_id'))

    print_df('TASK4_BLIND_SPOTS', blind_df,
             [F.col('place').asc(), F.col('article_id').asc()])

    # ---------------- Task 5: Jaccard similarity ----------------
    left = pairs_df.select(F.col('place').alias('p1'), 'article_id')
    right = pairs_df.select(F.col('place').alias('p2'), 'article_id')

    cooc_df = (left.join(right, on='article_id', how='inner')
               .filter(F.col('p1') < F.col('p2'))
               .groupBy('p1', 'p2')
               .agg(F.count(F.lit(1)).alias('cooccurrence_count')))

    n1 = doc_freq_df.select(F.col('place').alias('p1'), F.col('doc_count').alias('n1'))
    n2 = doc_freq_df.select(F.col('place').alias('p2'), F.col('doc_count').alias('n2'))

    jaccard_df = (cooc_df
                  .join(n1, on='p1', how='inner')
                  .join(n2, on='p2', how='inner')
                  .withColumn('jaccard',
                              F.col('cooccurrence_count') /
                              (F.col('n1') + F.col('n2') - F.col('cooccurrence_count')))
                  .select('p1', 'p2', 'jaccard', 'cooccurrence_count')
                  .cache())

    top_sort = [F.col('jaccard').desc(), F.col('p1').asc(), F.col('p2').asc()]
    bottom_sort = [F.col('jaccard').asc(), F.col('p1').asc(), F.col('p2').asc()]

    print_df('TASK5_TOP5', jaccard_df.orderBy(*top_sort).limit(5), top_sort)
    print_df('TASK5_BOTTOM5', jaccard_df.orderBy(*bottom_sort).limit(5), bottom_sort)

    # ---------------- Task 6: most covered places ----------------
    t6_sort = [F.col('total_mentions').desc(), F.col('place').asc()]
    top_places_df = (freq_df
                     .groupBy('place')
                     .agg(F.sum('term_frequency').alias('total_mentions'),
                          F.count('article_id').alias('article_count'))
                     .select('place', 'total_mentions', 'article_count')
                     .orderBy(*t6_sort)
                     .limit(5))

    print_df('TASK6_TOP5', top_places_df, t6_sort)

    # ---------------- Task 7: crowded stories ----------------
    t7_sort = [F.col('distinct_place_count').desc(), F.col('article_id').asc()]
    crowded_df = (pairs_df
                  .groupBy('article_id')
                  .agg(F.count('place').alias('distinct_place_count'))
                  .select('article_id', 'distinct_place_count')
                  .orderBy(*t7_sort)
                  .limit(10))

    print_df('TASK7_TOP10', crowded_df, t7_sort)

    spark.stop()


if __name__ == "__main__":
    main()