import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

with SparkSession.builder.config('spark.default.parallelism', '1').getOrCreate() as spark:
    text_file_df = spark.read.text(sys.argv[1])
    counts_df = text_file_df \
        .select(f.explode(f.split("value", " ")).alias("word")) \
        .withColumn("real_words", f.regexp_extract("word", "(\\p{L}+)", 1)) \
        .drop("words") \
        .where(f.col("real_words") != "") \
        .select(f.lower(f.col("real_words")).alias("real_words"))\
        .groupBy("real_words") \
        .count()
    # here repartition(1) doesn't make sense since we have already parallelism set to 1
    counts_df.repartition(1).write.mode("overwrite").csv(sys.argv[2])
