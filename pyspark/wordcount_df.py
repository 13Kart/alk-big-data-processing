from pyspark.sql import SparkSession
import pyspark.sql.functions as f

with SparkSession.builder.config('spark.default.parallelism', '1').getOrCreate() as spark:
    text_file_df = spark.read.text("../pan-tadeusz.txt")
    counts_df = text_file_df \
        .select(f.explode(f.split("value", " ")).alias("word")) \
        .withColumn("real_words", f.regexp_extract("word", "(\\p{L}+)", 1)) \
        .drop("words") \
        .where(f.col("real_words") != "") \
        .select(f.lower(f.col("real_words")).alias("real_words"))\
        .groupBy("real_words") \
        .count()
    counts_df.write.mode("overwrite").csv("../output")
