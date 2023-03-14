from pyspark.sql import SparkSession

with SparkSession.builder\
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0")\
        .getOrCreate() as spark:
    users = spark.read.format("bigquery").load("alk-big-data-processing.w1_live.simple_big_lake")
    users.show()
