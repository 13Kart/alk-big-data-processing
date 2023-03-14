from pyspark.sql import SparkSession

with SparkSession.builder\
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0")\
        .getOrCreate() as spark:
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", "w1_live")
    query = "SELECT * FROM `alk-big-data-processing.w1_live.big_lake_users`"
    users = spark.read.format("bigquery").load(query)
    users.show()
