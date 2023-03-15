from pyspark.sql import SparkSession

with SparkSession.builder.getOrCreate() as spark:
    users = spark.read.csv("gs://alk-big-data-processing-w1/users")
    users.show()
    users.createOrReplaceTempView("users")
    users_vol_2 = spark.sql("""
    SELECT *
    FROM users
    WHERE version = 2
    """)
