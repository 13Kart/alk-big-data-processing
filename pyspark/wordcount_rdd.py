from pyspark.sql import SparkSession

with SparkSession.builder.config('spark.default.parallelism', '1').getOrCreate() as spark:
    text_file_rdd = spark.sparkContext.textFile("../pan-tadeusz.txt")
    counts_rdd = text_file_rdd\
        .map(lambda line: line.strip())\
        .flatMap(lambda line: line.split())\
        .map(lambda word: "".join(i for i in word if i.isalpha()))\
        .filter(lambda word: word)\
        .map(lambda word: (word.lower(), 1))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda word_count_pair: f"{word_count_pair[0]}\t{word_count_pair[1]}")
    counts_rdd.saveAsTextFile("../output")
