from ms_SparkContext import *
def do(spark):
    lines = spark.textFile("wordcount.txt", 4)
    words = lines.flatMap(lambda line: line.split(" "))
    wordDict = words.map(lambda word: (word, 1))
    counts = wordDict.reduceByKey(HashPartitioner(5))
    print spark.collect(counts)