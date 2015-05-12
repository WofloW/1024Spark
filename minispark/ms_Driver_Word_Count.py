import sys
from ms_SparkContext import *

'''
    Test Word Count
'''
def doWordCount(spark, path):
    print "Start word counting"
    lines = spark.textFile(path, 4)
    words = lines.flatMap(lambda line: line.split(" "))
    wordDict = words.map(lambda word: (word, 1))
    counts = wordDict.reduceByKey(HashPartitioner(5))
    print spark.collect(counts)

if __name__ == '__main__':
    port = sys.argv[1]
    path = sys.argv[2]
    spark = SparkContext()
    gevent.joinall([
        gevent.spawn(spark.createServerHandle, port), 
        gevent.spawn(doWordCount, spark, path)
    ])