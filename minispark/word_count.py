import sys
from ms_SparkContext import *

'''
    Test Word Count
'''

print "Start word counting"
lines = spark.textFile(path, 4)
words = lines.flatMap(lambda line: line.split(" "))
wordDict = words.map(lambda word: (word, 1))
counts = wordDict.reduceByKey(HashPartitioner(5))
print spark.collect(counts)
