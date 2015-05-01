import pickle
import StringIO

import zerorpc
import cloudpickle

from RDD import *

sc = SparkContext()
lines = sc.textFile("wordcount.txt", 4)
words = lines.flatMap(lambda line: line.split(" "))
wordDict = words.map(lambda word: (word, 1))
counts = wordDict.reduceByKey(lambda a, b: a + b)

output = StringIO.StringIO()
pickler = cloudpickle.CloudPickler(output)
pickler.dump(counts)
objstr = output.getvalue()

c1 = zerorpc.Client()
c1.connect("tcp://127.0.0.1:4241")
c2 = zerorpc.Client()
c2.connect("tcp://127.0.0.1:4242")
c3 = zerorpc.Client()
c3.connect("tcp://127.0.0.1:4243")
c4 = zerorpc.Client()
c4.connect("tcp://127.0.0.1:4244")

print c1.hello(objstr, 0)
print c2.hello(objstr, 1)
print c3.hello(objstr, 2)
print c4.hello(objstr, 3)
