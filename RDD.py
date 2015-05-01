from itertools import *
from collections import defaultdict
from operator import add
import re

'''
    Util
'''
def nonNegativeMod(x ,mod):  
   rawMod = x % mod
   if rawMod < 0:
       return rawMod + mod
   else:
       return rawMod

#1!
class Partition():

    def __init__(self):
        self.index = 0

    #abstract
    def hashCode(self):
        return self.index

'''
    Partitioner
'''
#1!
class Partitioner():

    def __init__(self):
        self.numPartitions = 0

    #abstract
    def getPartition(sef, key):
        pass

#1!
class HashPartitioner(Partitioner):

    def __init__(self, partitions):
        Partitioner.__init__(self)
        self.numPartitions = partitions

    def getPartition(self, key):
            #todo portable_hash
            return nonNegativeMod(hash(key), self.numPartitions)

#not in use
class RangePartitioner(Partitioner):

    def __init__(self):
        Partitioner.__init__(self)
        self.ascending = true

    def getPartition(self, key):
        pass

'''
    Dependencies
'''
#1!
class Dependencies():

    def __init__(self, rdd):
        self.rdd = rdd

#1!
class NarrowDependency(Dependencies):

    def __init__(self, rdd):
        Dependencies.__init__(self, rdd)

    #abstract
    def getParents(self, partitionId):
        pass

#1!
class OneToOneDependency(NarrowDependency):

    def __init__(self, rdd):
        NarrowDependency.__init__(self, rdd)

    def getParents(self, partitionId):
        return [partitionId]

#not in use
class RangeDependency(NarrowDependency):

    def __init__(self, rdd, inStart, outStart, length):
        NarrowDependency.__init__(self, rdd)

    #todo not tested
    def getParents(self, partitionId):
        if partitionId >= outStart and partitionId < outStart + length:
            return [partitionId - outStart + inStart]

#not yet complete
class ShuffleDependency(Dependencies):

    def __init__(self, rdd, partitioner, serializer=None):
        Dependencies.__init__(self, rdd)
        self.partitioner = partitioner
        #todo
        self.shuffleId = 0
        self.serializer = serializer

'''
    RDD
'''
class RDD(object):

    def __init__(self, oneParent=None, sc=None, deps=None):
        #init
        self.deps = []
        self.sc = None
        if oneParent:
            self.deps = [OneToOneDependency(oneParent)]
            self.sc = oneParent.context()
        if sc:
            self.sc = sc
        if deps:
            self.deps = deps
        #optional
        self.partitions = []
        self.partitioner = None
        #will be overwritten when we're checkpointed
        self.dependencies_ = None
        self.partitions_ = None 

    #abstract
    #@param split: Partition
    #@param context: TaskContext
    def compute(self, split, context):
        pass

    #abstract
    def getPartitions(self):
        return self.partitions

    #1!
    def getDependencies(self):
        return self.deps

    #abstract
    def getPreferredLocations(split):
        pass

    #1!
    def iterator(self, split, context):
        #for now we don't have a persist
        #we may have checkpoint or persist in the future
        return self.compute(split, context)

    #1!
    def context(self):
        return self.sc

    #1!
    def firstParent(self):
        return self.getDependencies()[0].rdd

    #---realization---

    #1!
    def filter(self, f):
        return Filter(f, self)

    #1!
    def map(self, f):
        return Map(f, self)

    #1!
    def flatMap(self, f):
        return FlatMap(f, self)

    #1!
    def reduceByKey(self, f):
        return ReduceByKey(f, self)

    #---not yet completed---

    def mapValues(self, f):
        return MapValues(self, f)

    def union(self, resource):
        return Union(self, resource)

    def join(self, resource):
        return Join(self, resource)

    def crossProduct(self, resource):
        return CrossProduct(self, resource)

    def groupByKey(self, numPartitions=None, partitioner=None):
        return GroupByKey(self, numPartitions, partitioner)

    #temp solution
    def collect(self, split = None, context = None):
        result = []
        for i in self.compute(split, context):
            result.append(i)
        return result
    
    def count(self):
        return len(self.collect())

    def reduce(self, f):
        result = 0
        for i in self.compute():
            result = f(result, i)
        return result

class Sample(RDD):

    def __init__(self, parent, fraction):
        RDD.__init__(self, oneParent = parent)
        self.fraction = fraction

    def compute(self):
        pass

#---Narrow Dependency---

#1!
class Filter(RDD):
    
    def __init__(self, func, parent = None, context = None, dependencies = None):
        RDD.__init__(self, oneParent = parent, sc = context, deps = dependencies)
        self.func = func
        self.partitioner = parent.partitioner

    def getPartitions(self):
        return self.firstParent().getPartitions()

    def compute(self, split, context):
        for r in ifilter(self.func, self.firstParent().iterator(split, context)):
            yield r

#1!
class Map(RDD):

    def __init__(self, func, parent = None, context = None, dependencies = None):
        RDD.__init__(self, oneParent = parent, sc = context, deps = dependencies)
        self.func = func
        self.partitioner = parent.partitioner

    def getPartitions(self):
        return self.firstParent().getPartitions()

    def compute(self, split, context):
        for r in imap(self.func, self.firstParent().iterator(split, context)):
            yield r

#1!
class FlatMap(RDD):

    def __init__(self, func, parent = None, context = None, dependencies = None):
        RDD.__init__(self, oneParent = parent, sc = context, deps = dependencies)
        self.func = func
        self.partitioner = parent.partitioner

    def getPartitions(self):
        return self.firstParent().getPartitions()

    def compute(self, split, context):
        for i in imap(self.func, self.firstParent().iterator(split, context)):
            for j in i:
                yield j

#1!
class ReduceByKey(RDD):

    def __init__(self, func, parent = None, context = None, dependencies = None):
        RDD.__init__(self, oneParent = parent, sc = context, deps = dependencies)
        self.func = func
        self.partitioner = parent.partitioner

    def getPartitions(self):
        return self.firstParent().getPartitions()

    def compute(self, split, context):
        result = {}

        for i in self.firstParent().iterator(split, context):
            if result.has_key(i[0]):
                result[i[0]] = self.func(result[i[0]], i[1])
            else:
                result[i[0]] = i[1]
        for r in result.iteritems():
            yield r

#---not yet completed---

class MapValues(RDD):
    
    def __init__(self, parent, func):
        RDD.__init__(self, oneParent = parent)
        self.func = func
        
    def compute(self):
        for r in self.firstParent().iterator():
            yield (r[0], self.func(r[1]))

    def iterator(self):
        return self.compute()

#todo
class Union(RDD):

    def __init__(self, parent, resource):
        RDD.__init__(self)
        self.deps.append(OneToOneDependency(parent))
        self.deps.append(OneToOneDependency(resource))

    def compute(self):
        for r in chain(self.firstParent().iterator(), self.deps[1].compute()):
            yield r

#---Wide Dependency---

class Join(RDD):

    def __init__(self, parent, resource, numPartitions=None, partitioner=None):
        RDD.__init__(self)
        self.getDependencies().append(parent)
        self.getDependencies().append(resource)

    def compute(self):
        for i in izip(self.firstParent().iterator(), self.deps[1].compute()):
            yield (i[0][0], [i[0][1], i[1][1]])

    def iterator(self):
        pass

class CrossProduct(RDD):

    def __init__(self, parent, resource):
        RDD.__init__(self)
        self.deps.append(parent)
        self.deps.append(resource)

    def compute(self):
        for r in product(self.firstParent().iterator(), self.deps[1].compute()):
            yield r

    def iterator(self):
        pass

class GroupByKey(RDD):

    def __init__(self, parent, numPartitions=None, partitioner=None):
        RDD.__init__(self)
        self.deps.append(ShuffleDependency(parent, partitioner))
        self.result = {}
        if partitioner:
            self.partitioner = partitioner

    def getPartitioner(self):
        return self.partitioner
    
    def compute(self):
        for i in self.firstParent().iterator():
            if self.result.has_key(i[0]):
                self.result[i[0]].append(i[1])
            else:
                self.result[i[0]] = [i[1]]
        for r in self.result.iteritems():
            yield r

    def iterator(self):
        pass

class PartitionBy(RDD):

    def __init__(self, parent, partitioner):
        RDD.__init__(self, oneParent = parent)
        self.partitioner = partitioner
        self.partitions = []

    def compute(self):
        buckets = defaultdict(list)
        for (k, v) in self.firstParent().rdd.iterator():
            pass

'''
    Spark
'''
class SparkContext(object):

    #1!
    def parallelize(self, data, numSlices = 1):
        return Parallelize(data, numSlices, self, None)

    #1!
    def textFile(self, path, minPartitions = 1):
        return TextFile(path, minPartitions, self, None)

#1!
class Parallelize(RDD):

    def __init__(self, data, numSlices = None, context = None, dependencies = None):
        RDD.__init__(self, oneParent = None, sc = context, deps = dependencies)
        self.data = data
        self.partitioner = None

    def compute(self, split, context):
        for r in iter(self.data):
            yield r

class TextFile(RDD):

    def __init__(self, path, minPartitions = None, context = None, dependencies = None):
        RDD.__init__(self, oneParent = None, sc = context, deps = dependencies)
        #print self.context()
        self.partitioner = None
        f = open(path)
        data = f.readlines()
        f.close()
        bucketSize = len(data) // minPartitions
        for i in range(minPartitions):
            for j in range(bucketSize):
                self.partitions.append(data[i*(j+1)].split())
        print self.partitions

    def compute(self, split, context):
        for r in iter(self.getPartitions()[split]):
            yield r

'''
    TaskContext
'''
class TaskContext():

    def __init__(self, stageId, partitionId):
        self.stageId = stageId
        self.partitionId = partitionId

    

'''
    Test cases
'''
def doWordCount(path):
    print "Start word counting"
    lines = spark.textFile(path, 4)
    words = lines.flatMap(lambda line: line.split(" "))
    wordDict = words.map(lambda word: (word, 1))
    counts = wordDict.reduceByKey(lambda a, b: a + b)
    print counts.collect(1)

def doPageRank(path):

    def computeContribs(urls, rank):
        """Calculates URL contributions to the rank of other URLs."""
        num_urls = len(urls)
        for url in urls:
            yield (url, rank / num_urls)

    def parseNeighbors(urls):
        """Parses a urls pair string into urls pair."""
        parts = re.split(r'\s+', urls)
        return parts[0], parts[1]

    print "Start page ranking"
    lines = spark.textFile(path)
    links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(10):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

if __name__ == '__main__':
    spark = SparkContext()
    #data = [1,2,3,4,5]
    #RDDA = spark.parallelize(data).map(lambda x: x + 1).filter(lambda x: x > 3)
    #RDDB = spark.parallelize(data).map(lambda x: x + 4).filter(lambda x: x > 8)
    #print RDDA.collect()
    #print RDDB.collect()
    #RDDAB = RDDA.union(RDDB)
    #print RDDAB.collect()
    #print RDDAB.reduce(lambda a, b: a + b)
    #print RDDAB.flatMap(lambda x: range(x)).collect()
    #print RDDA.crossProduct(RDDB).collect()
    
    #data2 = [('a', 1), ('b', 2), ('c', 3), ('d', 5), ('a', 6), ('d', 12)]
    #data3 = [('a', 6), ('b', 7), ('c', 10), ('d', 2)]
    #RDDC = spark.parallelize(data2)
    #RDDD = spark.parallelize(data3)
    #print RDDC.collect()
    #print RDDD.collect()
    #print RDDC.join(RDDD).collect()
    #print RDDC.groupByKey().collect()
    #print RDDC.reduceByKey(lambda a, b: a + b).collect()

    doWordCount("wordcount.txt")
    #doPageRank("pagerank.txt")