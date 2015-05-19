from itertools import *
from ms_Dependency import *
from ms_Partitioner import *
from topByKey import *

'''
    RDD
'''


class RDD(object):
    def __init__(self, oneParent=None, sc=None, deps=None):
        # init
        self.deps = []
        self.sc = None
        if sc:
            self.sc = sc
        if deps:
            self.deps = deps
        if oneParent:
            self.deps = [OneToOneDependency(oneParent)]
            self.sc = oneParent.context()
        self.id = self.sc.newRddId()
        print self.__class__.__name__ + " created: (" + str(self.id) + ")"
        #optional
        self.partitions = None
        self.partitioner = None

    # abstract
    def compute(self, partitionId, context):
        pass

    #abstract
    def getPartitions(self):
        pass

    def getPartitioner(self):
        return self.partitioner

    def getDependencies(self):
        return self.deps

    def iterator(self, partitionId, context):
        #for now we don't have a persist
        #we may have checkpoint or persist in the future
        return self.compute(partitionId, context)

    def context(self):
        return self.sc

    def firstParent(self):
        return self.getDependencies()[0].rdd

    #---APIs---

    def filter(self, f):
        def func(context, pid, iter):
            for i in ifilter(f, iter):
                yield i

        return MappedRDD(func, self)

    def map(self, f):
        def func(context, pid, iter):
            for i in imap(f, iter):
                yield i

        return MappedRDD(func, self)

    def flatMap(self, f):
        def func(context, pid, iter):
            for i in imap(f, iter):
                for j in i:
                    yield j

        return MappedRDD(func, self)

    def mapValues(self, f):
        def func(context, pid, iter):
            for i in iter:
                yield (i[0], f(i[1]))
        return MappedRDD(func, self)

    def heapByPartitions(self, numHeap, key=lambda x: x, reverse=False):
        return HeapByPartitions(self, numHeap, key, reverse)

    def sortByKey(self, key=lambda x: x, reverse=False):
        return SortByKey(self, key, reverse)

    def groupByKey(self, partitioner):
        def func(context, pid, iter):
            result = {}
            for i in iter:
                if result.has_key(i[0]):
                    result[i[0]].append(i[1])
                else:
                    result[i[0]] = [i[1]]
            for r in result.iteritems():
                yield r

        return ShuffledRDD(func, self, partitioner)

    def reduceByKey(self, partitioner):
        def func(context, pid, iter):
            result = {}
            for i in iter:
                if result.has_key(i[0]):
                    result[i[0]] += i[1]
                else:
                    result[i[0]] = i[1]
            for r in result.iteritems():
                yield r

        return ShuffledRDD(func, self, partitioner)

    def join(self, other, partitioner):
        return Join(self, other, partitioner)

        #---End of APIs---


# ---Narrow Dependency---

class MappedRDD(RDD):
    def __init__(self, func, parent=None, context=None, dependencies=None, preservesPartitioning=False):
        RDD.__init__(self, oneParent=parent, sc=context, deps=dependencies)
        self.func = func
        self.preservesPartitioning = preservesPartitioning

    def getPartitioner(self):
        if self.preservesPartitioning:
            return self.firstParent().getPartitioner()
        else:
            return None

    def getPartitions(self):
        return self.firstParent().getPartitions()

    def compute(self, partitionId, context):
        for r in self.func(context, partitionId, self.firstParent().iterator(partitionId, context)):
            yield r


#---Wide Dependency---

class ShuffledRDD(RDD):
    def __init__(self, func, parent, partitioner):
        RDD.__init__(self, None, parent.context(), [ShuffleDependency(parent, partitioner)])
        self.func = func
        self.partitioner = partitioner

    def getPartitions(self):
        return range(self.partitioner.numPartitions)

    def getData(self, partitionId):
        result = []
        for i in range(len(self.firstParent().getPartitions())):
            result.extend(self.context().shuffleManager.read(self.deps[0].shuffleId, i, partitionId))
        return result

    def compute(self, partitionId, context):
        print self.getData(partitionId)
        for r in self.func(context, partitionId, iter(self.getData(partitionId))):
            yield r


class Join(RDD):
    def __init__(self, parent, other, partitioner):
        RDD.__init__(self, None, parent.context(),
                     [ShuffleDependency(parent, partitioner), ShuffleDependency(other, partitioner)])
        self.partitioner = partitioner

    def getPartitions(self):
        return range(self.partitioner.numPartitions)

    def getParentData(self, partitionId):
        result = []
        for i in range(len(self.getDependencies()[0].rdd.getPartitions())):
            result.extend(self.context().shuffleManager.read(self.getDependencies()[0].shuffleId, i, partitionId))
        return result

    def getOtherData(self, partitionId):
        result = []
        for j in range(len(self.getDependencies()[1].rdd.getPartitions())):
            result.extend(self.context().shuffleManager.read(self.getDependencies()[1].shuffleId, j, partitionId))
        return result

    def compute(self, partitionId, context):
        #print self.getData(partitionId)
        for i in izip(self.getParentData(partitionId), self.getOtherData(partitionId)):
            yield (i[0][0], [i[0][1], i[1][1]])


class HeapByPartitions(RDD):
    def __init__(self, oneParent, numHeap, key=lambda x: x, reverse=False):
        RDD.__init__(self, oneParent)
        self.heaps = [MyHeap(numHeap, key, not reverse) for i in range(len(self.firstParent().getPartitions()))]

    def getPartitions(self):
        return self.firstParent().getPartitions()

    def compute(self, partitionId, context):
        for d in self.firstParent().iterator(partitionId, context):
            self.heaps[partitionId].push(d)
        for r in self.heaps[partitionId].collect():
            yield r

class SortByKey(RDD):
    def __init__(self, oneParent, key=lambda x: x, reverse=False):
        RDD.__init__(self, oneParent)
        self.sort_data = [[] for i in range(len(self.firstParent().getPartitions()))]
        self.key = key
        self.reverse = reverse

    def getPartitions(self):
        return self.firstParent().getPartitions()

    def compute(self, partitionId, context):
        for d in self.firstParent().iterator(partitionId, context):
            self.sort_data[partitionId].append(d)
        self.sort_data[partitionId] = sorted(self.sort_data[partitionId], key=self.key, reverse=self.reverse)
        for r in self.sort_data[partitionId]:
            yield r