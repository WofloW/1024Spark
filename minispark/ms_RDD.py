from itertools import *
#import itertools
from operator import add
import re
from ms_Dependency import *
from ms_Partitioner import *

'''
    RDD
'''
class RDD(object):

    def __init__(self, oneParent=None, sc=None, deps=None):
        #init
        self.deps = []
        self.sc = None
        if sc:
            self.sc = sc
        if deps:
            self.deps = deps
        if oneParent:
            self.deps = [OneToOneDependency(oneParent)]
            self.sc = oneParent.context()
        #optional
        self.partitions = []
        self.partitioner = None
        #will be overwritten when we're checkpointed
        self.dependencies_ = None
        self.partitions_ = None
        self.id = self.sc.newRddId()

    #abstract
    #@param partitionId: Partition
    #@param context: TaskContext
    def compute(self, partitionId, context):
        pass

    #abstract
    def getPartitions(self):
        pass

    def getPartitioner(self):
        return self.partitioner

    #1!
    def getDependencies(self):
        return self.deps

    #1!
    def clearDependencies(self):
        self.deps = None

    #abstract
    def getPreferredLocations(partitionId):
        pass

    #1!
    def iterator(self, partitionId, context):
        #for now we don't have a persist
        #we may have checkpoint or persist in the future
        return self.compute(partitionId, context)

    #1!
    def context(self):
        return self.sc

    #1!
    def firstParent(self):
        return self.getDependencies()[0].rdd

    #---realization---

    #1!
    def filter(self, f):
        def func(context, pid, iter):
            for i in ifilter(f, iter):
                yield i
        return MappedRDD(func, self)

    #1!
    def map(self, f):
        def func(context, pid, iter):
            for i in imap(f, iter):
                yield i
        return MappedRDD(func, self)

    #1!
    def flatMap(self, f):
        def func(context, pid, iter):
            for i in imap(f, iter):
                for j in i:
                    yield j
        return MappedRDD(func, self)

    #1!
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

    #1!
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

    #---not yet completed---

    def mapValues(self, f):
        return MapValues(self, f)

    def union(self, other):
        return Union(self, other)

    def join(self, other):
        return Join(self, other)

    def crossProduct(self, other):
        return CrossProduct(self, other)

    def reduce(self, f):
        result = 0
        for i in self.compute():
            result = f(result, i)
        return result

#---Narrow Dependency---

class MappedRDD(RDD):

    def __init__(self, func, parent = None, context = None, dependencies = None, preservesPartitioning = False):
        RDD.__init__(self, oneParent = parent, sc = context, deps = dependencies)
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

    def __init__(self, parent, other):
        RDD.__init__(self)
        self.deps.append(OneToOneDependency(parent))
        self.deps.append(OneToOneDependency(other))

    def compute(self):
        for r in chain(self.firstParent().iterator(), self.deps[1].compute()):
            yield r

#---Wide Dependency---

class Join(RDD):

    def __init__(self, parent, other, numPartitions=None, partitioner=None):
        RDD.__init__(self)
        self.getDependencies().append(parent)
        self.getDependencies().append(other)

    def compute(self):
        for i in izip(self.firstParent().iterator(), self.deps[1].compute()):
            yield (i[0][0], [i[0][1], i[1][1]])

    def iterator(self):
        pass

class CrossProduct(RDD):

    def __init__(self, parent, other):
        RDD.__init__(self)
        self.deps.append(parent)
        self.deps.append(other)

    def compute(self):
        for r in product(self.firstParent().iterator(), self.deps[1].compute()):
            yield r

    def iterator(self):
        pass

class PartitionBy(RDD):

    def __init__(self, parent, partitioner):
        RDD.__init__(self, oneParent = parent)
        self.partitioner = partitioner
        self.partitions = []

    def compute(self):
        buckets = {}
        for (k, v) in self.firstParent().rdd.iterator():
            pass

'''
    Executor
'''
class Executer():

    def __init__(self, executorId, executorHostname, env, isLocal = False):
        print "Starting executor ID " + executorId + " on host " + executorHostname
        self.executorId = executorId
        self.executorHostname = executorHostname
        self.env = env
        self.isLocal = isLocal
        self.startDriverHeartbeater()

    def startDriverHeartbeater(self):
        pass

    def launchTask(self, context, taskId, attemptNumber, taskName, serializedTask):
        #todo
        #tr = TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName, serializedTask)
        #runningTasks.put(taskId, tr)
        #threadPool.execute(tr)
        pass

class TaskRunner():

    def __init__(self):
        pass
