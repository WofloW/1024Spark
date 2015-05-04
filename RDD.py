from itertools import *
from collections import defaultdict
from operator import add
import gevent
from gevent.event import Event
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
            #todo portable_hash -> default hash
            return nonNegativeMod(hash(key), self.numPartitions)

#not in use
class RangePartitioner(Partitioner):

    def __init__(self):
        Partitioner.__init__(self)
        self.ascending = True

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

#1!
class ShuffleDependency(Dependencies):

    def __init__(self, rdd, partitioner, serializer=None):
        Dependencies.__init__(self, rdd)
        self.partitioner = partitioner
        self.shuffleId = rdd.context().newShuffleId()
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
        self.id = self.sc.newRddId()

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

    def union(self, other):
        return Union(self, other)

    def join(self, other):
        return Join(self, other)

    def crossProduct(self, other):
        return CrossProduct(self, other)

    def groupByKey(self, partitioner):
        return GroupByKey(self, partitioner)

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

class PairRDD(RDD):

    def __init__(self, oneParent=None, sc=None, deps=None):
        RDD.__init__(oneParent, sc, deps)



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

#2!
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
class GroupByKey(RDD):

    def __init__(self, parent, partitioner):
        RDD.__init__(self, None, parent.context(), [ShuffleDependency(parent, partitioner)])
        self.result = {}
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
        buckets = defaultdict(list)
        for (k, v) in self.firstParent().rdd.iterator():
            pass

'''
    Spark
'''
class SparkContext(object):

    def __init__(self):
        self.dagScheduler = DAGScheduler(sc = self)
        self.nextShuffleId = 0
        self.nextRddId = 0

    def newShuffleId(self):
        id = self.nextShuffleId
        self.nextShuffleId += 1
        return id

    def newRddId(self):
        id = self.nextRddId
        self.nextRddId += 1
        return id

    #1!
    def parallelize(self, data, numSlices = 1):
        return Parallelize(data, numSlices, self, None)

    #1!
    def textFile(self, path, minPartitions = 1):
        return TextFile(path, minPartitions, self, None)

    def runJob(self, rdd, func, partitions):
        self.dagScheduler.runJob(rdd, func, partitions)

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
        #print self.partitions

    def compute(self, split, context):
        for r in iter(self.getPartitions()[split]):
            yield r


'''
    Scheduler
'''
class ActiveJob():

    def __init__(self, jobId, finalStage, func, partitions, listener = None, properties = None):
        self.numPartitions = len(partitions)
        self.finished = False * self.numPartitions
        self.numFinished = 0

class Stage():

    def __init__(self, id, rdd, numTasks, shuffleDep, parents, jobId):
        self.id = id
        self.rdd = rdd
        self.numTasks = numTasks
        self.shuffleDep = shuffleDep
        self.parents = parents
        self.jobId = jobId
        if shuffleDep:
            self.isShuffleMap = True
        else:
            self.isShuffleMap = False
        self.numPartitions  = len(rdd.partitions)
        self.outputLocs = []
        self.numAvailableOutputs = 0
        self.jobIds = set()
        self.resultOfJob = None
        self.pendingTasks = []
        self.nextAttemptId = 0
        #self.info = StageInfo(self)

    def newAttemptId(self):
        id = nextAttemptId
        nextAttemptId += 1
        return id

    def attempId(self):
        return self.nextAttemptId

    def isAvailable(self):
        if not self.isShuffleMap:
            return True
        else:
            return self.numAvailableOutputs == self.numPartitions

    def addOutputLoc(self, partition, status):
        if self.outputLocs[partition]:
            self.outputLocs[partition].insert(0, status)
        else:
            numAvailableOutputs += 1

    def removeOutputLoc(self, partition, bmAddress):
        prevList = self.outputLocs[partition]
        newList = [v for v in prevList if v.location != bmAddress]
        self.outputLocs[partition] = newList
        if prevList and not newList:
            numAvailableOutputs -= 1
    
    def removeOutputsOnExecutor(execId):
        pass

class DAGScheduler():

    def __init__(self, sc = None, taskScheduler = None, mapOutputTracker = None):
        self.sc = sc
        self.taskScheduler = taskScheduler
        self.mapOutputTracker = mapOutputTracker

        self.nextStageId = 0
        self.nextJobId = 0

        self.jobIdToStageIds = {}
        self.stageIdToStage = {}
        self.shuffleToMapStage = {}
        self.jobIdToActiveJob = {}

        self.waitingStages = set()
        self.runningStages = set()
        self.failedStages = set()
        self.activeJobs = set()

    def newJobId(self):
        id = self.nextJobId
        self.nextJobId += 1
        return id

    def newStageId(self):
        id = self.nextStageId
        self.nextStageId += 1
        return id

    def getShuffleMapStage(self, shuffleDep, jobId):
        if self.shuffleToMapStage.has_key(shuffleDep.shuffleId):
            return self.shuffleToMapStage[shuffleDep.shuffleId]
        else:
            stage = self.newOrUsedStage(shuffleDep.rdd, len(shuffleDep.rdd.partitions), shuffleDep, jobId)
            self.shuffleToMapStage[shuffleDep.shuffleId] = stage
            return stage

    def newStage(self, rdd, numTasks, shuffleDep, jobId):
        id = self.newStageId()
        stage = Stage(id, rdd, numTasks, shuffleDep, self.getParentStages(rdd, jobId), jobId)
        self.stageIdToStage[id] = stage
        self.updateJobIdStageIdMaps(jobId, stage)
        return stage

    def newOrUsedStage(self, rdd, numTasks, shuffleDep, jobId):
        stage = self.newStage(rdd, numTasks, shuffleDep, jobId)
        #if self.mapOutputTracker.containsShuffle(shuffleDep.shuffleId):
        #    serLocs = self.mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
        #    locs = MapOutputTracker.deserializeMapStatuses(serLocs)
        #    for i in xrange(locs):
        #        stage.outputLocs[i] = locs[i]
        #    stage.numAvailableOutputs = len([l for l in locs if l])
        #else:
        #    self.mapOutputTracker.registerShuffle(shuffleDep.shuffleId, len(rdd.partitions))
        return stage

    def updateJobIdStageIdMaps(self, jobId, stage):
        def updateJobIdStageIdMapsList(stages):
            if stages:
                s = stages[0]
                s.jobIds.add(jobId)
                if self.jobIdToStageIds.has_key(jobId):
                    self.jobIdToStageIds[jobId].add(s.id)
                else:
                    self.jobIdToStageIds[jobId] = set([s.id])
                parents = self.getParentStages(s.rdd, jobId)
                parentsWithoutThisJobId = [p for p in parents if jobId in p.jobIds]
                updateJobIdStageIdMapsList(parentsWithoutThisJobId.extend(stages[1:]))
        updateJobIdStageIdMapsList([stage])
    
    def getParentStages(self, rdd, jobId):
        parents = set()
        visited = set()
        def visit(r):
            if r not in visited:
                visited.add(r)
                for dep in r.getDependencies():
                    if issubclass(dep.__class__, ShuffleDependency):
                        parents.add(self.getShuffleMapStage(dep, jobId))
                    else:
                        visit(dep.rdd)
        visit(rdd)
        return list(parents)

    def activeJobForStage(self, stage):
        print stage.jobIds
        jobsThatUseStage = stage.jobIds
        for j in jobsThatUseStage:
            if self.jobIdToActiveJob.has_key(j):
                return j
            else:
                return None

    def submitStage(self, stage):
        jobId = self.activeJobForStage(stage)
        if jobId != None:
            if stage not in self.waitingStages and stage not in self.runningStages and stage not in self.failedStages:
                missing = sorted(self.getMissingParentStages(stage), key=lambda x: x.id)
                if not missing:
                    print "Submitting stage" + str(stage.id) + " (" + str(stage.rdd.id) + "), which has no missing parents"
                    self.submitMissingTasks(stage, jobId)
                else:
                    for parent in missing:
                        print "Looking for missing parent stage" + str(parent.id)
                        self.submitStage(parent)
                    self.waitingStages.add(stage)
        else:
            print "No active job for stage" + str(stage.id)

    def submitMissingTasks(self, stage, jobId):
        stage.pendingTasks = []
        properties = None
        if self.jobIdToActiveJob.has_key(jobId):
            #todo properties
            #properties = self.jobIdToActiveJob[stage.jobId].properties
            pass
        self.runningStages.add(stage)
        #listenerBus.post(SparkListenerStageSubmitted(stage.info, properties))
        print "Start running stage" + str(stage.id)


    def getMissingParentStages(self, stage):
        missing = set()
        visited = set()
        def visit(rdd):
            if rdd not in visited:
                #print rdd
                print "(" + str(rdd.id) + ") visited"
                visited.add(rdd)
                for dep in rdd.getDependencies():
                    if issubclass(dep.__class__, NarrowDependency):
                        visit(dep.rdd)
                    elif issubclass(dep.__class__, ShuffleDependency):
                        mapStage = self.getShuffleMapStage(dep, stage.jobId)
                        print "Stage (" + str(mapStage.id) + ") will do the shuffle"
                        #todo
                        #if not mapStage.isAvailable():
                        missing.add(mapStage)
        visit(stage.rdd)
        return list(missing)

    #def submitJob(self, rdd, func, partitions):
    #    maxPartitions = len(rdd.partitions)
    #    for p in partitions:
    #        if p >= maxPartitions or p < 0:
    #            return None
    #    jobId = self.newJobId()


    #def runJob(self, rdd, func, partitions):
    #    waiter = self.submitJob(rdd, func, partitions)

    def handleJobSubmitted(self, jobId, finalRDD, func, partitions):
        finalStage = self.newStage(finalRDD, len(partitions), None, jobId)
        if finalStage:
            job = ActiveJob(jobId, finalStage, func, partitions)
            self.jobIdToActiveJob[jobId] = job
            self.activeJobs.add(job)
            finalStage.resultOfJob = job
            #listenerBus.post(SparkListenerJobStart(job.jobId, jobIdToStageIds(jobId).toArray, properties))
            self.submitStage(finalStage)
        #Only after we finished running we can execute this
        #self.submitWaitingStages()

    def submitWaitingStages(self):
        waitingStagesCopy = list(self.waitingStages)
        self.waitingStages.clear()
        for stage in sorted(waitingStagesCopy, key=lambda x: x.jobId):
          self.submitStage(stage)

    def run(self, job):
        pass


'''
    Task Related
'''
class Task():

    def __init__(self, stageId, partitionId):
        self.stageId = stageId
        self.partitionId = partitionId
        self.context = None

    def run(self, taskAttemptId, attemptNumber):
        pass

    def runTask(self, context):
        pass

class TaskMetrics():

    def __init(self):
        self.hostname = ''

class TaskContext():

    def __init__(self, stageId, partitionId, attemptId, runningLocally = False, taskMetrics = None):
        self.stageId = stageId
        self.partitionId = partitionId
        self.attemptId = attemptId
        self.completed = False
    
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

class WorkerManager():

    def __init__(self, execId, host, port):
        self.execId = execId
        self.host = host
        self.port = port

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

    #doWordCount("wordcount.txt")
    #doPageRank("pagerank.txt")

    lines = spark.textFile("wordcount.txt", 4)
    print lines
    print lines.id
    words = lines.flatMap(lambda line: line.split(" "))
    print words
    print words.id
    wordDict = words.map(lambda word: (word, 1))
    print wordDict
    print wordDict.id
    groups = wordDict.groupByKey(HashPartitioner(4))
    print groups
    print groups.id
    groupsMap = groups.flatMap(lambda x: x)
    print groupsMap
    print groupsMap.id
    g1 = groupsMap.groupByKey(HashPartitioner(4))
    print g1
    print g1.id
    g2 = g1.map(lambda x: x)
    print g2
    print g2.id
    #counts = wordDict.reduceByKey(lambda a, b: a + b)

    dag = DAGScheduler(spark)
    #finalStage = dag.newStage(g2, 0, None, 0)
    #print "Submit final stage (" + str(finalStage.id) + ") create by final RDD (" + str(g2.id) + ")"
    dag.handleJobSubmitted(0, g2, None, range(4))
    #print dag.jobIdToStageIds
    #print dag.stageIdToStage
    #print dag.shuffleToMapStage
    #print dag.waitingStages
    #print dag.runningStages
    #print dag.failedStages
    #print dag.activeJobs