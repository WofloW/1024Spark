from ms_RDD import *
from ms_TaskScheduler import *
from ms_DAGScheduler import *
from ms_ShuffleManager import *

'''
    Spark
'''
class Context():

    def __init__(self):
        self.nextShuffleId = 0
        self.nextRddId = 0
        self.shuffleManager = ShuffleManager()

    def newShuffleId(self):
        id = self.nextShuffleId
        self.nextShuffleId += 1
        return id

    def newRddId(self):
        id = self.nextRddId
        self.nextRddId += 1
        return id

class SparkContext(Context):

    def __init__(self):
        Context.__init__(self)
        self.serverHandle = None
        self.taskScheduler = TaskScheduler(sc = self)
        self.dagScheduler = DAGScheduler(sc = self, taskScheduler = self.taskScheduler)
        
    def createServerHandle(self, port):
        self.serverHandle = zerorpc.Server(self)
        self.serverHandle.bind("tcp://127.0.0.1:" + str(port))
        self.serverHandle.run()

    def registerWorker(self, port):
        self.taskScheduler.registerWorker(port)

    #1!
    def parallelize(self, data, numSlices = 1):
        return Parallelize(data, numSlices, self, None)

    #1!
    def textFile(self, path, minPartitions = 1):
        return TextFile(path, minPartitions, Context(), None)

    def runJob(self, rdd, func, partitions):
        return self.dagScheduler.runJob(rdd, func, partitions)

    #temp solution
    def collect(self, rdd, partitionId = None, context = None):
        return self.runJob(rdd, lambda iter: list(iter), rdd.getPartitions())
    
    def count(self):
        return len(self.collect())

#1!
class Parallelize(RDD):

    def __init__(self, data, numSlices = None, context = None, dependencies = None):
        RDD.__init__(self, oneParent = None, sc = context, deps = dependencies)
        self.data = data
        self.partitioner = None

    def compute(self, partitionId, context):
        for r in iter(self.data):
            yield r

class TextFile(RDD):

    def __init__(self, path, minPartitions = None, context = None, dependencies = None):
        RDD.__init__(self, oneParent = None, sc = context, deps = dependencies)
        #print self.context()
        self.path = path
        self.minPartitions = minPartitions
        self.partitioner = None
        self.partitions
        self.data = []

    def getData(self):
        f = open(self.path)
        data = f.readlines()
        f.close()
        bucketSize = len(data) // self.minPartitions
        for i in range(self.minPartitions):
            for j in range(bucketSize):
                self.data.append(data[i*(j+1)].split())
        return self.data

    def getPartitions(self):
        return range(self.minPartitions)

    def compute(self, partitionId, context):
        for r in iter(self.getData()[partitionId]):
            yield r