from ms_RDD import *
from ms_TaskScheduler import *
from ms_DAGScheduler import *
from ms_ShuffleManager import *
from ms_Dependency import *
import socket

from split_file import *

'''
    Spark Context
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
        self.taskScheduler = TaskScheduler(sc=self)
        self.dagScheduler = DAGScheduler(sc=self, taskScheduler=self.taskScheduler)
        # self.createServerHandle()

    def createServerHandle(self, port):
        self.serverHandle = zerorpc.Server(self)
        localip = socket.gethostbyname(socket.gethostname())
        self.serverHandle.bind("tcp://%s:%s" % (localip, str(port)))
        self.serverHandle.run()

    def registerWorker(self, port):
        self.taskScheduler.registerWorker(port)

    def runJob(self, rdd, func, partitions):
        return self.dagScheduler.runJob(rdd, func, partitions)

    # ---APIs---

    def textFile(self, path, minPartitions=1):
        return TextFile(path, minPartitions, Context())

    def collect(self, rdd):
        return self.runJob(rdd, lambda iter: list(iter), rdd.getPartitions())

    def topByKey(self, rdd, top, key=lambda x: x, reverse=False):
        return sorted(self.collect(rdd.heapByPartitions(top, key, reverse)), key=key, reverse=reverse)[:top]

    def count(self, rdd):
        return len(self.collect(rdd))


class TextFile(RDD):
    def __init__(self, path, minPartitions=1, context=Context()):
        RDD.__init__(self, oneParent=None, sc=context, deps=None)
        # print self.context()
        self.path = path
        self.minPartitions = minPartitions
        self.partitioner = None
        self.fileReader = MyTextReader(path, minPartitions)

    def getData(self, partitionId):
        return self.fileReader.line_iterator(partitionId)

    def getPartitions(self):
        return range(self.minPartitions)

    def compute(self, partitionId, context):
        for r in iter(self.getData(partitionId)):
            if r:
                yield r