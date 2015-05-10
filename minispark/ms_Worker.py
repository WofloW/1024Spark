import StringIO
import pickle
import zerorpc
from itertools import *
from operator import add
import gevent
import re
import sys

from ms_RDD import *
from ms_SparkContext import *

class Worker(object):

    def __init__(self, port):
        #gevent.spawn(self.controller)
        self.port = port
        print "Worker start at " + str(self.port)

    def controller(self):
        while True:
            print "[Contoller]"
            gevent.sleep(1)

    def runResultTask(self, taskBinary, partitionId):
        print "running current result task for partition " + str(partitionId)
        task = pickle.loads(taskBinary)
        rdd = task[0]
        func = task[1]
        result = func(rdd.iterator(partitionId, None))
        print "result:"
        print result
        return (result, self.port)

    def runShuffleMapTask(self, taskBinary, partitionId):
        print "running current shufflemap task for partition " + str(partitionId)
        task = pickle.loads(taskBinary)
        rdd = task[0]
        dep = task[1]
        bucket = [[] for i in range(dep.partitioner.numPartitions)]
        for key, value in rdd.iterator(partitionId, None):
            targetPartitionId = dep.partitioner.getPartition(key)
            bucket[targetPartitionId].append((key, value))
        for i in range(dep.partitioner.numPartitions):
            print "Shuffled bucket for partition " + str(i) + ":"
            print bucket[i]
            rdd.context().shuffleManager.write(dep.shuffleId, partitionId, i, bucket[i])
        return (None, self.port)

if __name__ == '__main__':
    #default port
    port = sys.argv[1]
    s = zerorpc.Server(Worker(port))
    s.bind("tcp://0.0.0.0:" + port)
    s.run()
