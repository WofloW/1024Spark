import pickle
import zerorpc
from itertools import *
import gevent
import sys
import time

from ms_RDD import *
from ms_SparkContext import *

class Worker(object):

    def __init__(self, port):
        #gevent.spawn(self.controller)
        self.port = port
        self.serverHandle = None
        self.clientHandle = None

    def createServerHandle(self):
        print "Start running"
        self.serverHandle = zerorpc.Server(self)
        self.serverHandle.bind("tcp://127.0.0.1:" + str(self.port))
        self.serverHandle.run()

    def announceDriver(self, port):
        print "Connect to driver"
        driver = zerorpc.Client()
        driver.connect("tcp://127.0.0.1:" + str(port))
        driver.registerWorker(self.port)
        driver.close()
        print "Finished registering"

    def runResultTask(self, taskBinary, partitionId):
        status = 'running'
        result = None
        try:
            print "running current result task for partition " + str(partitionId)
            time.sleep(3)
            task = pickle.loads(taskBinary)
            rdd = task[0]
            func = task[1]
            print "Working for rdd (" + str(rdd.id) + ")"
            result = func(rdd.iterator(partitionId, None))
            print "result:"
            print result
            status = 'completed'
        except:
            status = 'failed'
        return (result, status, self.port)

    def runShuffleTask(self, taskBinary, partitionId):
        status = 'running'
        result = None
        try:
            print "running current shufflemap task for partition " + str(partitionId)
            time.sleep(3)
            task = pickle.loads(taskBinary)
            rdd = task[0]
            dep = task[1]
            print "Working for rdd (" + str(rdd.id) + ")"
            bucket = [[] for i in range(dep.partitioner.numPartitions)]
            for key, value in rdd.iterator(partitionId, None):
                targetPartitionId = dep.partitioner.getPartition(key)
                bucket[targetPartitionId].append((key, value))
            for i in range(dep.partitioner.numPartitions):
                print "Shuffled bucket for partition " + str(i) + ":"
                print bucket[i]
                rdd.context().shuffleManager.write(dep.shuffleId, partitionId, i, bucket[i])
            status = 'completed'
        except:
            status = 'failed'
        return (None, status, self.port)

if __name__ == '__main__':
    #default port
    port = sys.argv[1]
    worker = Worker(port)
    gevent.joinall([gevent.spawn(worker.createServerHandle), gevent.spawn(worker.announceDriver, 4001)])
