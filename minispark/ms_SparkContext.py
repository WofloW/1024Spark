from ms_RDD import *
from ms_TaskScheduler import *
from ms_DAGScheduler import *
from ms_ShuffleManager import *
from split_file import *

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
        self.taskScheduler = TaskScheduler(sc = self)
        self.dagScheduler = DAGScheduler(sc = self, taskScheduler = self.taskScheduler)
        
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
        self.partitioner = None
        # data_dir = os.path.dirname(path)
        data_dir = os.getcwd()
        input_file = path
        self.minPartitions = minPartitions
        self.split_infos, self.file_info = split_file(data_dir, minPartitions, input_file)
        self.lines = None

    def getPartitions(self):
        return range(self.minPartitions)

    def compute(self, split, context):
        if not self.lines:
            self.lines = read_input(self.split_infos[split], split, len(self.split_infos), self.file_info).split("\n")
        for r in iter(self.lines):
            yield r
