from ms_ShuffleManager  import *
'''
    Dependency
'''
#1!
class Dependency():

    def __init__(self, rdd):
        self.rdd = rdd

#1!
class NarrowDependency(Dependency):

    def __init__(self, rdd):
        Dependency.__init__(self, rdd)

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
class ShuffleDependency(Dependency):

    def __init__(self, rdd, partitioner):
        Dependency.__init__(self, rdd)
        self.partitioner = partitioner
        self.shuffleId = rdd.context().newShuffleId()