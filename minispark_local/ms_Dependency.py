'''
    Dependency
'''


class Dependency():
    def __init__(self, rdd):
        self.rdd = rdd


class NarrowDependency(Dependency):
    def __init__(self, rdd):
        Dependency.__init__(self, rdd)

    # abstract
    def getParents(self, partitionId):
        pass


class OneToOneDependency(NarrowDependency):
    def __init__(self, rdd):
        NarrowDependency.__init__(self, rdd)

    def getParents(self, partitionId):
        return [partitionId]


# not in use
class RangeDependency(NarrowDependency):
    def __init__(self, rdd, inStart, outStart, length):
        NarrowDependency.__init__(self, rdd)

    #todo not in use
    def getParents(self, partitionId):
        if partitionId >= outStart and partitionId < outStart + length:
            return [partitionId - outStart + inStart]


class ShuffleDependency(Dependency):
    def __init__(self, rdd, partitioner):
        Dependency.__init__(self, rdd)
        self.partitioner = partitioner
        self.shuffleId = rdd.context().newShuffleId()