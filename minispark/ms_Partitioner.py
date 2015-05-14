'''
    Util
'''


def nonNegativeMod(x, mod):
    rawMod = x % mod
    if rawMod < 0:
        return rawMod + mod
    else:
        return rawMod


'''
    Partitioner
'''
# 1!
class Partitioner():
    def __init__(self):
        self.numPartitions = 0

    #abstract
    def getPartition(sef, key):
        pass


#1!
class HashPartitioner(Partitioner):
    def __init__(self, numPartitions=1):
        Partitioner.__init__(self)
        self.numPartitions = numPartitions

    def getPartition(self, key):
        #todo portable_hash -> default hash
        return nonNegativeMod(hash(key), self.numPartitions)


#not in use
class RangePartitioner(Partitioner):
    def __init__(self):
        Partitioner.__init__(self)
        self.ascending = true

    def getPartition(self, key):
        pass
