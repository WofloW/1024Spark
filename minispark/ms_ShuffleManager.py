import pickle
import os

'''
    Shuffle Manager
    Stores the intermediate files on local disk (hdfs)
'''


class ShuffleManager():
    def __init__(self):
        self.shuffleIds = set()

    def registerShuffle(self, shuffleId):
        self.shuffleIds.add(shuffleId)

    def write(self, shuffleId, partitionId, targetPartitionId, data):
        file = open("shuffle_" + str(shuffleId) + "_" + str(partitionId) + "_" + str(targetPartitionId), "w")
        file.write(pickle.dumps(data))
        file.close()

    def read(self, shuffleId, partitionId, targetPartitionId):
        result = None
        file = open("shuffle_" + str(shuffleId) + "_" + str(partitionId) + "_" + str(targetPartitionId))
        result = pickle.loads(file.read())
        file.close()
        return result