from itertools import *

'''
    RDD
'''
class RDD(object):

    def __init__(self):
        self.partitions = []

    def partitioner(self):
        pass

    def preferredLocations(self, p):
        pass

    def partitions(self):
        return self.partitions

    def dependencies(self):
        pass

    def iterator(self):
        pass

    def map(self, f):
        return Map(self, f)

    def flatMap(self, f):
        return FlatMap(self, f)

    def filter(self, f):
        return Filter(self, f)

    def union(self, resource):
        return Union(self, resource)

    def join(self, resource):
        return Join(self, resource)

    def crossProduct(self, resource):
        return CrossProduct(self, resource)

    def groupByKey(self):
        return GroupByKey(self)

    def reduceByKey(self, f):
        return ReduceByKey(self, f)

    def collect(self):
        result = []
        for i in self.iterator():
            result.append(i)
        return result
    
    def count(self):
        return len(self.collect())

    def reduce(self, f):
        result = 0
        for i in self.iterator():
            result = f(result, i)
        return result

class Sample(RDD):

    def __init__(self, parent, fraction):
        RDD.__init__(self)
        this.parent = parent
        this.fraction = fraction

    def iterator(self):
        pass

class Map(RDD):

    def __init__(self, parent, func):
        RDD.__init__(self)
        self.parent = parent
        self.func = func
    
    def dependencies(self):
        return self.parent

    def iterator(self):
        print "Class: " + self.__class__.__name__
        for r in imap(self.func, self.parent.iterator()):
            yield r

class FlatMap(RDD):

    def __init__(self, parent, func):
        RDD.__init__(self)
        self.parent = parent
        self.func = func
    
    def dependencies(self):
        return self.parent

    def iterator(self):
        print "Class: " + self.__class__.__name__
        for i in imap(self.func, self.parent.iterator()):
            for j in i:
                yield j

class Filter(RDD):
    
    def __init__(self, parent, func):
        RDD.__init__(self);
        self.parent = parent
        self.func = func

    def dependencies(self):
        return self.parent

    def iterator(self):
        print "Class: " + self.__class__.__name__
        for r in ifilter(self.func, self.parent.iterator()):
            yield r

class Union(RDD):

    def __init__(self, parent, resource):
        RDD.__init__(self)
        self.parent = parent
        self.resource = resource

    def iterator(self):
        for r in chain(self.parent.iterator(), self.resource.iterator()):
            yield r

class Join(RDD):

    def __init__(self, parent, resource):
        RDD.__init__(self)
        self.parent = parent
        self.resource = resource

    def iterator(self):
        for i in izip(self.parent.iterator(), self.resource.iterator()):
            yield (i[0][0], [i[0][1], i[1][1]])

class CrossProduct(RDD):

    def __init__(self, parent, resource):
        RDD.__init__(self)
        self.parent = parent
        self.resource = resource

    def iterator(self):
        for r in product(self.parent.iterator(), self.resource.iterator()):
            yield r

class GroupByKey(RDD):

    def __init__(self, parent):
        RDD.__init__(self)
        self.parent = parent
        self.result = {}
    
    def iterator(self):
        for i in self.parent.iterator():
            if self.result.has_key(i[0]):
                self.result[i[0]].append(i[1])
            else:
                self.result[i[0]] = [i[1]]
        for r in self.result.iteritems():
            yield r

class ReduceByKey(RDD):

    def __init__(self, parent, func):
        RDD.__init__(self)
        self.parent = parent
        self.func = func
        self.result = {}

    def iterator(self):
        for i in self.parent.iterator():
            if self.result.has_key(i[0]):
                self.result[i[0]] = self.func(self.result[i[0]], i[1])
            else:
                self.result[i[0]] = i[1]
        for r in self.result.iteritems():
            yield r

'''
    Spark
'''
class Spark(object):

    def loadData(self, data):
        return LoadData(data)

    def loadFile(self, path):
        return LoadFile(path)

class LoadData(RDD):

    def __init__(self, data):
        RDD.__init__(self)
        self.data = data

    def iterator(self):
        return iter(self.data)

class LoadFile(RDD):

    def __init__(self, path):
        RDD.__init__(self)
        f = open(path)
        data = f.readlines()
        f.close()
        self.elements = data

    def iterator(self):
        return iter(self.elements)

if __name__ == '__main__':
    spark = Spark()
    data = [1,2,3,4,5]
    RDDA = spark.loadData(data).map(lambda x: x + 1).filter(lambda x: x > 3)
    RDDB = spark.loadData(data).map(lambda x: x + 4).filter(lambda x: x > 8)
    #print RDDA.collect()
    #print RDDB.collect()
    RDDAB = RDDA.union(RDDB)
    #print RDDAB.collect()
    #print RDDAB.reduce(lambda a, b: a + b)
    #print RDDAB.flatMap(lambda x: range(x)).collect()
    #print RDDA.crossProduct(RDDB).collect()
    
    data2 = [('a', 1), ('b', 2), ('c', 3), ('d', 5), ('a', 6), ('d', 12)]
    data3 = [('a', 6), ('b', 7), ('c', 10), ('d', 2)]
    RDDC = spark.loadData(data2)
    RDDD = spark.loadData(data3)
    #print RDDC.collect()
    #print RDDD.collect()
    #print RDDC.join(RDDD).collect()
    #print RDDC.groupByKey().collect()
    #print RDDC.reduceByKey(lambda a, b: a + b).collect()

    counts = spark.loadFile("myfile").flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    print counts.collect()