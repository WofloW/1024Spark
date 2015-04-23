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
        return imap(self.func, self.parent.iterator())

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
        return ifilter(self.func, self.parent.iterator())

class Union(RDD):

    def __init__(self, parent, resource):
        RDD.__init__(self)
        self.parent = parent
        self.resource = resource

    def iterator(self):
        return chain(self.parent.iterator(), self.resource.iterator())

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
        return product(self.parent.iterator(), self.resource.iterator())

'''
    Spark
'''
class Spark(object):

    def dictData(self, data):
        return DictData(data)

    def textData(self, data):
        return TextData(data)

    def textFile(self, path):
        return TextFile(data)

class DictData(RDD):

    def __init__(self, data):
        RDD.__init__(self)
        self.elements = data

    def iterator(self):
        return iter(self.elements.iteritems())

class TextData(RDD):

    def __init__(self, data):
        RDD.__init__(self)
        self.elements = data

    def iterator(self):
        return iter(self.elements)

class TextFile(RDD):

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
    RDDA = spark.textData(data).map(lambda x: x+1).filter(lambda x: x > 3)
    RDDB = spark.textData(data).map(lambda x: x+4).filter(lambda x: x > 8)
    print RDDA.collect()
    print RDDB.collect()
    RDDAB = RDDA.union(RDDB)
    #print RDDAB.collect()
    #print RDDAB.reduce(lambda a, b: a + b)
    #print RDDAB.flatMap(lambda x: range(x)).collect()
    print RDDA.crossProduct(RDDB).collect()
    
    data2 = {'a':1, 'b':2, 'c':3, 'd':5}
    data3 = {'a':6, 'b':7, 'c':10, 'd':2}
    RDDC = spark.dictData(data2)
    RDDD = spark.dictData(data3)
    #print RDDC.collect()
    #print RDDD.collect()
    #print RDDC.join(RDDD).collect()