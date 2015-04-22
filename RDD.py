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

    def filter(self, f):
        return Filter(self, f)

    def union(self, resource):
        return Union(self, resource)

    def join(self, resource):
        return Join(self, resource)

    def collect(self):
        result = []
        for i in self.iterator():
            if i != None:
                result.append(i)
        return result
    
    def count(self):
        return len(self.collect())

    def reduce(self, f):
        result = 0
        for i in self.iterator():
            if i != None:
                result = f(result, i)
        return result


class Map(RDD):

    def __init__(self, parent, func):
        RDD.__init__(self)
        self.parent = parent
        self.func = func
    
    def dependencies(self):
        return self.parent

    def iterator(self):
        print "Class: " + self.__class__.__name__
        for i in self.parent.iterator():
            yield self.func(i)

class Filter(RDD):
    
    def __init__(self, parent, func):
        RDD.__init__(self);
        self.parent = parent
        self.func = func

    def dependencies(self):
        return self.parent

    def iterator(self):
        print "Class: " + self.__class__.__name__
        for i in self.parent.iterator():
            if self.func(i):
                yield i
            else:
                yield None

class Union(RDD):

    def __init__(self, parent, resource):
        RDD.__init__(self)
        self.parent = parent
        self.resource = resource

    def iterator(self):
        for i in self.parent.iterator():
            yield i
        for i in self.resource.iterator():
            yield i

class Join(RDD):

    def __init__(self, parent, resource):
        RDD.__init__(self)
        self.parent = parent
        self.resource = resource

    def iterator(self):
        for i in self.parent.iterator():
            yield (i[0], [i[1], self.resource.iterator().next()[1]])

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
        return self.elements.iteritems()

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
    #data = [1,2,3,4,5]
    #RDDA = spark.textData(data).map(lambda x: x+1).filter(lambda x: x > 3)
    #RDDB = spark.textData(data).map(lambda x: x+4).filter(lambda x: x > 8)
    #print RDDA.collect()
    #print RDDB.collect()
    #RDDAB = RDDA.union(RDDB)
    #print RDDAB.collect()
    #print RDDAB.reduce(lambda a, b: a + b)
    
    data2 = {'a':1, 'b':2, 'c':3, 'd':5}
    data3 = {'a':6, 'b':7, 'c':10, 'd':2}
    RDDC = spark.dictData(data2)
    RDDD = spark.dictData(data3)
    #print RDDC.collect()
    #print RDDD.collect()
    print RDDC.join(RDDD).collect()