from heapq import *
import heapq


class MyHeap(object):
    # order:
    # True:maxHeap, ascending, low to high
    #   False:minHeap, descending, high to low
    def __init__(self, n, key=lambda x: x, order=True):
        self.key = key
        self.n = n
        self._data = []
        self.order = order

    def push(self, item):
        if self.order:
            if len(self._data) < self.n:
                self.maxpush((self.key(item), item))
            else:
                heapq._heappushpop_max(self._data, (self.key(item), item))
        else:
            if len(self._data) < self.n:
                heapq.heappush(self._data, (self.key(item), item))
            else:
                heapq.heappushpop(self._data, (self.key(item), item))

    def maxpush(self, item):
        self._data.append(item)
        heapq._siftdown_max(self._data, 0, len(self._data) - 1)

    def collect(self):
        if self.order:
            return [heapq._heappushpop_max(self._data, None)[1] for i in range(len(self._data))]
        else:
            return [heappop(self._data)[1] for i in range(len(self._data))]


if __name__ == '__main__':
    print "----simple data----"
    data = [1, 3, 5, 7, 9, 2, 4, 6, 8, 0]
    print "original data", data, "\n"

    n = 4
    order = True
    ascending = MyHeap(n, lambda x: x, order)
    print ("topByKey(%d, %s, %s)" % (4, "lambda x: x", order))
    for i in data:
        ascending.push(i)

    ascending_result = ascending.collect()
    print "result: ", ascending_result[::-1]
    # for e in reversed(ascending_result):
    # print e

    order = False
    print ("topByKey(%d, %s, %s)" % (4, "lambda x: x", order))
    descending = MyHeap(n, lambda x: x, order)
    for i in data:
        descending.push(i)
    descending_result = descending.collect()

    print "result: ", descending_result[::-1]
    # for e in reversed(descending_result):
    #     print e

    print
    print "----complex data----"
    pairdata = [(0, 1), (1, 3), (2, 5), (3, 7), (4, 9), (5, 2), (6, 4), (7, 6), (8, 8), (9, 0)]
    print "original data", pairdata, "\n"

    order = True
    print ("topByKey(%d, %s, %s)" % (4, "lambda x: x[1]", order))
    ascending = MyHeap(n, lambda x: x[1], order)
    for i in pairdata:
        ascending.push(i)

    ascending_result = ascending.collect()
    print ascending_result[::-1]
    # for e in reversed(ascending_result):
    #     print e

    order = False
    print ("topByKey(%d, %s, %s)" % (4, "lambda x: x[1]", order))
    descending = MyHeap(n, lambda x: x[1], order)
    for i in pairdata:
        descending.push(i)
    descending_result = descending.collect()

    print descending_result[::-1]
    # for e in reversed(descending_result):
    #     print e