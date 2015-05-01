import StringIO
import pickle

import cloudpickle
import gevent
import zerorpc

class Worker(object):

    def __init__(self):
        #gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print "[Contoller]"
            gevent.sleep(1)

    def hello(self, objstr, partitionId):
        input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(input)
        f = unpickler.load()
        return str(f.collect(partitionId))

        return "ACK"

print 'Worker1'
s = zerorpc.Server(Worker())
s.bind("tcp://0.0.0.0:4241")
s.run()
