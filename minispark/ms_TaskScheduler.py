import zerorpc
import gevent
import time
from gevent.queue import Queue
'''
    Task Related
'''
class TaskScheduler():

    def __init__(self, sc = None):
        self.sc = sc
        self.dagScheduler = None
        self.activeWorkers = set()
        self.availableWorkers = Queue()

    def setDAGScheduler(self, dagScheduler):
        self.dagScheduler = dagScheduler

    def registerWorker(self, port):
        print "Worker registered on port " + str(port)
        self.availableWorkers.put(port)

    def runTask(self, task):
        #print "Attempt to run the task"
        workerResult = None
        workerPort = None
        workerStatus = None
        if self.availableWorkers.qsize() > 0:
            port = self.availableWorkers.get()
            print "Found available worker on " + str(port)
            try:
                worker = zerorpc.Client()
                worker.connect("tcp://127.0.0.1:" + str(port))
                #print "Connected to worker " + str(port)
                print "Run " + task.__class__.__name__ + " on partition" + str(task.partitionId)
                if issubclass(task.__class__, ResultTask):
                    workerResult, workerStatus, workerPort = worker.runResultTask(task.taskBinary, task.partitionId)
                elif issubclass(task.__class__, ShuffleTask):
                    workerResult, workerStatus, workerPort = worker.runShuffleTask(task.taskBinary, task.partitionId)
                if workerStatus == 'completed':
                    print "Task completed on worker " + str(workerPort)
                    self.handleTaskCompletion(task, workerPort, workerResult)
                elif workerStatus == 'failed':
                    print "Task failed on worker " + str(workerPort) + ", attempt to run again!"
                    self.runTask(task)
            except:
                print "Connection problem on worker " + str(port) + " detected, attempt to run again!"
                self.runTask(task)
        else:
            while self.availableWorkers.qsize() == 0:
                print "Partition " + str(task.partitionId) + " -> waiting workers"
                gevent.sleep(3)
            self.runTask(task)

            
    def handleTaskCompletion(self, task, port, result):
        self.registerWorker(port)
        self.sc.dagScheduler.handleTaskCompletion(task, port, result)

    def submitTasks(self, taskset):
        #print "Task set submitted for stage " + str(taskset.stageId)
        threads = [gevent.spawn(self.runTask, t) for t in taskset.tasks]
        gevent.joinall(threads)

class Task():

    def __init__(self, stageId, partitionId):
        self.stageId = stageId
        self.partitionId = partitionId

    def run(self, taskAttemptId, attemptNumber):
        pass

    def runTask(self, context):
        pass

class ResultTask(Task):

    def __init__(self, stageId, taskBinary, partitionId, outputId):
        Task.__init__(self, stageId, partitionId)
        self.taskBinary = taskBinary
        self.outputId = outputId

class ShuffleTask(Task):

    def __init__(self, stageId, taskBinary, partitionId):
        Task.__init__(self, stageId, partitionId)
        self.taskBinary = taskBinary

class TaskSet():

    def __init__(self, tasks, stageId, jobId):
        self.tasks = tasks
        self.stageId = stageId
        self.jobId = jobId
    