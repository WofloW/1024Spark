import zerorpc
import gevent
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

    def addWorker(self, port):
        self.availableWorkers.put(port)
            
    def runTask(self, task):
        workerResult = None
        workerPort = None
        if self.availableWorkers.qsize:
            worker = zerorpc.Client()
            port = self.availableWorkers.get()
            worker.connect("tcp://127.0.0.1:" + str(port))
            print "Connected to worker " + str(port)
            print "Run " + task.__class__.__name__ + " on partition" + str(task.partitionId)
            if issubclass(task.__class__, ResultTask):
                workerResult, workerPort = worker.runResultTask(task.taskBinary, task.partitionId)
            elif issubclass(task.__class__, ShuffleTask):
                workerResult, workerPort = worker.runShuffleTask(task.taskBinary, task.partitionId)
            self.handleTaskCompletion(task, workerPort, workerResult)
        else:
            print "No enough workers"
            
    def handleTaskCompletion(self, task, port, result):
        self.addWorker(port)
        self.sc.dagScheduler.handleTaskCompletion(task, port, result)

    def submitTasks(self, taskset):
        print "Taskset submitted for stage " + str(taskset.stageId)
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
    