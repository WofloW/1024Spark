import zerorpc
import gevent
import time
from gevent.queue import Queue
import subprocess
import socket

'''
    Task Related
'''
auto = True

class TaskScheduler():
    def __init__(self, sc=None):
        self.sc = sc
        self.dagScheduler = None
        self.activeWorkers = set()
        self.availableWorkers = Queue()
        f = open("slaves")
        self.new_worker_list = f.readlines()

    def setDAGScheduler(self, dagScheduler):
        self.dagScheduler = dagScheduler

    def registerWorker(self, ip_port):
        print "Worker registered on IP port ", ip_port
        self.availableWorkers.put(ip_port)

    def runTask(self, task):
        # print "Attempt to run the task"
        workerResult = None
        workerPort = None
        workerStatus = None
        if self.availableWorkers.qsize() > 0:
            ip_port = self.availableWorkers.get()
            print "Found available worker on " + ip_port
            try:
                worker = zerorpc.Client(timeout=1000000, heartbeat=10000000)
                worker.connect("tcp://" + ip_port)
                print "Connected to worker " + str(ip_port)
                print "Run " + task.__class__.__name__ + " on partition" + str(task.partitionId)
                if issubclass(task.__class__, ResultTask):
                    workerResult, workerStatus, worker_ip_port = worker.runResultTask(task.taskBinary, task.partitionId)
                elif issubclass(task.__class__, ShuffleTask):
                    workerResult, workerStatus, worker_ip_port = worker.runShuffleTask(task.taskBinary,
                                                                                       task.partitionId)
                if workerStatus == 'completed':
                    print "Task completed on worker " + str(ip_port)
                    self.handleTaskCompletion(task, worker_ip_port, workerResult)
                elif workerStatus == 'failed':
                    print "Task failed on worker " + str(ip_port) + ", attempt to run again!"
                    self.runTask(task)
            except:
                print "Connection problem on worker " + str(ip_port) + " detected, attempt to run again!"
                self.runTask(task)
        else:
            if auto and len(self.new_worker_list) > 0:
                new_worker_ipport = self.new_worker_list.pop()
                worker_ip = new_worker_ipport.split(":")[0]
                worker_port = new_worker_ipport.split(":")[1]
                # localip = socket.gethostbyname(socket.gethostname())
                localip = "127.0.0.1"
                master_ipport = localip+":4000"
                subprocess.Popen(["python", "pyremote.py", worker_ip, master_ipport, worker_port])
                print "Automaticly create a worker %s" % new_worker_ipport
            while self.availableWorkers.qsize() == 0:
                print "Partition " + str(task.partitionId) + " -> waiting workers"
                gevent.sleep(3)
            self.runTask(task)


    def handleTaskCompletion(self, task, ip_port, result):
        self.registerWorker(ip_port)
        self.sc.dagScheduler.handleTaskCompletion(task, ip_port, result)

    def submitTasks(self, taskset):
        # print "Task set submitted for stage " + str(taskset.stageId)
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
    