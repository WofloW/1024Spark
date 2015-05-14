import zerorpc
# Thanks Kaiming for his pickler
import pickle, with_function

from ms_RDD import *
from ms_TaskScheduler import *

'''
    DAG Scheduler
    Analyzing dependencies graph, wrapping into stage and executing by stage
'''


class ActiveJob():
    def __init__(self, jobId, finalStage, func, partitions):
        self.numPartitions = len(partitions)
        self.finished = [False] * self.numPartitions
        self.numFinished = 0

        self.jobId = jobId
        self.finalStage = finalStage
        self.func = func
        self.partitions = partitions

        self.results = [[] for i in range(self.numPartitions)]

    def isFinished(self):
        return self.numFinished == self.numPartitions


class JobWaiter():
    def __init__(self, dagScheduler, jobId):
        self.dagScheduler = dagScheduler
        self.jobId = jobId

    def isFinished(self):
        return self.dagScheduler.jobIdToActiveJob[self.jobId].isFinished()

    def isCreated(self):
        return self.dagScheduler.jobIdToActiveJob.has_key(self.jobId)

    def awaitResult(self):
        while not self.isCreated():
            #print "Waiting for job creation..."
            gevent.sleep(1)
        while not self.isFinished():
            #print "Checking status..."
            gevent.sleep(1)

    def getResult(self):
        result = []
        for r in self.dagScheduler.jobIdToActiveJob[self.jobId].results:
            result.extend(r)
        return result


class Stage():
    def __init__(self, id, rdd, numTasks, shuffleDep, parents, jobId):
        self.id = id
        self.rdd = rdd
        self.numTasks = numTasks
        self.shuffleDep = shuffleDep
        self.parents = parents
        self.jobId = jobId
        if shuffleDep:
            self.isShuffle = True
        else:
            self.isShuffle = False
        self.numPartitions = len(rdd.getPartitions())
        self.outputLocs = [[] for i in range(self.numPartitions)]
        self.numAvailableOutputs = 0
        self.jobIds = set()
        self.resultOfJob = None
        self.pendingTasks = []

    def isAvailable(self):
        if not self.isShuffle:
            return True
        else:
            return self.numAvailableOutputs == self.numPartitions

    def addOutputLoc(self, partition, ip_port):
        prevList = self.outputLocs[partition][:]
        if ip_port not in prevList:
            self.outputLocs[partition].insert(0, ip_port)
            if not prevList:
                self.numAvailableOutputs += 1
        print "Current output locs -> " + str(self.outputLocs)

    def removeOutputLoc(self, partition, ip_port):
        prevList = self.outputLocs[partition]
        newList = [p for p in prevList if p != ip_port]
        self.outputLocs[partition] = newList
        if prevList and len(prevList) != len(newList):
            self.numAvailableOutputs -= 1


class DAGScheduler():
    def __init__(self, sc, taskScheduler):
        self.sc = sc
        self.taskScheduler = taskScheduler
        self.taskScheduler.setDAGScheduler(self)

        self.nextStageId = 0
        self.nextJobId = 0

        self.jobIdToStageIds = {}
        self.stageIdToStage = {}
        self.shuffleToMapStage = {}
        self.jobIdToActiveJob = {}

        self.waitingStages = set()
        self.runningStages = set()
        self.failedStages = set()
        self.successedStages = set()

        self.activeJobs = set()

    def newJobId(self):
        id = self.nextJobId
        self.nextJobId += 1
        return id

    def newStageId(self):
        id = self.nextStageId
        self.nextStageId += 1
        return id

    def getShuffleStage(self, shuffleDep, jobId):
        if self.shuffleToMapStage.has_key(shuffleDep.shuffleId):
            stage = self.shuffleToMapStage[shuffleDep.shuffleId]
            #print "parent exists"
            return stage
        else:
            stage = self.newStage(shuffleDep.rdd, len(shuffleDep.rdd.getPartitions()), shuffleDep, jobId)
            self.shuffleToMapStage[shuffleDep.shuffleId] = stage
            return stage

    def newStage(self, rdd, numTasks, shuffleDep, jobId):
        id = self.newStageId()
        print "Create new stage " + str(id) + " <- rdd (" + str(rdd.id) + ")"
        stage = Stage(id, rdd, numTasks, shuffleDep, self.getParentStages(rdd, jobId), jobId)
        self.stageIdToStage[id] = stage
        self.updateJobIdStageIdMaps(jobId, stage)
        return stage

    def updateJobIdStageIdMaps(self, jobId, stage):
        def updateJobIdStageIdMapsList(stages):
            if stages:
                s = stages[0]
                s.jobIds.add(jobId)
                if self.jobIdToStageIds.has_key(jobId):
                    self.jobIdToStageIds[jobId].add(s.id)
                else:
                    self.jobIdToStageIds[jobId] = set([s.id])
                parents = self.getParentStages(s.rdd, jobId)
                parentsWithoutThisJobId = [p for p in parents if jobId in p.jobIds]
                updateJobIdStageIdMapsList(parentsWithoutThisJobId.extend(stages[1:]))

        updateJobIdStageIdMapsList([stage])

    def getMissingParentStages(self, stage):
        #print "Checking missing parents for stage " + str(stage.id) + " (" + str(stage.rdd.id) + ")"
        missing = set()
        visited = set()

        def visit(rdd):
            if rdd not in visited:
                #print rdd
                #print "(" + str(rdd.id) + ") visited"
                visited.add(rdd)
                for dep in rdd.getDependencies():
                    if issubclass(dep.__class__, NarrowDependency):
                        visit(dep.rdd)
                    elif issubclass(dep.__class__, ShuffleDependency):
                        mapStage = self.getShuffleStage(dep, stage.jobId)
                        print "Found parent stage " + str(mapStage.id) + " (" + str(mapStage.rdd.id) + ")"
                        if not mapStage.isAvailable():
                            print "Stage " + str(mapStage.id) + " (" + str(mapStage.rdd.id) + ") is not finished"
                            missing.add(mapStage)
                        else:
                            print "Stage " + str(mapStage.id) + " (" + str(
                                mapStage.rdd.id) + ") is finished or not a shuffle stage"

        visit(stage.rdd)
        return list(missing)

    def getParentStages(self, rdd, jobId):
        parents = set()
        visited = set()
        waitingForVisit = []

        def visit(r):
            if r not in visited:
                visited.add(r)
                for dep in r.getDependencies():
                    if issubclass(dep.__class__, ShuffleDependency):
                        #print "has parent stage"
                        parentStage = self.getShuffleStage(dep, jobId)
                        parents.add(parentStage)
                    else:
                        visit(dep.rdd)

        waitingForVisit.append(rdd)
        while waitingForVisit:
            visit(waitingForVisit.pop())
        return list(parents)

    def activeJobForStage(self, stage):
        jobsThatUseStage = stage.jobIds
        for j in jobsThatUseStage:
            if self.jobIdToActiveJob.has_key(j):
                return j
            else:
                return None

    def submitStage(self, stage):
        jobId = self.activeJobForStage(stage)
        if jobId != None:
            if stage not in self.waitingStages and stage not in self.runningStages and stage not in self.failedStages and stage not in self.successedStages:
                missing = sorted(self.getMissingParentStages(stage), key=lambda x: x.id, reverse=True)
                if not missing:
                    #start running
                    print "Submitting stage " + str(stage.id) + " (" + str(
                        stage.rdd.id) + "), which has no missing parents"
                    self.submitFinalStage(stage, jobId)
                else:
                    for parent in missing:
                        #print "Looking for missing parent stage " + str(parent.id)
                        self.submitStage(parent)
                    print "Add stage " + str(stage.id) + " to the waiting list"
                    self.waitingStages.add(stage)
                self.submitWaitingStages()
        else:
            print "No active job for stage" + str(stage.id)

    def submitWaitingStages(self):
        count = len(self.waitingStages)
        if count:
            print "---------> There are still " + str(count) + " stages remain in the waiting list"
            waitingStagesCopy = self.waitingStages.copy()
            self.waitingStages.clear()
            for stage in sorted(waitingStagesCopy, key=lambda x: x.jobId):
                if stage not in self.successedStages:
                    self.submitStage(stage)
        else:
            print "No stage left in the waiting list"

    def submitFinalStage(self, stage, jobId):
        stage.pendingTasks = []
        partitionsToCompute = []
        if stage.isShuffle:
            partitionsToCompute = [i for i in range(stage.numPartitions) if not stage.outputLocs[i]]
        else:
            job = stage.resultOfJob
            partitionsToCompute = [i for i in range(job.numPartitions) if not job.finished[i]]
        print "Target partitions: " + str(partitionsToCompute)
        print "Start running stage" + str(stage.id) + " (" + str(stage.rdd.id) + ")"
        self.runningStages.add(stage)
        #print self.runningStages
        taskBinary = ''
        if stage.isShuffle:
            taskBinary = pickle.dumps([stage.rdd, stage.shuffleDep])
        else:
            taskBinary = pickle.dumps([stage.rdd, stage.resultOfJob.func])
        tasks = []
        if stage.isShuffle:
            for id in partitionsToCompute:
                if not stage.outputLocs[id]:
                    tasks.append(ShuffleTask(stage.id, taskBinary, id))
        else:
            job = stage.resultOfJob
            for id in partitionsToCompute:
                if not job.finished[id]:
                    p = job.partitions[id]  #same thing
                    tasks.append(ResultTask(stage.id, taskBinary, p, id))
        if len(tasks) > 0:
            stage.pendingTasks.extend(tasks)
            print "Task set of stage " + str(stage.id) + " (" + str(stage.rdd.id) + ") has been submitted"
            self.taskScheduler.submitTasks(TaskSet(tasks, stage.id, stage.jobId))

    def runJob(self, rdd, func, partitions):
        return self.submitJob(rdd, func, partitions)

    def submitJob(self, rdd, func, partitions):
        jobId = self.newJobId()
        waiter = JobWaiter(self, jobId)
        threads = [gevent.spawn(self.handleJobSubmitted, jobId, rdd, func, partitions),
                   gevent.spawn(waiter.awaitResult)]
        gevent.joinall(threads)
        return waiter.getResult()

    def handleJobSubmitted(self, jobId, finalRDD, func, partitions):
        print "Job submitted"
        finalStage = self.newStage(finalRDD, len(partitions), None, jobId)
        if finalStage:
            job = ActiveJob(jobId, finalStage, func, partitions)
            self.jobIdToActiveJob[jobId] = job
            self.activeJobs.add(job)
            finalStage.resultOfJob = job
            self.submitStage(finalStage)
        #Only after we finished running we can execute this
        self.submitWaitingStages()

    #todo when a task is completed
    def handleTaskCompletion(self, task, ip_port, result):
        stage = self.stageIdToStage[task.stageId]
        #if finished
        print [s.partitionId for s in stage.pendingTasks]
        stage.pendingTasks.remove(task)
        #If it doesn't contain shuffle
        if issubclass(task.__class__, ResultTask):
            job = stage.resultOfJob
            if job:
                job.finished[task.outputId] = True
                print job.finished
                job.numFinished += 1
                if job.numFinished == job.numPartitions:
                    self.markStageAsFinished(stage)
            job.results[task.outputId].extend(result)
        elif issubclass(task.__class__, ShuffleTask):
            print "Shuffle task for stage " + str(stage.id) + " finished on " + str(ip_port) + " for partition " + str(
                task.partitionId)
            stage.addOutputLoc(task.partitionId, ip_port)
            if stage in self.runningStages and not stage.pendingTasks:
                self.markStageAsFinished(stage)
                self.submitWaitingStages()


    def markStageAsFinished(self, stage):
        self.runningStages.remove(stage)
        self.successedStages.add(stage)