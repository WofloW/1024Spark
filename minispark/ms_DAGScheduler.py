import zerorpc
#Thanks Kaiming for his pickler
import pickle, with_function

from ms_RDD import *
from ms_TaskScheduler import *

'''
    Scheduler
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

class JobWaiter():

    def __init__(self, dagScheduler, jobId):
        self.dagScheduler = dagScheduler
        self.jobId = jobId
        self.jobFinished = False

    def awaitResult(self):
        LOOP = True
        while LOOP:
            if self.dagScheduler.jobIdToActiveJob.has_key(self.jobId):
                while not self.jobFinished:
                    print "Checking status..."
                    result = reduce(lambda x, y: x and y, self.dagScheduler.jobIdToActiveJob[self.jobId].finished)
                    self.jobFinished = result
                    gevent.sleep(1)
                LOOP = False
            else:
                print "Waiting for active job..."
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
            self.isShuffleMap = True
        else:
            self.isShuffleMap = False
        self.numPartitions  = len(rdd.getPartitions())
        self.outputLocs = [[] for i in range(self.numPartitions)]
        self.numAvailableOutputs = 0
        self.jobIds = set()
        self.resultOfJob = None
        self.pendingTasks = []

    def isAvailable(self):
        if not self.isShuffleMap:
            return True
        else:
            return self.numAvailableOutputs == self.numPartitions

    def addOutputLoc(self, partition, port):
        print "!!! Current output locs -> " + str(self.outputLocs) + " for partition " + str(partition)
        prevList = self.outputLocs[partition][:]
        if port not in prevList:
            self.outputLocs[partition].insert(0, port)
            if not prevList:
                self.numAvailableOutputs += 1
        print "!!! Now output locs -> " + str(self.outputLocs[partition])

    def removeOutputLoc(self, partition, port):
        prevList = self.outputLocs[partition]
        newList = [p for p in prevList if p != port]
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

        self.activeJobs = set()

    def newJobId(self):
        id = self.nextJobId
        self.nextJobId += 1
        return id

    def newStageId(self):
        id = self.nextStageId
        self.nextStageId += 1
        return id

    def getShuffleMapStage(self, shuffleDep, jobId):
        if self.shuffleToMapStage.has_key(shuffleDep.shuffleId):
            print "Link to used stage " + str(self.shuffleToMapStage[shuffleDep.shuffleId].id)
            return self.shuffleToMapStage[shuffleDep.shuffleId]
        else:
            stage = self.newStage(shuffleDep.rdd, len(shuffleDep.rdd.getPartitions()), shuffleDep, jobId)
            print "Created new stage " + str(stage.id)
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
    
    def getParentStages(self, rdd, jobId):
        parents = set()
        visited = set()
        waitingForVisit = []
        def visit(r):
            if r not in visited:
                visited.add(r)
                for dep in r.getDependencies():
                    if issubclass(dep.__class__, ShuffleDependency):
                        print "Create parent stages <- rdd (" + str(rdd.id) + ")"
                        parentStage = self.getShuffleMapStage(dep, jobId)
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
        print "Submitting stage " + str(stage.id)
        jobId = self.activeJobForStage(stage)
        if jobId != None:
            if stage not in self.waitingStages and stage not in self.runningStages and stage not in self.failedStages:
                missing = sorted(self.getMissingParentStages(stage), key=lambda x: x.id)
                if not missing:
                    #start running
                    print "Submitting stage " + str(stage.id) + " (" + str(stage.rdd.id) + "), which has no missing parents"
                    self.submitMissingTasks(stage, jobId)
                else:
                    for parent in missing:
                        print "Looking for missing parent stage " + str(parent.id)
                        self.submitStage(parent)
                    print "Add stage " + str(stage.id) + " to the waiting list"
                    self.waitingStages.add(stage)
        else:
            print "No active job for stage" + str(stage.id)
    
    def getMissingParentStages(self, stage):
        print "Checking missing parents for stage " + str(stage.id)
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
                        mapStage = self.getShuffleMapStage(dep, stage.jobId)
                        print "Found parent stage " + str(mapStage.id) + " for stage " + str(stage.id)
                        #todo
                        #print "Check available outputs: " + str(mapStage.numAvailableOutputs)
                        if not mapStage.isAvailable():
                            missing.add(mapStage)
                        else:
                            print "Stage " + str(mapStage.id) + " is finished or not a shuffle stage"
        visit(stage.rdd)
        return list(missing)

    def submitWaitingStages(self):
        print "There are still " + str(len(self.waitingStages)) + " stages remain in the waiting list"
        waitingStagesCopy = list(self.waitingStages)
        self.waitingStages.clear()
        for stage in sorted(waitingStagesCopy, key=lambda x: x.jobId):
            self.submitStage(stage)

    def submitMissingTasks(self, stage, jobId):
        stage.pendingTasks = []

        partitionsToCompute = []
        if stage.isShuffleMap:
            partitionsToCompute = [i for i in range(stage.numPartitions) if not stage.outputLocs[i]]
        else:
            job = stage.resultOfJob
            partitionsToCompute = [i for i in range(job.numPartitions) if not job.finished[i]]

        print "Start running stage" + str(stage.id)
        self.runningStages.add(stage)

        taskBinary = ''
        if stage.isShuffleMap:
            taskBinary = pickle.dumps([stage.rdd, stage.shuffleDep])
        else:
            taskBinary = pickle.dumps([stage.rdd, stage.resultOfJob.func])

        tasks = []
        if stage.isShuffleMap:
            for id in partitionsToCompute:
                if not stage.outputLocs[id]:
                    tasks.append(ShuffleMapTask(stage.id, taskBinary, id))
        else:
            job = stage.resultOfJob
            for id in partitionsToCompute:
                if not job.finished[id]:
                    p = job.partitions[id]#same thing
                    tasks.append(ResultTask(stage.id, taskBinary, p, id))

        if len(tasks) > 0:
            stage.pendingTasks.extend(tasks)
            print "Task set of stage " + str(stage.id) + " has been submitted"
            self.taskScheduler.submitTasks(TaskSet(tasks, stage.id, stage.jobId))

    def markStageAsFinished(self, stage):
          self.runningStages.remove(stage)

    def submitJob(self, rdd, func, partitions):
        jobId = self.newJobId()
        waiter = JobWaiter(self, jobId)
        threads = [gevent.spawn(self.handleJobSubmitted, jobId, rdd, func, partitions), gevent.spawn(waiter.awaitResult)]
        gevent.joinall(threads)
        return waiter.getResult()

    def runJob(self, rdd, func, partitions):
        return self.submitJob(rdd, func, partitions)

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
    def handleTaskCompletion(self, task, port, result):
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
        elif issubclass(task.__class__, ShuffleMapTask):
            print "Shuffle task finished on " + str(port) + " for partition " + str(task.partitionId)
            stage.addOutputLoc(task.partitionId, port)
            if stage in self.runningStages and stage.pendingTasks:
                self.markStageAsFinished(stage)
            if None in stage.outputLocs:
                #handle stage fails
                self.submitStage(stage)
            else:
                newlyRunnable = []
                for stage in self.waitingStages:
                    if self.getMissingParentStages(stage):
                        newlyRunnable.append(stage)
                for stage in newlyRunnable:
                    self.waitingStages.remove(stage)
                    self.runningStages.add(stage)
                for stage in sorted(newlyRunnable, key=lambda x: x.id):
                    jobId = self.activeJobForStage(stage)
                    self.submitMissingTasks(stage, jobId)