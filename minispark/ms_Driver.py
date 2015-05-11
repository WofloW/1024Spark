from ms_SparkContext import *

#sc = SparkContext()
#lines = sc.textFile("wordcount.txt", 4)
#words = lines.flatMap(lambda line: line.split(" "))
#wordDict = words.map(lambda word: (word, 1))
#counts = wordDict.reduceByKey(lambda a, b: a + b)

'''
    Test cases
'''
def doWordCount(path):
    print "Start word counting"
    lines = spark.textFile(path, 4)
    words = lines.flatMap(lambda line: line.split(" "))
    wordDict = words.map(lambda word: (word, 1))
    counts = wordDict.reduceByKey(lambda a, b: a + b)
    print counts.collect(1)

def doPageRank(path):

    def computeContribs(urls, rank):
        """Calculates URL contributions to the rank of other URLs."""
        num_urls = len(urls)
        for url in urls:
            yield (url, rank / num_urls)

    def parseNeighbors(urls):
        """Parses a urls pair string into urls pair."""
        parts = re.split(r'\s+', urls)
        return parts[0], parts[1]

    print "Start page ranking"
    lines = spark.textFile(path)
    links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(10):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

if __name__ == '__main__':
    #spark = SparkContext()
    #data = [1,2,3,4,5]
    #RDDA = spark.parallelize(data).map(lambda x: x + 1).filter(lambda x: x > 3)
    #RDDB = spark.parallelize(data).map(lambda x: x + 4).filter(lambda x: x > 8)
    #print RDDA.collect()
    #print RDDB.collect()
    #RDDAB = RDDA.union(RDDB)
    #print RDDAB.collect()
    #print RDDAB.reduce(lambda a, b: a + b)
    #print RDDAB.flatMap(lambda x: range(x)).collect()
    #print RDDA.crossProduct(RDDB).collect()
    
    #data2 = [('a', 1), ('b', 2), ('c', 3), ('d', 5), ('a', 6), ('d', 12)]
    #data3 = [('a', 6), ('b', 7), ('c', 10), ('d', 2)]
    #RDDC = spark.parallelize(data2)
    #RDDD = spark.parallelize(data3)
    #print RDDC.collect()
    #print RDDD.collect()
    #print RDDC.join(RDDD).collect()
    #print RDDC.groupByKey().collect()
    #print RDDC.reduceByKey(lambda a, b: a + b).collect()

    #doWordCount("wordcount.txt")
    #doPageRank("pagerank.txt")

    #counts = wordDict.reduceByKey(lambda a, b: a + b)

    ###
    #dag = DAGScheduler(spark)
    ###

    #finalStage = dag.newStage(g2, 0, None, 0)
    #print "Submit final stage (" + str(finalStage.id) + ") create by final RDD (" + str(g2.id) + ")"
    
    ###
    #dag.handleJobSubmitted(0, g2, None, range(4))
    ###

    #print dag.jobIdToStageIds
    #print dag.stageIdToStage
    #print dag.shuffleToMapStage
    #print dag.waitingStages
    #print dag.runningStages
    #print dag.failedStages
    #print dag.activeJobs

    #
    spark = SparkContext()

    def do(spark):
        lines = spark.textFile("wordcount.txt", 3)
        print lines
        print lines.id
        words = lines.flatMap(lambda line: line.split(" "))
        print words
        print words.id
        wordDict = words.map(lambda word: (word, 1))
        print wordDict
        print wordDict.id
        groups = wordDict.reduceByKey(HashPartitioner(4))
        print groups
        print groups.id
        g1 = groups.groupByKey(HashPartitioner(5))
        print g1
        print g1.id
        g2 = g1.map(lambda x: x)
        print g2
        print g2.id
        ###
        #dag = DAGScheduler(spark)
        ###
        #dag.handleJobSubmitted(0, wordDict, None, range(4))
        ###
        #print wordDict.__dict__
        print spark.collect(g2)

    gevent.joinall([gevent.spawn(spark.createServerHandle, 4001), gevent.spawn(do, spark)])

    

