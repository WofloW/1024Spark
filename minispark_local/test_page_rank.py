from ms_SparkContext import *

def do(spark):
    def computeContribs(urls, rank):
        """Calculates URL contributions to the rank of other URLs."""
        num_urls = len(urls)
        for url in urls:
            yield (url, rank / num_urls)

    def parseNeighbors(urls):
        """Parses a urls pair string into urls pair."""
        parts = urls.split(' ')
        return parts[0], parts[1]

    print "Start page ranking"
    lines = spark.textFile("pagerank.txt")
    links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey(HashPartitioner())
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    contribs = links.join(ranks, HashPartitioner()).flatMap(
        lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    ranks = contribs.reduceByKey(HashPartitioner()).mapValues(lambda rank: rank * 0.85 + 0.15)

    for iteration in range(10):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks, HashPartitioner()).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(HashPartitioner()).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    for (link, rank) in spark.collect(ranks):
        print("%s has rank: %s." % (link, rank))