from ms_SparkContext import *

'''
    Test Extenshion
'''


def test(spark, path):
    numbers = spark.textFile("sort.txt", 3)
    print spark.TopByKey(numbers, 2)


if __name__ == '__main__':
    port = sys.argv[1]
    path = sys.argv[2]
    spark = SparkContext()
    gevent.joinall([
        gevent.spawn(spark.createServerHandle, port),
        gevent.spawn(test, spark, path)
    ])