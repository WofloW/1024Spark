import sys
from ms_SparkContext import *


if __name__ == '__main__':
    script_name = sys.argv[1]
    port = sys.argv[2]
    spark = SparkContext()
    gevent.joinall([
        gevent.spawn(spark.createServerHandle, port),
        gevent.spawn(execfile, script_name)
    ])