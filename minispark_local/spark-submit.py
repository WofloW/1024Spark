import sys
import code
from gevent import fileobject
import importlib
from ms_SparkContext import *

# Usage:
# python spark-sbumit.py test_word_count.py 4000
# python spark-sbumit.py test_page_rank.py 4000

"""
    Micro Spark Submit
"""

if __name__ == '__main__':

    script = sys.argv[1]
    port = sys.argv[2]
    spark = SparkContext()
    module = importlib.import_module(script[:-3])

    gevent.joinall([
        gevent.spawn(spark.createServerHandle, port),
        gevent.spawn(module.do, spark)
    ])
