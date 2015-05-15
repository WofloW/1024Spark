from ms_SparkContext import *
from datetime import datetime

def do(spark):
    start_time = datetime.now()
    numbers = spark.textFile("sort.txt", 5)
    print spark.TopByKey(numbers, 5, lambda x:x.split(",")[1], False)
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))