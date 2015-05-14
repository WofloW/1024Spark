from ms_SparkContext import *
def do(spark):
    numbers = spark.textFile("sort.txt", 5)
    print spark.TopByKey(numbers, 5, lambda x:x.split(",")[1], False)