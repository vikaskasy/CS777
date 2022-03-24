from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
    sc = SparkContext.getOrCreate()
    print(sc.version)
    rdd = sc.textFile(sys.argv[1])
    spark = SparkSession.builder.appName('Task1').getOrCreate()

    def correctRows(p):
        if(len(p)==17):
            if(float(p[5]) and float(p[11])):
                if(float(p[4])> 60 and float(p[5])>0.10 and float(p[11])> 0.10 and float(p[16])> 0.10):
                    return p
    df = spark.read.csv(sys.argv[1])
    rdd1 = df.rdd.map(tuple)

    taxiclean = rdd1.filter(correctRows)
    t = taxiclean.map(lambda x:((x[0],x[1]))).distinct()\
    .map(lambda x:(x[0],1))\
    .reduceByKey(lambda a,b: a+b)\
        .top(10, lambda x:x[1])

    print(t)
    topTaxi = sc.parallelize(t).coalesce(1)
    topTaxi.saveAsTextFile(sys.argv[2])


    sc.stop()
    spark.stop()