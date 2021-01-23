from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests
from pyspark.sql import Row, SQLContext


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 60)# sc, time interval for batch update.
ssc.checkpoint("checkpoint_TwitterApp")
dataStream = ssc.socketTextStream("localhost", 8100) # stream data from TCP; source, port



words = dataStream.flatMap(lambda line: line.split(" "))

hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

tags_totals = hashtags.reduceByKey(lambda a,b:a+b)

tags_totals.pprint()


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate