import sys
import os
import pwd
from datetime import datetime

#import findspark
#findspark.init("/opt/cloudera/parcels/CDH-6.1.0-1.cdh6.1.0.p0.770702/lib/spark/")

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext("local[*]", "CountAndVolumePerBatch")
sc.setLogLevel("ERROR")   #Make sure warnings and errors observed by spark are printed.

ssc = StreamingContext(sc, 5)  #generate a mini-batch every 5 seconds
zookeeper = "bigdata01.hpda.ulb.ac.be:2181"
username = pwd.getpwuid( os.getuid() )[ 0 ] 
topic = username + ".orders"
inputStream = KafkaUtils.createStream(ssc, zookeeper,
                                  "raw-event-streaming-consumer", {topic:1})

def parseOrder(line):
  '''parses a single line in the orders file'''
  s = line.strip().split(",")
  try:
      if s[6] != u"B" and s[6] != u"S":
        raise Exception('Wrong format ' + str(s))
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"),
               "orderId": int(s[1]), 
               "clientId": int(s[2]),
               "symbol": s[3], 
               "amount": int(s[4]), 
               "price":  float(s[5]), 
               "buy": s[6] == u"B"}]
  except Exception as err:
      print("Wrong line format (%s): %s" % (line,err))
      return []

from operator import add
orders = inputStream.map(lambda x: x[1]).flatMap(parseOrder)
ordersPerMinute = orders.map(lambda o: 1).window(60, 15) # windows lenth = 60 sec, slide = 15 sec
orderCountPerMinute = ordersPerMinute.reduce(add)
orderCountPerMinute.pprint()

# windows operations requires checkpointing
sc.setCheckpointDir("checkpoint")

ssc.start()
ssc.awaitTermination()


