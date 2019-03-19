import sys
import os
from datetime import datetime
from pathlib import Path

STREAM_IN = 'stream-IN'
STREAM_OUT = 'stream-OUT'

# We first delete all files from the STREAM_IN folder
# before starting spark streaming.
# This way, all files are new
print("Deleting existing files in %s ..." % STREAM_IN)
p = Path('.') / STREAM_IN
for f in p.glob("*.ordtmp"):
  os.remove(f)
print("... done")

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

sc = SparkContext("local[*]", "BuyAndSellPerBatch")
sc.setLogLevel("WARN")   #Make sure warnings and errors observed by spark are printed.

ssc = StreamingContext(sc, 5)  #generate a mini-batch every 5 seconds
filestream = ssc.textFileStream(STREAM_IN) #monitor new files in folder stream-IN

def parseOrder(line):
  '''parses a single line in the orders file into a dictionary'''
  s = line.split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"),
               "orderId": int(s[1]), 
               "clientId": int(s[2]),
               "symbol": s[3], 
               "amount": int(s[4]), 
               "price":  float(s[5]), 
               "buy": s[6] == "B"}]
  except Exception as err:
      print("Wrong line format (%s): %s" % (line,err))
      return [] #ignore this line since it trew an error while parsing

# Convert the input DStream (where each RDD contains lines) into a
# DStream of python dictionaries (where each RDD contains
# dictionaries) flatMap applies parseOrder on each line in each RDD in
# the DStream, where results are flattened
orders = filestream.flatMap(parseOrder)

from operator import add

# Calculate total number of buy/sell orders (buy -> key = True, sell -> key = False)
# map applies its argument function on each RDD in the DStream
# reduceByKey applies reduceBykey on each RDD in the DStream
numPerType = orders.map(lambda o: (o['buy'], 1)).reduceByKey(add)

# Print the first 10 lines of each RDD computed in the DStream to stdou
# This is usefull for debugging purposes only
numPerType.pprint()

# -----ALTERNATIVE TO PPRINT----
# If instead you want to save each computed RDD to a file, uncomment the following
# This creates a new folder for each RDD computed; inside the folder 1 file for
#  each partition in the rdd is created. To make this easy to inspect, we
# repartition the RDD into 1 single partition (but this is not required).
# ------------------------------
#numPerType.repartition(1).saveAsTextFiles(STREAM_OUT, "txt")

# Now start consuming input and wait forever (or until you press CTRL+C)
ssc.start()
ssc.awaitTermination()


