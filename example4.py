"""SimpleApp.py"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local", "Calculate Mean App")
ssc = StreamingContext(sc, 30)
dataRdd = [sc.parallelize(d, 1) for d in [[1,2,3],[4,5],[6,7,8,9,9]]]
qs = ssc.queueStream(dataRdd)

def list_median((med,mylist),newval):
    mylist = [newval] if not mylist else mylist.append(newval)
    mylist = sorted(mylist)
    return (mylist[int(len(mylist)/2)], mylist)

medians = qs.reduce(list_median).map(lambda (med,list): med)

medians.pprint()
ssc.start()
ssc.awaitTermination()