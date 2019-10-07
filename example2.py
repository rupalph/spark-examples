"""SimpleApp.py"""
from pyspark import SparkContext

numFile = "/Users/rupalph/IdeaProjects/spark/numbers.txt"  # Should be some file on your system
sc = SparkContext("local", "Calculate Mean App")
numData = sc.textFile(numFile).cache()

#Sorts the input file of numbers
# sortedCount = numData.flatMap(lambda x: x.split(' ')) \
#     .map(lambda x: (int(x), 1)) \
#     .sortByKey()
#
# output = sortedCount.collect()
# print "*****"
# for (num, unitcount) in output:
#     print(num)
#
# #calculates avg from input file of numbers
# avg = numData.flatMap(lambda x: x.split(' ')) \
#     .map(lambda x: (1, int(x))) \
#     .reduceByKey(lambda x,y:float(x+y)/2)
#
# output2 = avg.collect()
# print "*****"
# for (count, num) in output2:
#     print(num)


rdd = numData.flatMap(lambda x: x.split(' ')).map(lambda x: int(x))
output = rdd.reduce(lambda x,y: float(x+y)/2)
print "*****"
print(output)