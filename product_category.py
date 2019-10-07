"""SimpleApp.py"""
from pyspark import SparkContext

sc = SparkContext("local", "Product Category App")

data = [('Apple','Fruit',200),('Banana','Fruit',24),('Tomato','Fruit',56),('Potato','Vegetable',103),('Carrot','Vegetable',34)]
rdd = sc.parallelize(data,4)

category_price_rdd = rdd.map(lambda x: (x[1],x[2]))
output = category_price_rdd.collect()
print "*****"
for (key, value) in output:
    print("{} {}".format(key, value))

category_total_price_rdd = category_price_rdd.reduceByKey(lambda x,y:x+y)
output2 = category_total_price_rdd.collect()

print "*****"
for (key, value) in output2:
    print("{} {}".format(key, value))


category_product_rdd = rdd.map(lambda x: (x[1],x[0]))
# category_product_rdd.collect()

grouped_products_by_category_rdd = category_product_rdd.groupByKey()
findata = grouped_products_by_category_rdd.collect()
for data in findata:
    print data[0],list(data[1])