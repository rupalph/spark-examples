from __future__ import print_function

import sys
from operator import add
from urllib.request import urlopen, urlretrieve

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: findpeak.py <stock-symbol>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("PythonFindPeak") \
        .getOrCreate()

    api_key = 'H4F33JFQ1EC6T0ZO'
    symbols = sys.argv[1:]
    date_col = "timestamp"
    high_col = "high"
    low_col = "low"

    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("lowest_value", DoubleType(), False),
        StructField("lowest_time", TimestampType(), False),
        StructField("peak_value", DoubleType(), False),
        StructField("peak_time", TimestampType(), False)
    ])
    final_df = spark.createDataFrame([],schema)

    for symbol in symbols:
        csv_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol={}&apikey={}&datatype=csv'
        csv_url = csv_url.format(symbol,api_key)
        print(csv_url)

        urlretrieve(csv_url, symbol+'.csv')
        df = spark.read.csv(symbol+'.csv',header=True, mode="DROPMALFORMED",inferSchema="true")
        df.cache
        #df.printSchema()

        # highest = df.select(date_col, col(high_col)).orderBy(desc(high_col)).take(1)
        # lowest = df.select(date_col, col(low_col)).orderBy(low_col).take(1)
        #
        # print("###########")
        #
        # print(highest)
        # print(lowest)
        #
        # stats = df.groupBy().agg(min(low_col), max(high_col))
        # stats.show()
        w = Window.partitionBy("symbol")

        p = df.withColumn('symbol',lit(symbol))\
            .withColumn("value", max(high_col).over(w))\
            .filter((col(high_col)==col('value'))).select('symbol','value', col(date_col) ).withColumn('stats',lit('peak'))
        p.cache
        l = df.withColumn('symbol',lit(symbol)) \
            .withColumn("value", min(low_col).over(w)) \
            .filter((col(low_col) == col('value'))).select('symbol','value', col(date_col)).withColumn('stats',lit('lowest'))
        l.cache

        res_df = p.union(l).groupBy('symbol').pivot('stats').agg(first('value').alias('value'), first(date_col).alias('time'))
        res_df.printSchema()

        final_df = final_df.unionAll(res_df)

    # maxValue = df.select(high_col).rdd.max()[0]
    #for (datex, value) in output:
    #    print("%s: %s" % (datex, value))
    # print("#################################################################")
    # print("Date: %s Peak Value: %s" % (output[date_col], output[high_col]))
    # print("Date: %s Lowest Value: %s" % (lowest[date_col], lowest[low_col]))
    # print("#################################################################")
    final_df.show()
    spark.stop()


# To Run
#  /Users/rupalph/software/spark-2.4.3-bin-hadoop2.7/bin/spark-submit ~/IdeaProjects/spark/findpeak.py ~/IdeaProjects/spark/TWLO.csv