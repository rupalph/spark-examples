from __future__ import print_function

import sys
from operator import add
from urllib.request import urlopen, urlretrieve

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *

# To run:
# export PYSPARK_PYTHON=python3
# spark-submit <python file name> <arguments>

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
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", StringType(), False)

    ])

    final_df = spark.createDataFrame([], schema)

    for symbol in symbols:
        # csv_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={}&apikey={}&datatype=csv'
        # csv_url = csv_url.format(symbol,api_key)
        # print(csv_url)
        #
        # urlretrieve(csv_url, symbol+'.csv')
        df = spark.read.csv(symbol+'.csv',header=True, mode="DROPMALFORMED",inferSchema="true")
        df.cache

        yearDf = df.withColumn('Year',year(df['timestamp']))

        # Reformating column and keeping same column name, in scala done using foldleft
        # max_df = yearDf.groupBy('Year').max()
        # for col in ['Open','High','Low','Close','Adj Close']:
        #     max_df = max_df.withColumn(col,format_number(sdf[col].cast('float'),2))

        windowSpec = Window.partitionBy("Year").orderBy(df['high'].desc())
        max_df = yearDf.select('*', first(df['high']).over(windowSpec).alias('max')).withColumn('symbol', lit(symbol)).filter('max == high')
        max_df.cache

        max_df = max_df.select('symbol', to_date(yearDf['timestamp'], 'yyyy-MM-dd').alias('date'), 'open', 'high', 'low', 'close', 'volume')
        final_df = final_df.unionAll(max_df)

    final_df.show()
    final_df.coalesce(1).write.csv('stocks.csv')
    spark.stop()
