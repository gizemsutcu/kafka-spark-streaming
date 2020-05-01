import sys
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from datetime import date
import pyspark.sql.functions as funcs
from pyspark.sql import Window
from pyspark.sql.streaming import *

# gizemsutcu

sparkSession = SparkSession.builder.master("local").appName("kafka-pyspark").getOrCreate()

data = sparkSession.readStream.format("kafka").option("kafka.bootstrap.servers","209.250.250.135:9092").option("subscribe", "virtual-market-analysis").load()

cast = data.selectExpr("CAST(value AS STRING)")


employee_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("birth_date", StringType(), True),
    StructField("telephone_number", StringType(), True),
    StructField("city_id", IntegerType(), True)])

product_schema = StructType([
      StructField("product_id", IntegerType(), True),
      StructField("product_name", StringType(), True),
      StructField("product_cost", FloatType(), True)])

schema = StructType([
    StructField("employee", employee_schema, True),
    StructField("product", product_schema, True),
    StructField("product_piece", IntegerType(), True),
    StructField("current_ts", StringType(), True)])

marketData = cast.select(from_json("value", schema).alias("jsontostructs")).select("jsontostructs.*")

# change data types from string to timestamp
timestampData = marketData.withColumn("timestamp", to_timestamp(marketData["current_ts"], "dd/MM/yyyy, HH:mm:ss"))

# total number of purchases made in the last 1 minute
# you can change the time
windowDataSort = timestampData.groupBy(funcs.window("timestamp", "1 minute")).count()
windowDataSortQuery = windowDataSort.sort(desc("count"))

# number of products taken in the last 1 minute (from maximum to minimum)
windowDataSort = timestampData.groupBy(funcs.window("timestamp", "1 minute")).sum("product_piece")
windowDataSortQuery = windowDataSort.sort(desc("sum(product_piece)"))

# decreased number of stocks for each product in the last 1 minute (from maximum to minimum)
windowDataSort = timestampData.groupBy(funcs.window("timestamp", "1 minute"), "product.product_name").count()
windowDataSortQuery = windowDataSort.sort(desc("count"))

# the amount spent by each employee in the last 1 minute
total_cost = marketData.withColumn("total_cost", marketData['product.product_cost']*marketData['product_piece'])
timestampData2 = total_cost.withColumn("timestamp", to_timestamp(marketData["current_ts"], "dd/MM/yyyy, HH:mm:ss"))
windowDataSortQuery = timestampData2.groupBy(funcs.window("timestamp", "1 minute"), "employee.user_id")\
   .sum("total_cost").sort(desc("sum(total_cost)"))


# spark streaming start
query = (windowDataSortQuery.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination())
