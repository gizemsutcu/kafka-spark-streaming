import sys
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from datetime import date
import pyspark.sql.functions as funcs

# gizemsutcu


sparkSession = SparkSession.builder.master("local").appName("kafka-pyspark").getOrCreate()

data = sparkSession.read.format("kafka").option("kafka.bootstrap.servers","209.250.250.135:9092").option("subscribe", "virtual-market-analysis").load()

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


# SPARK QUERY #

# to view the data
marketData.show(20, False)

# to display data types
marketData.printSchema()

# to find out the number of data
marketData.count()

# to see how many times employees shopped
marketData.groupBy("employee.user_id").count().show()

# to see how many times employees shopped(in order from maximum to minimum)
marketData.groupBy("employee.user_id").count().sort(desc("count")).show()

# city ranking from maximum to minimum shopping (top 10 cities)
marketData.groupBy("employee.city_id").count().sort(col("count").desc()).limit(10).show()

# employee ranking from minimum to maximum shopping (top 4 employees)
marketData.groupBy("employee.user_id").count().sort(col("count").asc()).limit(4).show()

# total amount for each shopping data
total_cost = marketData.withColumn("total_cost", marketData['product.product_cost']*marketData['product_piece']).show(20, False)

# the total amount of purchases made by each employee
total_cost.groupBy("employee.user_id").sum("total_cost").sort(desc("sum(total_cost)")).show()

# the total amount of purchases made by each city
total_cost.groupBy("employee.city_id").sum("total_cost").sort(desc("sum(total_cost)")).show()

# decreased amount of product in the warehouse for each product in each city
city_stock = marketData.groupBy("employee.city_id", "product.product_name").sum("product_piece").sort(desc("city_id"), desc("sum(product_piece)"))
city_stock.show()

# maximum reduced product and quantity in the warehouse for each product in each city
city_stock.groupBy("city_id").max("sum(product_piece)").sort(desc("city_id")).show(20, False)

# the total amount that each employee spends for each product
total_cost.groupBy("employee.user_id","product.product_name").sum("total_cost").sort(desc("sum(total_cost)")).show()

# reduced total quantity for each product
end_stock_state = marketData.groupBy("product.product_name").sum("product_piece").sort(desc("sum(product_piece)"))
# second and long way
stock = marketData.groupBy("product.product_name","product_piece").count().sort(desc("count"))
stock_data = stock.withColumn("stock_reduction_number",stock['count']*stock['product_piece'])
columns_to_drop = ["product_piece","count"]
stock_data = stock_data.drop(*columns_to_drop)
stock_data_2 = stock_data.groupBy("product_name").sum("stock_reduction_number").show()

# employees and phone numbers with a phone number of 9
marketData.filter(frjData["employee.telephone_number"].contains('9')).select("employee.user_id","employee.telephone_number").show()

# today = datetime.date.today()
ageData = marketData.withColumn("age", (datediff(to_date(lit(today)), to_date(marketData["employee.birth_date"],"dd/MM/yyyy")) /365).cast(IntegerType()))

# account of the ages of each employee
ageData.select("employee.user_id", "age").show(20, False)

# average age account of all employees
ageData.agg(funcs.avg("age")).show(20, False)

# average age account of employees for each city
ageData.groupBy("employee.city_id").agg(functions.avg("age")).sort(desc("city_id")).show(20, False)

