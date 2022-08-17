# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[1]").appName("pysparkExample3").getOrCreate()
spark.version
spark.sparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC ##Aggregate functions

# COMMAND ----------


simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *
df.select(approx_count_distinct("salary"),avg("salary"),count("salary"),mean("salary")).show()
df.select(collect_list("salary")).show(truncate=False)
df.select(collect_set("salary")).show(truncate=False)

# COMMAND ----------

import pyspark.sql.functions 

df.select(max("department"),min("salary"),mean("salary"),sum("department"),first("salary"),last("salary")).show()
# df.select(min("salary")).show()
# df.select(mean("salary")).show()
# df.select(sum("salary")).show()
# df.select(first("salary")).show()
# df.select(last("salary")).show()


# COMMAND ----------

from pyspark.sql.functions import stddev,stddev_samp

df.select(stddev("salary"), stddev_samp("salary"), \
    stddev_pop("salary")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Window functions

# COMMAND ----------

from pyspark.sql.window import *
window_con=Window.partitionBy("department").orderBy("salary")
df.withColumn("rank",rank().over(window_con))\
.withColumn("lag",lag("salary",2).over(window_con))\
.withColumn("Lead",lead("salary",2).over(window_con))\
.withColumn("cumulative distribution",cume_dist().over(window_con))\
.withColumn("ntile",ntile(2).over(window_con))\
.withColumn("Percent_rank",percent_rank().over(window_con))\
  .withColumn("dense_rank",dense_rank().over(window_con))\
.withColumn("row_number",row_number().over(window_con)).show()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Date and timestamp functions

# COMMAND ----------

from pyspark.sql.functions import *
data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
df.show()

# current_date
df.select(current_date().alias("current_date")).show()

# date_format, to_date
df.select("input",date_format("input","dd-MM-yyyy").alias("Changed_format")).show()
df1=df.select("input",to_date("input","yyyy-MM-dd").alias("date_format"))
df.printSchema()
df1.show()
df1.printSchema()

# datediff, months_between, add_months, date_add, date_sub
df.select("input",datediff(current_date(),"input").alias("differce_btw_dates")).show()
df.select("input",months_between(current_date(),"input").alias("diff_btw_mnths")).show()
df.select("input",add_months(col("input"),3).alias("add_months"), 
    add_months(col("input"),-3).alias("sub_months"), 
    date_add(col("input"),4).alias("date_add"), 
    date_sub(col("input"),4).alias("date_sub") ).show()

# year, month, next_day, weekofyear
df.select("input", 
     year("input").alias("year"), 
     month("input").alias("month"), 
     next_day("input","Sunday").alias("next_day"), 
     weekofyear(col("input")).alias("weekofyear") 
  ).show()
df.select(col("input"),  
     dayofweek(col("input")).alias("dayofweek"), 
     dayofmonth(col("input")).alias("dayofmonth"), 
     dayofyear(col("input")).alias("dayofyear"), 
  ).show()

# creation of dataframe
data=[["1","02-01-2020 11 01 19 06"],["2","03-01-2019 12 01 19 406"],["3","03-01-2021 12 01 19 406"]]
df2=spark.createDataFrame(data,["id","input"])
df2.show(truncate=False)

#current_timestamp()
df2.select(current_timestamp().alias("current_timestamp")
  ).show(1,truncate=False)


#to_timestamp()
df2.select(col("input"), 
    to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp") 
  ).show(truncate=False)


#hour, minute,second
data1=[["1","2020-02-01 11:01:19.06"],["2","2019-03-01 12:01:19.406"],["3","2021-03-01 12:01:19.406"]]
df3=spark.createDataFrame(data1,["id","input"])

df3.select(col("input"), 
    hour(col("input")).alias("hour"), 
    minute(col("input")).alias("minute"),
    second(col("input")).alias("second") 
  ).show(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC ##JSON functions

# COMMAND ----------

# JSON functions: from_json, to_json, json_tuple, get_json_object, schema_of_json()

from pyspark.sql.functions import *
from pyspark.sql.types import *
js="""{"id":1,"Name":"asdf","country":"India","state":"AP","mobile":"9564582004"}"""
df=spark.createDataFrame([(1,js)],["ID","Json_string"])
df.printSchema()
df.show(truncate=False)

df2=df.withColumn("Json_string",from_json(df.Json_string,MapType(StringType(),StringType())))
df2.printSchema()
df2.show(truncate=False)

df2.withColumn("Json_string",to_json(col("Json_string"))).show(truncate=False)

df.select(col("id"),json_tuple("Json_string","Name","country","state")) \
    .toDF("id","Name","Country","State") \
    .show(truncate=False)

df.select(col("id"),get_json_object("Json_string","$.country").alias("Country")).show(truncate=False)

df.select(schema_of_json(js)) \
    .show(truncate=False)