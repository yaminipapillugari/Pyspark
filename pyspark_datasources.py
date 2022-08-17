# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[1]").appName("pysparkExample1").getOrCreate()
spark.version
spark.sparkContext

# COMMAND ----------

df=spark.createDataFrame([(1,"mobile"),(2,"tv"),(3,"charger"),(4,"extenxtion box")],["Id","Product_name"])
df.printSchema()
df.show()

# COMMAND ----------

rdd= sc.parallelize([1,2,3,4,5,6,7,8,9])
# rdd1=sc.range(20)
rdd.collect()
# rdd1.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##CSV Data

# COMMAND ----------

# Reading csv file
df=spark.read.option("header","true").csv("dbfs:/FileStore/pyspark_demo/csv_file.csv")
df.show()
df1 = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/pyspark_demo/csv_file.csv")
df1.show()


# COMMAND ----------

# Reading csv files
df=spark.read.option("header","true").csv("dbfs:/FileStore/pyspark_demo/")
df.show()


# COMMAND ----------

# Perform some transformations
df1=df.select(["customer_email","net_revenue"])
df1.show()


# COMMAND ----------

# Writing csv file to particular path
df1.write.option("header","true").mode("overwrite").csv("dbfs:/FileStore/pyspark_demo/cust_data1.csv")


# COMMAND ----------

# Reading the written csv data
df2=spark.read.option("header","true").csv("dbfs:/FileStore/pyspark_demo/cust_data1.csv")
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Parquet Data

# COMMAND ----------

# Write data to parquet file
data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)

df.write.mode("append").option("header","true").parquet("dbfs:/FileStore/pyspark_demo/employee_data.parquet")


# COMMAND ----------

#Read data from parquet file
df=spark.read.parquet("dbfs:/FileStore/pyspark_demo/employee_data.parquet")
df.show()

# COMMAND ----------

# Creation of SQL table from parquet file and writing again to parquet table
tabledf=df.createOrReplaceTempView("ParquetTable")
df1=spark.sql("select * from ParquetTable where salary=4000")
df1.show()
df1.write.mode("overwrite").option("header","true").parquet("dbfs:/FileStore/pyspark_demo/employee_data_sal.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ##JSON data

# COMMAND ----------

# Reading and writing json file which is in multipl lines
df=spark.read.option("multiline","true").json("dbfs:/FileStore/pyspark_demo/json_file.json")
df.count()
df.write.mode("overwrite").json("dbfs:/FileStore/pyspark_demo/json_file1.json")
df1=spark.read.option("multiline","true").json("dbfs:/FileStore/pyspark_demo/json_file1.json")
df1.show()

# COMMAND ----------

# Reading multiple files
df2 = spark.read.json(
    ['dbfs:/FileStore/pyspark_demo/json_file.json','dbfs:/FileStore/pyspark_demo/json_file1.json'])
df2.printSchema() 
df2.show()

# Read all JSON files from a folder
df3 = spark.read.json("dbfs:/FileStore/pyspark_demo/*.json")
df3.show()


# COMMAND ----------

# Create a table from json File
spark.sql("CREATE OR REPLACE TEMPORARY VIEW json_table USING json OPTIONS" + 
      " (path 'dbfs:/FileStore/pyspark_demo/json_file1.json')")
spark.sql("select * from json_table").show()

# COMMAND ----------

