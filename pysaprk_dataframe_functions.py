# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[1]").appName("pysparkExample4").getOrCreate()

# COMMAND ----------

# Creation of dataframe and convert rdd to dataframe and show function
# 1.from rdd
rdd=sc.range(5)
rdd.collect()
df=rdd.toDF("Integer")
df.show()

# 2.from list collection
data=[("Google","Sundar"),("Microsoft","satyaa"),("Infosys","Murthy")]
df1=spark.createDataFrame(data).toDF("Company","CEO")
df1.show()

# 3.using datasources
df2=spark.read.option("header","true").csv("dbfs:/FileStore/pyspark_demo/csv_file.csv")
df2.show()

# Creation of emptyRDD and empty dataframe
rdd1=sc.emptyRDD()
print(rdd1)
df4 = spark.createDataFrame(rdd1,schema)
df4.printSchema()
df4.show()

from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])
df3=spark.createDataFrame([],schema)
df3.printSchema()


# COMMAND ----------

# StructType abd StructField
from pyspark.sql.types import *
data=[("James",23),("Ann",40)]
schemaval= StructType([\
  StructField("Name",StringType(),True),\
  StructField("Id",IntegerType(),True)
]) 
df=spark.createDataFrame(data,schemaval)
df.show()

# Creation of struct array and map schema
arrayStructureSchema = StructType([
    StructField('name', StructType([
       StructField('firstname', StringType(), True),
       StructField('middlename', StringType(), True),
       StructField('lastname', StringType(), True)
       ])),
       StructField('hobbies', ArrayType(StringType()), True),
       StructField('properties', MapType(StringType(),StringType()), True)
    ])


# COMMAND ----------

# row class
from pyspark.sql import Row
data=[Row("James",23),Row("Anna",40)]
# columns=["Name","Roll_no"]
df=spark.createDataFrame(data).toDF("Name","Roll_no")
df.printSchema()
df.show()

rdd=sc.parallelize(data)
collData=rdd.collect()
print(collData)

# Person=Row("name","lang","state")
# data = [Person("James,,Smith",["Java","Scala","C++"],"CA"), 
#     Person("Michael,Rose,",["Spark","Java","C++"],"NJ"),
#     Person("Robert,,Williams",["CSharp","VB"],"NV")]

# columns = ["name","languagesAtSchool","currentState"]
# df=spark.createDataFrame(data)
# df.printSchema()
# df.show()

# COMMAND ----------

# column class
data=[("James",23),("Ann",40)]
df=spark.createDataFrame(data).toDF("name.fname","gender")
df.printSchema()

# Using DataFrame object (df)
df.select(df.gender).show()
df.select(df["gender"]).show()

#Accessing column name with dot (with backticks)
df.select(df["`name.fname`"]).show()

#Using SQL col() function
from pyspark.sql.functions import col
df.select(col("gender")).show()

#Accessing column name with dot (with backticks)
df.select(col("`name.fname`")).show()


# COMMAND ----------

# createDataFrame,show and printSchema methods
df1 = spark.createDataFrame([(1,"asd"),(2,"sdf"),(3,"dfg")],("id","Name"))
df1.printSchema()
df1.show()

# Select and collect methods
from pyspark.sql.functions import col 
df1.select("*").show()
df1.select([col for col in df1.columns]).show()
df1.select(df1.Name).show()
df1.collect()

# COMMAND ----------

# withColumn, withColumnRenamed

df1 = spark.createDataFrame([(1,"asd"),(2,"sdf"),(3,"dfg")],("id","Name"))
# df1.printSchema()
# df1.show()

from pyspark.sql.functions import col,lit
#ex1: Change the type of the column with withColumn method
df1.withColumn("Name",col("Name").cast("Integer")).printSchema()

#ex2: Change the column values
df1.withColumn("id",col("id")*5).show()

# Change the name of the column with columnRenamed method
df2=df1.withColumnRenamed("id","Identity").withColumnRenamed("Name","Re-named")

#ex3: Create nw column
df2.withColumn("Mail-id",lit("yamini@gmail.com")).show()
df1.printSchema()
df2.printSchema()
df2.show()

# COMMAND ----------

# filter method
from pyspark.sql.functions import col
df=spark.createDataFrame([("swetha","F"),("asdf","M"),("sfgf","F")],["Name","sex"])
df.filter(col('sex')=='F').show()
df.filter("sex=='M'").show()
df.filter((df.Name=='swetha') & (df.sex=='F')).show()

# where method
df.where(df.Name=='swetha').show()

# COMMAND ----------

# distinct method
data = [("James", "Sales", 3000, 1), \
    ("Michael", "Sales", 4600, 2), \
    ("Robert", "Sales", 4100, 3), \
    ("Maria", "Finance", 3000, 4), \
    ("James", "Sales", 3000, 5), \
    ("Scott", "Finance", 3300,6), \
    ("Jen", "Finance", 3900, 7), \
    ("Jeff", "Marketing", 3000, 8), \
    ("Kumar", "Marketing", 2000, 9), \
    ("Saif", "Sales", 4100, 0) \
  ]
columns= ["employee_name", "department", "salary", "samp"]
df = spark.createDataFrame(data = data, schema = columns)
# df.printSchema()
# df.show(truncate=False)

# drop
df.drop(df.samp)
df.show()
# <-- showing error output -->
print(df.distinct().count()) 
print(df.count())
print(df.dropDuplicates(["salary"]).count())

# dropDuplicates
dropDisDF = df.dropDuplicates(["department"])
dropDisDF.show()
print("Distinct count of department & salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)


# COMMAND ----------

# Sort - more efficient, orderBy - order od data is guaranted and groupBy methods
df.sort(df.department.desc(),df.salary).show()
df.orderBy(col('department'),col("employee_name")).show()
df.groupBy("department").sum("salary").show()

# COMMAND ----------

# UDF intro
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

def convertSeq(val):
    val=val+1
    return val
convertudf= udf(lambda x:convertSeq(x))
df.withColumn("convert_seq_no",convertudf("Name"))
    

# COMMAND ----------

# sample
df=spark.range(50)
df.sample(True,0.1,2).collect()

# COMMAND ----------

# na.fill method
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    (None, "tracey smith"),
    ("3", None)]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)
df.na.fill("default").show()

# COMMAND ----------

# partitionBy
df.show()
df.write.option("header",True).partitionBy("Name").csv("dbfs:/tmp/partition_file1.csv")


# COMMAND ----------

