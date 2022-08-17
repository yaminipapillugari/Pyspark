# Databricks notebook source
import pyspark

#Creation of Sparksession

from pyspark.sql import SparkSession
spark= SparkSession.builder.master("local[1]").appName("SparkExample2").getOrCreate()
print(spark.version)
spark.sparkContext

# COMMAND ----------

#Creation of DataFrame

data=[("pen",10),("chocolate",20),("bottle",50),("watch",100)]
columns=["Items","Cost"]
df=spark.createDataFrame(data).toDF(*columns)
df.show()
df1=spark.createDataFrame([(1,"yamini"),(2,"manasa"),(3,"asdf")])
df1.show()


# COMMAND ----------

#Creation of dataframe using SQL quries
df.createOrReplaceTempView("SampleTable")
df1= spark.sql("select * from sampleTable")
df1.show()

# COMMAND ----------

spark.getActiveSession()

# COMMAND ----------

# Spark session and some of its methods
# spark1= spark.newSession("local")
#createDataFrame  method
df1=spark.createDataFrame([(1,"yamini"),(2,"manasa"),(3,"asdf")],("id","name"))
df1.show()

# version attribute
print(spark.version)

# getActiveSession method
spark.getActiveSession()

#range method
df2= spark.range(1,20,4,5)
df2.show()

#Sql method and Creation of dataframe using SQL quries
df1.createOrReplaceTempView("SampleTable")
df4= spark.sql("select id as Identity from sampleTable")
df4.show()

#table method
df5= spark.table("SampleTable")
df5.show()

#read attribute
df3=spark.read.csv("dbfs:/FileStore/pyspark_demo/csv_file.csv")
df3.show()
# spark.stop()


# COMMAND ----------

# Creation of Spark Context and some of its methods

from pyspark import SparkConf, SparkContext
# conf = SparkConf()
# conf.setMaster("local")
sc = SparkContext.getOrCreate()
print(sc.appName)

#sparkuser method
print(sc.sparkUser())

#sparkTracker method
print(sc.statusTracker())

#parallelize method
rdd=sc.parallelize([1,2,3,4],2)
print(rdd.collect())
print(rdd.getNumPartitions())

# Create empty RDD
rdd = spark.sparkContext.emptyRDD()
print(rdd.collect())
print(rdd.isEmpty())

#union method
rdd=sc.range(1,5)
rdd1=sc.parallelize("hello")
rdd2=sc.union([rdd,rdd1])
print(rdd2.collect())

#textFile method
rdd = sc.textFile("dbfs:/FileStore/pyspark_demo/pyspark_methods.txt",2)
print(rdd.collect())


#wholeTxtFiles method
# rdd3 = spark.sparkContext.wholeTextFiles("bfs:/FileStore/pyspark_demo/*")
# rdd3.foreach(f=>{
#     println(f._1+"=>"+f._2)
#   })

# COMMAND ----------

# Create RDD using range

df = spark.range(1, 9,numPartitions=4)
print(df.collect())
print(df.rdd.getNumPartitions())

#range with 1 parameters
rdd1=spark.sparkContext.range(6)
print(rdd1.collect())
print(str(rdd1.getNumPartitions()))

#range with 2 parameters
rdd2=spark.sparkContext.range(4,9)
print(rdd2.collect())
print(str(rdd2.getNumPartitions()))

#range with 3 parameters
rdd3=spark.sparkContext.range(1,9,2)
print(rdd3.collect())
print(str(rdd3.getNumPartitions()))

#range with 4 parameters
rdd4 = sc.range(20,30,2, 2)
rdd4.getNumPartitions()
print(rdd4.collect())
print(str(rdd4.getNumPartitions()))

# COMMAND ----------

# Create RDD using Parallelize function and print no.of partiotions and first element in rdd
# rdd actions
rdd=sc.parallelize([2,3,4,5,6,7,8,9],4)
print(rdd.collect())
print(rdd.count())
print(str(rdd.getNumPartitions()))
print(rdd.first())
print(rdd.take(3))
print(rdd.max())

# Action - reduce
rdd1 = rdd.reduce(lambda a,b: (5+6,8))
print(rdd1)

# saveAsTextFile
rdd.saveAsTextFile("dbfs:/FileStore/pyspark_demo/rdd_textfile1.txt")


# COMMAND ----------

# RDD transformations
df=spark.createDataFrame([("tv",1),("remote",2),("charger",3),("Mobile",4)],("device","code"))
print(df.collect())
from pyspark import SparkConf, SparkContext
# conf = SparkConf()
# conf.setMaster("local")
sc = SparkContext.getOrCreate()
print(sc.appName)


rdd=sc.parallelize([2,3,4,5,6,7,8,9],4)
# filter
rdd2=rdd.filter(lambda a:a in [2 ,5,6,8,10,15,16 ])
print(rdd2.collect())

# flatmap - returns flatten map(more rows)
rdd=sc.parallelize(["yamini reddy","sruthi naidu","manasa pipilli","yamini reddy"])
rdd1=rdd.flatMap(lambda x: x.split(" "))
print(rdd1.collect())

# map - returns an array(same no.of rows)
rdd2=rdd.map(lambda x: x.split(" "))
print(rdd2.collect())

rdd3 = rdd.map(lambda x: (x,1))
print(rdd3.collect())

rdd4 = rdd.map(lambda x: x+"as")
print(rdd4.collect())

# sortByKey



# COMMAND ----------

rdd6=sc.parallelize([(4,"asdf"),(3,"ghj"),(2,"bnvhj"),(1,"zMHg")])
rdd7 = rdd6.sortByKey()
print(rdd7.collect())

# reduceByKey
rdd5 = rdd3.reduceByKey(lambda a,b: a+b)
print(rdd5.collect())

# COMMAND ----------

rdd=sc.parallelize([10,12,14,16,18,20,22,24,26,28])
rdd.collect()

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

# repartition
rp=rdd.repartition(5)
rp.getNumPartitions()


# COMMAND ----------

# coalesce
colsc=rp.coalesce(3)
colsc.getNumPartitions()

# COMMAND ----------

# cache
rdd1=rdd.cache()
rdd1.persist().is_cached

# COMMAND ----------


    
rdd=sc.parallelize(["asd","ader","sfg","yre"])
rdd.collect()
rdd1=rdd.foreach(print)
print(rdd1)
rdd1=sc.parallelize([1,2,3,4,5,6,7])
col= ["id"]
# df2=rdd1.toDF()
# df2.show()

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

from pyspark.sql.functions import col,lit
# Change the type of the column with withColumn method
df1.withColumn("Name",col("Name").cast("Integer")).show()

# Change the column values
df1.withColumn("id",col("id")*5).show()

# Change the name of the column with columnRenamed method
df2=df1.withColumnRenamed("id","Identity").withColumnRenamed("Name","Re-named")

# Create nw column
df2.withColumn("Mail-id",lit("yamini@gmail.com")).show()
df1.printSchema()
df2.printSchema()
df2.show()
df=spark.createDataFrame([("Yamini","F"),("asdf","M"),("sfgf","F")],["Name","Age"])
df.filter(age=='F').show()