# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pysparkExample2.com').getOrCreate()
data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,100000),("Maria","F",500000),
        ("Jen","",None)]
spark.version
spark.sparkContext


# COMMAND ----------

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

# COMMAND ----------

# When and otherwise functions
from pyspark.sql.functions import *
df1=df.withColumn("sal",when(df.salary<100000,"low").when(df.salary==100000,"medium").when(df.salary>100000,"high").when(df.salary.isNull()," ").otherwise(df.salary))
df1.show()

# Using select and when
df2=df.select("*",when(df.salary<100000,"low").when(df.salary==100000,"medium").when(df.salary>100000,"high").when(df.salary.isNull()," ").otherwise(df.salary).alias("sal_catagory"))
df2.show()
# case when else end with expr
df3= df.withColumn("sal",expr("CASE WHEN salary<100000 THEN 'low' "+"WHEN salary==100000 THEN 'medium'"+ "WHEN salary>100000 THEN 'high'"+"ELSE salary END"))
df3.show()

# using case when on sql expression
df.createOrReplaceTempView("emp_view")
df4=spark.sql("select *,CASE WHEN salary<100000 THEN 'low' "+"WHEN salary==100000 THEN 'medium'"+ "WHEN salary>100000 THEN 'high'"+"ELSE salary END as sal_cat from emp_view ")
df4.show()

# COMMAND ----------

# exp function 
# ex1:alias with
df.show()
df1=df.select("name","salary",expr("salary+5000").alias("sal_increment"))
df1.show()
# ex2:alias name in exp
df2=df.select("name","salary",expr("salary+5000 as sal_increment"))
df2.show()

# ex3:case with exp
df3= df.withColumn("sal",expr("CASE WHEN salary<100000 THEN 'low' "+"WHEN salary==100000 THEN 'medium'"+ "WHEN salary>100000 THEN 'high'"+"ELSE salary END"))
df3.show()


# COMMAND ----------

# lit funtion
from pyspark.sql.functions import col
df.show()
# ex1:
df1=df.withColumn("lit_gender_status",lit("YES"))
df1.show()
# ex2:
df2=df.withColumn("lit_gender_status",when(col("gender")=='M',lit("YES")).otherwise("NO"))
df2.show()

# COMMAND ----------

# Split function
from pyspark.sql.functions import col

data=[(1,"yamini papillgari"),(2,"Anusha bheemagoni"),(3,"sravani pemmaka"),(4,"swetha yellala")]
df=spark.createDataFrame(data).toDF("Id","Name")
df.show()
df.printSchema()
  
# Creation of an array using split method
df1=df.select(split(col("Name")," ").alias("Name_array")).drop("Name")
df1.printSchema()
df1.show()



# COMMAND ----------

# concat_ws method: Convert array type to string
df1.show()
from pyspark.sql.functions import *
df2=df1.withColumn("Name_string",concat_ws(" ",col("Name_array")))
df2.show()
df2.printSchema()

# Using same function with sql expression
df1.createOrReplaceTempView("ARRAY_STRING")
spark.sql("select Name_array, concat_ws(' ',Name_array) as NameString from ARRAY_STRING") \
    .show(truncate=False)

# COMMAND ----------

# substring method - fetch part of a string
data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]
df=spark.createDataFrame(data,columns)
df.show()

# Using withColumn
from pyspark.sql.functions import *
df.withColumn('year', substring('date', 1,4))\
    .withColumn('month', substring('date', 5,2))\
    .withColumn('day', substring('date', 7,2)).show()
df.printSchema()

# Using select
df1=df.select("date",substring('date', 1,4).alias("year"),substring('date', 5,2).alias("Month"),substring('date', 7,2).alias("Date"))
df1.show()

#Using with selectExpr
df2=df.selectExpr('date', 'substring(date, 1,4) as year', \
                  'substring(date, 5,2) as month', \
                  'substring(date, 7,2) as day')

#Using substr from Column type
df3=df.withColumn('year', col('date').substr(1, 4))\
  .withColumn('month',col('date').substr(5, 2))\
  .withColumn('day', col('date').substr(7, 2))
df3.show()


# COMMAND ----------

# regexp_replace, translate and overlay - used to replace the column values with other string or other column values

from pyspark.sql import SparkSession
address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]

df =spark.createDataFrame(address,["id","address","state"])
df.show()

#Replace string
from pyspark.sql.functions import regexp_replace
df.withColumn('address', regexp_replace('address', 'Rd', 'Road')).show(truncate=False)

#Replace string
from pyspark.sql.functions import when
df.withColumn('address', 
      when(df.address.endswith('Rd'),regexp_replace(df.address,'Rd','Road')) \
     .when(df.address.endswith('St'),regexp_replace(df.address,'St','Street')) \
     .when(df.address.endswith('Ave'),regexp_replace(df.address,'Ave','Avenue')) \
     .otherwise(df.address)) \
     .show(truncate=False)   

#Using translate - it works on characters 
from pyspark.sql.functions import translate
df.withColumn('address', translate('address', '123', 'ABC')) \
  .show(truncate=False)

#Replace column with another column
from pyspark.sql.functions import expr
df = spark.createDataFrame([("ABCDE_XYZ", "XYZ","FGH")], ("col1", "col2","col3"))
df.withColumn("new_column",
              expr("regexp_replace(col1, col2, col3)")
              .alias("replaced_value")
              ).show()
  
#Overlay - Replace column value with a string value from another column.
from pyspark.sql.functions import overlay
df = spark.createDataFrame([("ABCDE_XYZ123", "FGH")], ("col1", "col2"))
df.select(overlay("col1", "col2", 7).alias("overlayed")).show()


# COMMAND ----------

# current_date, datediff, months_between, to_date
data = [("1","2019-07-01"),("2","2019-06-24"),("3","2019-08-24")]

df=spark.createDataFrame(data=data,schema=["id","date"])
df.show()
df.printSchema()

from pyspark.sql.functions import *
df.select(
      col("date"),
      current_date().alias("current_date"),
      datediff(current_date(),col("date")).alias("datediff")
    ).printSchema()

df.withColumn("datesDiff", datediff(current_date(),col("date"))) \
      .withColumn("montsDiff", months_between(current_date(),col("date"))) \
      .withColumn("montsDiff_round",round(months_between(current_date(),col("date")),2)) \
      .withColumn("yearsDiff",months_between(current_date(),col("date"))/lit(12)) \
      .withColumn("yearsDiff_round",round(months_between(current_date(),col("date"))/lit(12),2)) \
      .show()

# to_date
data2 = [("1","07-01-2019"),("2","06-24-2019"),("3","08-24-2019")]  
df2=spark.createDataFrame(data=data2,schema=["id","date"])
df2.printSchema()
df2.select(
    to_date(col("date"),"MM-dd-yyyy").alias("date"),
    current_date().alias("endDate")
    ).printSchema()

spark.sql("select round(months_between('2019-07-01',current_date())/12,2) as years_diff").show()



# COMMAND ----------

# to_timestamp
df3=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df3.printSchema()
df4=df3.select("input_timestamp",to_timestamp("input_timestamp").alias("timestamp"))
df4.show()
df4.printSchema()

# COMMAND ----------

# to_date and date_format, current_timestamp
df=spark.sql("select to_date('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as date_type")
df.printSchema()
df.select(current_timestamp(),current_date(),date_format(current_date(),"MMMM-dd-yyyy"),date_format(current_date(),"MMM-dd-yyyy")).show(truncate=False)

# COMMAND ----------

# Array type functions: split(),array(),array_contains(),explode()


# explode and flatten
arraydf= spark.createDataFrame([("sai",[["maths,science"],["biology,english"]]),("manu",[["java,c,c++"],["html,css,js"]])]).toDF("Name","Subjects")
arraydf.show(truncate=False)
arraydf.printSchema()
arraydf.select("Name",explode("subjects").alias("explode_sub")).show()
arraydf.select("Name",flatten("subjects").alias("flatten_sub")).show(truncate=False)

# COMMAND ----------

# array_contains
from pyspark.sql.functions import array_contains
df=arraydf.select("Name",flatten("subjects").alias("flatten_sub"))
df.show(truncate=False)
df.printSchema()
df.show()
df.select("Name",array_contains("flatten_sub","chemistry")
    .alias("array_contains")).show()
df.select("flatten_sub").show(truncate=False)


# COMMAND ----------

# array() - merge column values
df1=spark.createDataFrame([("asdff","hjk"),("sdgd","cxzhg")]).toDF("Previous_name","Current_name")
df1.show()
df1.select(array("Previous_name","Current_name").alias("Names")).printSchema()

# COMMAND ----------

# array_contains
from pyspark.sql.types import StringType, ArrayType
arrayCol = ArrayType(StringType(),False)


data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()


from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java")
    .alias("array_contains")).show()



# COMMAND ----------

# collect_list,collect_set,countDistinct
from pyspark.sql.functions import *
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
# df.printSchema()
# df.show(truncate=False)

# collect_list - include duplicates
df.select(collect_list("salary")).show(truncate=False)
# collect_set - return list after excluding duplicates
df.select(collect_set("salary")).show(truncate=False)

#countDistinct
df2 = df.select(countDistinct("department"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: "+str(df2.collect()[0][0]))


# COMMAND ----------

# maptype functions: explode, map_keys, map_values
# create_map - used to create column with map type
from pyspark.sql.functions import *
df.show()
df1= df.select("employee_name","department",create_map(lit("salary"),"salary").alias("salary_map"))
df1.show()
df1.printSchema()
# explode - prints map into keyvalue pair
df1.select(df1.department,explode("salary_map")).show()
# map_keys - prints all keys
df1.select(df1.department,map_keys("salary_map")).show()
# map_values - print values for the column
df1.select(df1.department,map_values("salary_map")).show()



# COMMAND ----------

# struct - creates struct type column
from pyspark.sql.functions import col,struct,when
updatedDF = df.withColumn("OtherInfo", 
    struct(col("employee_name").alias("Name"),
    col("department").alias("dept"),
    col("salary").alias("salary"),
    when(col("salary").cast("Integer") < 3000,"Low")
      .when(col("salary").cast("Integer") < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  ))

updatedDF.printSchema()
updatedDF.show(truncate=False)


# COMMAND ----------

# Window functions: row_number,rank ,dense_rank

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
.withColumn("rank",rank().over(windowSpec)) \
.withColumn("dense_rank",dense_rank().over(windowSpec)) \
.show(truncate=False)

# sum,avg
df.select(sum("salary"),avg("salary")).show()

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