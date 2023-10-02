# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/flight_data.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df1 = spark.read.format("csv")\
    .option("inferSchema", "false") \
  .option("header", "false") \
  .option("mode", "FAILFAST") \
  .load("/FileStore/tables/flight_data.csv")

df1.show(2)

# COMMAND ----------

# Create a view or table

temp_table_name = "flight_data_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `flight_data_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "flight_data_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1 = spark.read.format(file_type) \
  .option("inferSchema", "true") \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

import pyspark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType,IntegerType 

# COMMAND ----------

#manual schema

my_schema = StructType([
    StructField("DEST_COUNTRY_NAME",StringType(),True),
    StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
    StructField("count",IntegerType(),True)
])

# COMMAND ----------


df1 = spark.read.format(file_type) \
  .option("inferSchema", "false") \
  .schema(my_schema)\
  .option("header", "false") \
  .option('skipRows',1)\
  .option("mode", "PERMISSIVE") \
  .load(file_location)
#changed from 'FAILFAST' #coz its giving error to read count column null as integer
df1.show(5)

# COMMAND ----------

# CURRUPTED RECORDS
emp_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/employee.csv")

emp_df.show()
# in permissive it show all records

# COMMAND ----------

# CURRUPTED RECORDS
emp_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","DROPMALFORMED")\
    .load("/FileStore/tables/employee.csv")

emp_df.show()

# COMMAND ----------

# # CURRUPTED RECORDS
# emp_df = spark.read.format("csv")\
#     .option("header","true")\
#     .option("inferschema","true")\
#     .option("mode","FAILFAST")\
#     .load("/FileStore/tables/employee.csv")

# emp_df.show()
# #in failfast shows error

# COMMAND ----------

# How to print bad records

from pyspark.sql.types import StringType, StructField, StructType, IntegerType


# COMMAND ----------

emp_schema = StructType(
    [
        StructField("id",IntegerType(),True),
        StructField("name",StringType(),True),
        StructField("age",IntegerType(),True),
        StructField("salary",IntegerType(),True),
        StructField("address",StringType(),True),
        StructField("nominee",StringType(),True),
        StructField("_currupt_record",StringType(),True)
    ]
)

# COMMAND ----------

emp_df1 = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .schema(emp_schema)\
    .load("/FileStore/tables/employee.csv")

emp_df1.show(truncate = False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

#to create new file of bad records
emp_df1 = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .schema(emp_schema)\
    .option("badRecordsPath","/FileStore/tables/Bad_records")\
    .load("/FileStore/tables/employee.csv")

emp_df1.show(truncate = False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/Bad_records/20231001T144109/bad_records/

# COMMAND ----------

# to read that new stored record file

bd = spark.read.format("json")\
    .load("/FileStore/tables/Bad_records/20231001T144109/bad_records/")

bd.show()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,JSON file read
# line delimited json file
spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/line_delimited_json.json")\
    .show()

# COMMAND ----------

# line delimited json file with extra field
spark.read.format("json")\
    .option("inferSchema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/single_file_json_with_extra_fields.json")\
    .show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


