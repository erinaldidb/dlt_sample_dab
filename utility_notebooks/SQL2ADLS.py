# Databricks notebook source
# DBTITLE 1,SalesDB from relational.fit.cvut.cz
# MAGIC %md
# MAGIC ![Model](https://relational.fit.cvut.cz/assets/img/datasets-generated/SalesDB.svg)

# COMMAND ----------

dbconnection = "jdbc:mysql://relational.fit.cvut.cz:3306/SalesDB"
user = "guest"
password = "relational"

tables = ["Employees", "Customers", "Products"]

# COMMAND ----------

dfs = [spark.read.format("jdbc")\
        .options(
            url=dbconnection,
            dbtable = table,
            user=user,
            password=password).load() for table in tables]

# COMMAND ----------

#volume used in UC
volume_path = "/Volumes/ema_rina/dlt_sample/landing"
dbutils.fs.ls(volume_path)
dbutils.fs.rm(volume_path, True) #Cleanup destination folder

# COMMAND ----------

import pyspark.sql.functions as fn
#Replicate dim as csv ( no header so we can test get schema from jdbc )
for idx, table in enumerate(tables):
    dfs[idx]\
    .withColumn("operation", fn.lit("INSERT"))\
    .withColumn("sequenceNum", fn.current_timestamp())\
    .write.format("csv").mode("overwrite").save(f'{volume_path}/{table}')

# COMMAND ----------

import time
import pyspark.sql.functions as fn
#Replicate Sales parquet as stream
limit = 1000
dbutils.fs.rm(f'{volume_path}/Sales', True) #Cleanup destination folder
for i in range(6000):
    df = spark.read.format("jdbc")\
        .options(
            url=dbconnection,
            dbtable = f'(Select * from Sales ORDER BY RAND() LIMIT {limit}) as sale',
            user=user,
            password=password)\
        .load()\
        .withColumn("operation", fn.lit("INSERT"))\
        .withColumn("sequenceNum", fn.current_timestamp())\
        .write.format("csv").mode("append").save(f'{volume_path}/Sales')
    time.sleep(60)
