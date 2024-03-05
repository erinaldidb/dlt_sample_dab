# Databricks notebook source
# DBTITLE 1,SalesDB from relational.fit.cvut.cz
# MAGIC %md
# MAGIC ![Model](https://relational.fit.cvut.cz/assets/img/datasets-generated/SalesDB.svg)

# COMMAND ----------

dbconnection = "jdbc:mysql://relational.fit.cvut.cz:3306/SalesDB"
user = "guest"
password = "relational"

tables = ["Employees", "Customers", "Products"]
operations = ["UPDATE", "DELETE"]

# COMMAND ----------

#volume used in UC
volume_path = "/Volumes/ema_rina/dlt_sample/landing"
dbutils.fs.ls(volume_path)
#dbutils.fs.rm(volume_path, True) #Cleanup destination folder

# COMMAND ----------

import time
import random
import pyspark.sql.functions as fn
#Replicate Sales parquet as stream
limit = 10
for i in range(600):
    table = random.sample(tables,1)[0]
    df = spark.read.format("jdbc")\
        .options(
            url=dbconnection,
            dbtable = f'(Select * from {table} ORDER BY RAND() LIMIT {limit}) as sale',
            user=user,
            password=password)\
        .load()\
        .withColumn("operation", fn.lit(random.sample(operations,1)[0]))\
        .withColumn("sequenceNum", fn.current_timestamp())\
        .write.format("csv").mode("append").save(f'{volume_path}/{table}')
    time.sleep(20)
