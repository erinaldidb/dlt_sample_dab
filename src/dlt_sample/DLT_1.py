# Databricks notebook source
import dlt
import pyspark.sql.functions as fn
import pyspark.sql.types as types

dbconnection = "jdbc:mysql://relational.fit.cvut.cz:3306/SalesDB"
user = "guest"
password = "relational"

tables = ["Employees", "Customers", "Products", "Sales"]
schemas = {
    "Employees": "EmployeeID int, FirstName string, MiddleInitial string, LastName string, operation string, sequenceNum string",
    "Customers": "CustomerID int, FirstName string, MiddleInitial string, LastName string, operation string, sequenceNum string",
    "Products": "ProductID int, Name string, Price float, operation string, sequenceNum string",
    "Sales": "SalesID int, SalesPersonID int, CustomerID int, ProductID int, Quantity int, operation string, sequenceNum string"
}

# COMMAND ----------

def get_rules(table):
  #Rules can also be retrieved from a table
  rules = {
      "Employees" : {"name_not_null":"FirstName IS NOT NULL AND LastName IS NOT NULL"},
      "Customers" : {"name_not_null":"FirstName IS NOT NULL AND LastName IS NOT NULL"},
      "Products" : {"name_not_null":"Name IS NOT NULL"},
      "Sales" : {"quantity_gt_0":"Quantity > 0"}
  }
  return rules[table]

# COMMAND ----------

def generate_raw_tables(table_name):
    @dlt.table(
        name = "bronze_"+table_name,
        table_properties = { 
            "quality": "bronze"
        }
    )
    @dlt.expect_all(get_rules(table_name))
    def generate_table():
        schema = schemas[table_name]
        
        return (spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", "csv")
            .load(f'/Volumes/ema_rina/dlt_sample/landing/{table_name}'))

# COMMAND ----------

for idx, table in enumerate(tables):
    generate_raw_tables(table)
