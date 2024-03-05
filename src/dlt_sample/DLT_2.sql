-- Databricks notebook source
CREATE OR REPLACE STREAMING LIVE VIEW transform_1_employees AS
SELECT EmployeeID, initcap(FirstName) AS FirstName, MiddleInitial, initcap(LastName) AS LastName, operation, sequenceNum FROM stream(live.bronze_employees)

-- COMMAND ----------

CREATE OR REPLACE STREAMING LIVE VIEW transform_2_employees AS
SELECT *, md5(cast(concat(FirstName, LastName) as binary)) as md5_name FROM stream(live.transform_1_employees)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_employees
TBLPROPERTIES (
  "pipelines.autoOptimize.zOrderCols" = "EmployeeID",
  "quality" = "silver"
  );

APPLY CHANGES INTO
  live.silver_employees
FROM
  stream(live.transform_2_employees)
KEYS
  (EmployeeID)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
--COLUMNS * EXCEPT
--  (operation, sequenceNum)
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_customers
TBLPROPERTIES (
  "pipelines.autoOptimize.zOrderCols" = "CustomerID",
  "quality" = "silver"
  );

APPLY CHANGES INTO
  live.silver_customers
FROM
  stream(live.bronze_customers)
KEYS
  (CustomerID)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
--COLUMNS * EXCEPT
--  (operation, sequenceNum)
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_products
TBLPROPERTIES (
  "pipelines.autoOptimize.zOrderCols" = "ProductID",
  "quality" = "silver"
  );
  
APPLY CHANGES INTO
  live.silver_products
FROM
  stream(live.bronze_products)
KEYS
  (ProductID)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
--COLUMNS * EXCEPT
--  (operation, sequenceNum)
STORED AS
  SCD TYPE 2;
