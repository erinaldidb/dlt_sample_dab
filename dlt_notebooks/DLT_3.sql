-- Databricks notebook source
CREATE TEMPORARY LIVE VIEW expensive_products_vw AS
Select * from live.silver_products where Price > 100

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW enriched_sales_vw AS 
SELECT s.SalesID as sales_id, e.FirstName as seller_first_name, e.LastName as seller_last_name, p.Name as product_name, s.Quantity as selled_quantity, p.Price as product_price, p.ProductID as product_id
FROM LIVE.bronze_sales s
LEFT JOIN LIVE.silver_customers c on s.CustomerID = c.CustomerID and s.sequenceNum >= c.`__START_AT` and s.sequenceNum <= coalesce(c.`__END_AT`, date("2099-01-01"))
LEFT JOIN LIVE.silver_products p on s.ProductID = p.ProductID and s.sequenceNum >= p.`__START_AT` and s.sequenceNum <= coalesce(p.`__END_AT`, date("2099-01-01"))
LEFT JOIN LIVE.silver_employees e on s.SalesPersonID = e.EmployeeID and s.sequenceNum >= e.`__START_AT` and s.sequenceNum <= coalesce(e.`__END_AT`, date("2099-01-01"))

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE total_exp_prod_selled AS
SELECT s.product_id, s.product_name, SUM(s.selled_quantity) as selled_quantity, s.product_price as price FROM LIVE.enriched_sales_vw s
INNER JOIN LIVE.expensive_products_vw ep on s.product_id = ep.ProductId
group by s.product_id, s.product_name, s.product_price

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE most_profitable_sales AS
SELECT *, s.selled_quantity * s.product_price as profit FROM LIVE.enriched_sales_vw s
where s.product_id != 276
order by profit desc limit 100
