# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ebd26eb-1475-418b-99a7-cb40fa7db777",
# META       "default_lakehouse_name": "sales_lakehouse",
# META       "default_lakehouse_workspace_id": "f11271a2-a297-421d-8f17-0036535e8757",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ebd26eb-1475-418b-99a7-cb40fa7db777"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Materialized lake views built
# 
# - MLVs are built from the main fact view gold_sales_fact MLV. all other views are derived from that.
# - views are basic views that I thought common sales anaytics would aim to capture
# - refresh schedulled from lakehouse at 6am AEST
# 
# ##### sales_gold.gold_sales_fact
# One row per order line (order × product) with customer/product/rep/region attributes plus calculated amounts (gross/net, discount effects).
# 
# ##### sales_gold.gold_daily_sales
# One row per day summarising orders, order lines, units, and revenue (gross/net).
# 
# ##### sales_gold.gold_monthly_sales
# One row per month (e.g., YYYY-MM) summarising orders, units, and revenue.
# 
# ##### sales_gold.gold_rep_performance
# One row per sales rep per month summarising orders, units, and revenue (leaderboards/performance).
# 
# ##### sales_gold.gold_product_performance
# One row per product per month summarising units, revenue, and order count.
# 
# ##### sales_gold.gold_customer_value
# One row per customer summarising first/last order dates, total orders, units, and revenue (customer KPIs).


# MARKDOWN ********************

# #### Wide fact table

# CELL ********************

# MAGIC %%sql
# MAGIC DROP MATERIALIZED LAKE VIEW IF EXISTS sales_gold.mlv_gold_sales_fact
# MAGIC ;
# MAGIC CREATE MATERIALIZED LAKE VIEW sales_gold.mlv_gold_sales_fact
# MAGIC AS
# MAGIC SELECT
# MAGIC     -- ids
# MAGIC     oi.order_id,
# MAGIC     oi.product_id,
# MAGIC 
# MAGIC     -- order header
# MAGIC     o.customer_id,
# MAGIC     o.sales_rep_id,
# MAGIC     o.order_date,
# MAGIC     o.status,
# MAGIC 
# MAGIC     -- customer attributes
# MAGIC     c.customer_name,
# MAGIC     c.segment,
# MAGIC     c.country     AS customer_country,
# MAGIC     c.state       AS customer_state,
# MAGIC     c.city        AS customer_city,
# MAGIC     c.postcode    AS customer_postcode,
# MAGIC 
# MAGIC     -- product attributes
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC 
# MAGIC     -- sales rep + region attributes
# MAGIC     sr.rep_name,
# MAGIC     sr.region     AS rep_region_state,
# MAGIC     r.state_name  AS rep_region_name,
# MAGIC     r.country     AS rep_region_country,
# MAGIC     r.gst_rate,
# MAGIC 
# MAGIC     -- measures (line)
# MAGIC     oi.quantity,
# MAGIC     CAST(oi.unit_price_aud AS DECIMAL(18,2)) AS unit_price_aud,
# MAGIC     oi.discount_pct,
# MAGIC 
# MAGIC     -- derived measures
# MAGIC     CAST(oi.quantity * oi.unit_price_aud AS DECIMAL(18,2)) AS gross_amount_aud,
# MAGIC     CAST((oi.quantity * oi.unit_price_aud) * (1 - oi.discount_pct) AS DECIMAL(18,2)) AS net_amount_aud,
# MAGIC 
# MAGIC     -- GST (if gst_rate is like 0.10)
# MAGIC     CAST(((oi.quantity * oi.unit_price_aud) * (1 - oi.discount_pct)) * r.gst_rate AS DECIMAL(18,2)) AS gst_amount_aud,
# MAGIC     CAST(((oi.quantity * oi.unit_price_aud) * (1 - oi.discount_pct)) * (1 + r.gst_rate) AS DECIMAL(18,2)) AS net_incl_gst_aud
# MAGIC 
# MAGIC FROM sales_silver.ordered_items oi
# MAGIC JOIN sales_silver.order_status o ON o.order_id = oi.order_id
# MAGIC LEFT JOIN sales_silver.customers c ON c.customer_id = o.customer_id
# MAGIC LEFT JOIN sales_silver.products p ON p.product_id = oi.product_id
# MAGIC LEFT JOIN sales_silver.sales_reps sr ON sr.rep_id = o.sales_rep_id
# MAGIC LEFT JOIN sales_silver.regions r ON r.state = sr.region
# MAGIC WHERE oi.unit_price_aud IS NOT NULL AND oi.quantity IS NOT NULL AND oi.quantity <> 0
# MAGIC ORDER BY order_date DESC
# MAGIC ;
# MAGIC SELECT * FROM sales_lakehouse.sales_gold.mlv_gold_sales_fact LIMIT 1000


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### daily sales summary

# CELL ********************

# MAGIC %%sql
# MAGIC DROP MATERIALIZED LAKE VIEW IF EXISTS sales_gold.mlv_gold_daily_sales
# MAGIC ;
# MAGIC CREATE MATERIALIZED LAKE VIEW sales_gold.mlv_gold_daily_sales AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(DISTINCT order_id)                      AS orders,
# MAGIC   SUM(quantity)                                 AS total_sold,
# MAGIC   CAST(SUM(gross_amount_aud) AS DECIMAL(18,2))  AS gross_aud,
# MAGIC   CAST(SUM(net_amount_aud) AS DECIMAL(18,2))    AS net_aud,
# MAGIC   CAST(SUM(gst_amount_aud) AS DECIMAL(18,2))    AS gst_aud,
# MAGIC   CAST(SUM(net_incl_gst_aud) AS DECIMAL(18,2))  AS net_incl_gst_aud
# MAGIC FROM sales_gold.mlv_gold_sales_fact
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date DESC
# MAGIC ;
# MAGIC SELECT * FROM sales_lakehouse.sales_gold.mlv_gold_daily_sales LIMIT 1000


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### monthly sales summary

# CELL ********************

# MAGIC %%sql
# MAGIC DROP MATERIALIZED LAKE VIEW IF EXISTS sales_gold.mlv_gold_monthly_sales
# MAGIC ;
# MAGIC CREATE MATERIALIZED LAKE VIEW sales_gold.mlv_gold_monthly_sales AS
# MAGIC SELECT
# MAGIC   DATE_FORMAT(order_date, 'yyyy-MM') AS year_month,
# MAGIC   COUNT(DISTINCT order_id) AS orders,
# MAGIC   SUM(quantity) AS total_sold,
# MAGIC   CAST(SUM(net_amount_aud) AS DECIMAL(18,2)) AS net_aud
# MAGIC FROM sales_gold.mlv_gold_sales_fact
# MAGIC GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
# MAGIC ORDER BY  DATE_FORMAT(order_date, 'yyyy-MM') DESC
# MAGIC ;
# MAGIC SELECT * FROM sales_lakehouse.sales_gold.mlv_gold_monthly_sales LIMIT 1000


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Sales rep performance

# CELL ********************

# MAGIC %%sql
# MAGIC DROP MATERIALIZED LAKE VIEW IF EXISTS sales_gold.mlv_gold_rep_performance
# MAGIC ;
# MAGIC CREATE MATERIALIZED LAKE VIEW sales_gold.mlv_gold_rep_performance AS
# MAGIC SELECT
# MAGIC   sales_rep_id,
# MAGIC   rep_name,
# MAGIC   rep_region_state,
# MAGIC   rep_region_name,
# MAGIC   DATE_FORMAT(order_date, 'yyyy-MM') AS year_month,
# MAGIC   COUNT(DISTINCT order_id) AS orders,
# MAGIC   SUM(quantity) AS total_sold,
# MAGIC   CAST(SUM(net_amount_aud) AS DECIMAL(18,2)) AS net_aud
# MAGIC FROM sales_gold.mlv_gold_sales_fact
# MAGIC GROUP BY sales_rep_id, rep_name, rep_region_state, rep_region_name, DATE_FORMAT(order_date, 'yyyy-MM')
# MAGIC ORDER BY DATE_FORMAT(order_date, 'yyyy-MM') DESC
# MAGIC ;
# MAGIC SELECT * FROM sales_lakehouse.sales_gold.mlv_gold_rep_performance LIMIT 1000


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### product sales performance

# CELL ********************

# MAGIC %%sql
# MAGIC DROP MATERIALIZED LAKE VIEW IF EXISTS sales_gold.mlv_gold_product_performance
# MAGIC ;
# MAGIC CREATE MATERIALIZED LAKE VIEW sales_gold.mlv_gold_product_performance AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   DATE_FORMAT(order_date, 'yyyy-MM') AS year_month,
# MAGIC   SUM(quantity) AS total_sold,
# MAGIC   CAST(SUM(net_amount_aud) AS DECIMAL(18,2)) AS net_aud,
# MAGIC   COUNT(DISTINCT order_id) AS orders
# MAGIC FROM sales_gold.mlv_gold_sales_fact
# MAGIC WHERE product_id IS NOT NULL
# MAGIC GROUP BY product_id, product_name, category, DATE_FORMAT(order_date, 'yyyy-MM')
# MAGIC ORDER BY DATE_FORMAT(order_date, 'yyyy-MM') DESC
# MAGIC ;
# MAGIC SELECT * FROM sales_lakehouse.sales_gold.mlv_gold_product_performance LIMIT 1000


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### customer value - customer KPI's

# CELL ********************

# MAGIC %%sql
# MAGIC DROP MATERIALIZED LAKE VIEW IF EXISTS sales_gold.mlv_gold_customer_value
# MAGIC ;
# MAGIC CREATE MATERIALIZED LAKE VIEW sales_gold.mlv_gold_customer_value AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   segment,
# MAGIC   MIN(order_date) AS first_order_date,
# MAGIC   MAX(order_date) AS last_order_date,
# MAGIC   COUNT(DISTINCT order_id) AS orders,
# MAGIC   SUM(quantity) AS total_sold,
# MAGIC   CAST(SUM(net_amount_aud) AS DECIMAL(18,2)) AS net_aud
# MAGIC FROM sales_gold.mlv_gold_sales_fact
# MAGIC GROUP BY customer_id, customer_name, segment
# MAGIC ORDER BY MAX(order_date) DESC
# MAGIC ;
# MAGIC SELECT * FROM sales_lakehouse.sales_gold.mlv_gold_customer_value LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
