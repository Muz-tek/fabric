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
# META     },
# META     "environment": {
# META       "environmentId": "8aae97dc-dcfa-a4ea-4432-806ea2317726",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Process bronze data into silver delta files and tables
# 
# #### steps:
# 1. display the data and profile each column by looking at distinct values in each column. this can include looking at the frequency of particular values, patterns of missingness etc. can also assess columns for potential skewness which wont pose issues till data increases in size
# 2. cleanse and standardise inconsistencies
# 3. cast columns to appropriate format. <br>
# 3.1. for complex transformation like the dates with many different string formats, I first assessed the types of patterns in the date column. I then assessed the complexitiy, then finally if dates were present in other columns. From this I decided that a date parsing function / udf might a good modularised approach to handing messy dates. <br>
# 3.2. senstive columns like emails, phone numbers and names could be masked with dynamic data masking to increase privacy and data security (silver or gold schemas) <br>
# 4. write dataframe to silver tables on the lakehouse as a delta file partitioned by the ingest_date
# 5. repeat for all bronze tables
# 6. once all data is cleaned and written to silver, I archived all bronze files into an archive folder in Files on the sales_lakehouse/Files/sales_bronze folder


# MARKDOWN ********************

# ### Cleansing methodolgy
# below highlights one example of the approach I took to profiling and understanding the order_items.csv file. I repeated this for all tables.
# 
# display(quantities_df)
# 
# - order_id - main issue is inconsistencies with capitalisation - changed all to upper case
# - product_id - main issue is inconsistencies with capitalisation - changed all to upper case
# - quantity - change all quantites less than zero to 0. cant have a negative amount of something sold. 
# - unit_price_aud - removed all non integer characters
# - discount_pct - converted all % to decimal values. 
# - order_date - many different date format. common in large manual data sets. converted using a date parsing function / udf
# 
# ```
# display(ordered_items.select('product_id').distinct().orderBy(desc("product_id")))
# 
# display(ordered_items.select('order_id').distinct().orderBy(desc("order_id")))
# 
# display(ordered_items.select('quantity').distinct().orderBy(desc("quantity")))
# display(ordered_items.select('*').where("quantity == -1"))
# 
# display(ordered_items.select('unit_price_aud').distinct().orderBy(desc("unit_price_aud")))
# 
# display(ordered_items.select('discount_pct').distinct().orderBy(desc("discount_pct")))
# ```


# MARKDOWN ********************

# #### Define and parameters and libraries

# CELL ********************


# libraries
from pyspark.sql.functions import *
from notebookutils import mssparkutils
import sys, os

# variable library
varLibrary = notebookutils.variableLibrary.getLibrary("sales_variable_library")
workspace = varLibary.workspace
lakehouse = varLibrary.lakehouse

# base parameters
base_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse"
bronze_schema = "sales_bronze"
silver_schema = "sales_silver"

files_path = f"{base_path}/Files/{bronze_schema}"
table_path = f"{base_path}/Tables/{silver_schema}"
silver_path = files_path.replace("bronze", "silver")

##########
# import / run the salesDataProcessor Class function
sys.path.append("/lakehouse/default/Files/code")
# print(os.listdir("/lakehouse/default/Files/code"))

import importlib, sales_silver_functions as s
importlib.reload(s)
DataProcessor = s.salesDataProcessor(spark)

# pipeline parameters - if manually run then define params, else pipeline params
pipeline_run_id, pipeline_name, pipeline_trigger_time = DataProcessor.init_pipeline_params()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


####################################
# silver data frame
# ordered_items - order_items.csv

# Profiling
# data purpose - stores order_id information as well as quantity of units sold, unit price and discount % per product_id.
# observations - data is quite raw, seems like it may be manually updated using excel. System data is generally more standardised than this. 
## 15% of product_ids are missing
### product id's that end in 999 may represent unknown id. most product_ids are in the 100's.
# links to products table via product_id
# fk = order_id 
# fk = product_id
# no pk - create a surrogate pk from order and product ids

# ordered_items - order_items.csv
ordered_items_df = DataProcessor.read_table(lakehouse, bronze_schema, "ordered_items")

silver_ordered_items_df = ordered_items_df \
    .withColumn("order_id", upper("order_id")) \
    .withColumn("product_id", upper("product_id")) \
    .withColumn("ordered_items_id", upper(concat(col("order_id"), lit("_"), col("product_id")))) \
    .withColumn("quantity", when(col("quantity") < 0, 0).otherwise(col("quantity")).cast("integer")) \
    .withColumn("unit_price_aud",
        when(
            regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", "").rlike(r"\.\d{1,2}$"),
            regexp_replace(regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", ""), ",", "")
        ) \
        .when(
            regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", "").rlike(r",\d{1,2}$"),
            regexp_replace(regexp_replace(regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", ""), r"\.", ""), ",", ".")
        ) \
        .otherwise(regexp_replace(col("unit_price_aud"), r"-", ""))
        .cast("decimal(10,2)")) \
    .withColumn("discount_pct", regexp_replace(col("discount_pct"), "%", "")) \
    .withColumn("discount_pct",
        when(col("discount_pct") > 1, col("discount_pct").cast("double") / 100) \
        .otherwise(col("discount_pct").cast("double"))
    ) \
    .selectExpr(
        "order_id",
        "product_id",
        "ordered_items_id",
        "quantity",
        "unit_price_aud",
        "discount_pct",
        "ingest_timestamp"
    )

DataProcessor.silver_incremental_load(
    silver_ordered_items_df, 
    "ordered_items", 
    ["ordered_items_id"], 
    "t.ordered_items_id = s.ordered_items_id",
    silver_schema,
    table_path,
    )

DataProcessor.upsert_audit_table(silver_schema, "ordered_items", pipeline_run_id, pipeline_name, pipeline_trigger_time)

display(ordered_items_df)
display(silver_ordered_items_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# silver dataframe

# profiling observations
## order date significant data quality issues. used udf to parse all mixed date combos.
## similar capitalisation issues as prevs df
## status column highlight order progress. 
# pk = customer_id
# fk = order_id
# fk = sales_rep_id

# most sales 
# SR-02 - 25%
# SR-01 - 14%
# SR-06 - 13%
# Other - 48%

# customer order status - orders_raw.csv
order_status_df = DataProcessor.read_table(lakehouse, bronze_schema, "order_status")

silver_order_status_df = order_status_df \
    .withColumn("order_id", upper("order_id")) \
    .withColumn("order_date_clean", s.parse_mixed_date(col("order_date"))) \
    .withColumn("customer_id", upper("customer_id")) \
    .withColumn("sales_rep_id", upper("sales_rep_id")) \
    .withColumn("status_clean",
        when(lower(col("status")).rlike("^comp"), "completed") \
        .when(lower(col("status")).rlike("^canc"), "cancelled") \
        .otherwise(None)
    ) \
    .selectExpr(
        "order_id",
        "order_date_clean AS order_date",
        "customer_id",
        "sales_rep_id",
        "status_clean AS status",
        "ingest_timestamp"
    )
    
DataProcessor.silver_incremental_load(
    silver_order_status_df, 
    "order_status", 
    ["order_id"], 
    "t.order_id = s.order_id",
    silver_schema,
    table_path,
    )

DataProcessor.upsert_audit_table(silver_schema, "order_status", pipeline_run_id, pipeline_name, pipeline_trigger_time)

display(order_status_df)
display(silver_order_status_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(order_status_df.select("order_date").distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# silver dataframe

# profiling observations
## some email addresses end with .co. this may be a mistake but levave as is till better information is sourced. could be result poorly joined data - unknown
##  the country, state and city columns have mismatched data. in reality would need to talk to SME or business analyst about data quality. 
### For example, canberra is not NSW or New Zealand. Left as is for now.
## join date column contains dates with months dont exist. converted to null where appropriate.
# pk = customer_id
# fk = sate and country can be used to match with region table data

# existing customers - customers_raw.csv
customer_df = DataProcessor.read_table(lakehouse, bronze_schema, "customers")

silver_customer_df = customer_df \
    .withColumn("customer_id", upper("customer_id")) \
    .withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn("country", upper(col("country"))) \
    .withColumn("join_date_clean", s.parse_mixed_date(col("join_date"))) \
    .selectExpr(
        "customer_id",
        "customer_name",
        "email",
        "phone_clean AS phone",
        "country",
        "state",
        "city",
        "postcode",
        "segment",
        "join_date_clean AS join_date",
        "ingest_timestamp"
    )

DataProcessor.silver_incremental_load(
    silver_customer_df, 
    "customers", 
    ["customer_id"], 
    "t.customer_id = s.customer_id",
    silver_schema,
    table_path,
    )

DataProcessor.upsert_audit_table(silver_schema, "customers", pipeline_run_id, pipeline_name, pipeline_trigger_time)

display(customer_df)
display(silver_customer_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# silver dataframe

# profiling observations
## small lookup table. contains product descriptions for product ids
## minor issues with capitalisation and standardisation of unit_prices_aud
## missing PK for azure consumption. would bring this up with business along with other data quality issues. added new product_id
# pk = product_id
# fk = none

# inventory of products - products.csv
products_df = DataProcessor.read_table(lakehouse, bronze_schema, "products")

silver_products_df = products_df \
    .withColumn("product_id",
        when(col("product_name") == "Azure OpenAI Consumption", lit("P-106")) \
        .otherwise(upper(col("product_id")))
    ) \
    .withColumn("unit_price_aud",
        when(
            regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", "").rlike(r"\.\d{1,2}$"),
            regexp_replace(regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", ""), ",", "")
        ) \
        .when(
            regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", "").rlike(r",\d{1,2}$"),
            regexp_replace(regexp_replace(regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", ""), r"\.", ""), ",", ".")
        ) \
        .otherwise(regexp_replace(regexp_replace(col("unit_price_aud"), r"[^0-9,\.\-]", ""), r"-", ""))
        .cast("decimal(10,2)")
    ) \
    .withColumn("flag_clean",
        when(lower(col("active_flag")).rlike("^y"), "Y") \
        .when(lower(col("active_flag")).rlike("^t"), "Y") \
        .otherwise("N")
    ) \
    .selectExpr(
        "product_id",
        "product_name",
        "category",
        "unit_price_aud",
        "flag_clean AS active_flag",
        "ingest_timestamp"
    )

DataProcessor.silver_incremental_load(
    silver_products_df, 
    "products", 
    ["product_id"], 
    "t.product_id = s.product_id",
    silver_schema,
    table_path,
    )

DataProcessor.upsert_audit_table(silver_schema, "products", pipeline_run_id, pipeline_name, pipeline_trigger_time)

display(products_df)
display(silver_products_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# silver dataframe

# profiling observations
## small lookup table. contains product descriptions for product ids
## minor issues with capitalisation and standardisation of unit_prices_aud
# pk = product_id
# fk = none

# sales rep locations - lookup_regions.csv
regions_df = DataProcessor.read_table(lakehouse, bronze_schema, "regions")

silver_regions_df = regions_df \
    .withColumn("gst_rate", regexp_replace(col("gst_rate"), "%", "")) \
    .withColumn("gst_rate",
        when(col("gst_rate") > 1, col("gst_rate").cast("double") / 100) \
        .otherwise(col("gst_rate").cast("double"))) \
    .selectExpr(
        "state",
        "state_name",
        "country",
        "gst_rate",
        "ingest_timestamp"
    )

DataProcessor.silver_incremental_load(
    silver_regions_df, 
    "regions", 
    ["state"], 
    "t.state = s.state",
    silver_schema,
    table_path,
    )

DataProcessor.upsert_audit_table(silver_schema, "regions", pipeline_run_id, pipeline_name, pipeline_trigger_time)

display(regions_df)
display(silver_regions_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# list of sales reps - sales_reps.csv

## dropped dups and non sales reps
## dim table 
## pk rep_id
sales_reps_df = DataProcessor.read_table(lakehouse, bronze_schema, "sales_reps") \

silver_sales_reps_df = sales_reps_df \
    .filter(((col("rep_id") != "sr-02") & (lower(col("rep_name")) != "contractor x"))) \
    .withColumn("rep_id", upper(col("rep_id"))) \
    .withColumn("region_clean", regexp_replace(col("region"), " ", "")) \
    .withColumn("region_clean", upper(col("region_clean"))) \
    .selectExpr(
        "rep_id",
        "rep_name",
        "region_clean AS region",
        "email",
        "ingest_timestamp"
    )

DataProcessor.silver_incremental_load(
    silver_sales_reps_df, 
    "sales_reps", 
    ["rep_id"], 
    "t.rep_id = s.rep_id",
    silver_schema,
    table_path,
    )
    
DataProcessor.upsert_audit_table(silver_schema, "sales_reps", pipeline_run_id, pipeline_name, pipeline_trigger_time)

display(sales_reps_df)
display(silver_sales_reps_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Update Audit Table (only runs if all cells above ran successfully)
