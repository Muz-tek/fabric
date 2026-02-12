# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ebd26eb-1475-418b-99a7-cb40fa7db777",
# META       "default_lakehouse_name": "sales_lakehouse_dev",
# META       "default_lakehouse_workspace_id": "f11271a2-a297-421d-8f17-0036535e8757",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ebd26eb-1475-418b-99a7-cb40fa7db777"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- create bronze audit table
# MAGIC CREATE OR REPLACE TABLE sales_lakehouse_dev.dbo.audit
# MAGIC (
# MAGIC     layer STRING,
# MAGIC     table_name STRING,
# MAGIC     folder STRING,
# MAGIC     pipeline_run_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     pipeline_trigger_time STRING,
# MAGIC     watermark TIMESTAMP
# MAGIC )
# MAGIC ;
# MAGIC -- DROP TABLE sales_lakehouse_dev.dbo.audit

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tables = [
#     ("customers", "Files/sales_bronze/customers/customers_raw.csv"),
#     ("order_quantities", "Files/sales_bronze/order_quantities/order_items_raw.csv"),
#     ("order_status", "Files/sales_bronze/order_status/orders_raw.csv"),
#     ("products", "Files/sales_bronze/products/products_raw.csv"),
#     ("regions", "Files/sales_bronze/regions/lookup_regions.csv"),
#     ("sales_reps", "Files/sales_bronze/sales_reps/sales_reps_raw.csv"),
# ]

# for name, path in tables:
#     df = (spark.read
#           .format("csv")
#           .option("header", "true")
#           .option("inferSchema", "true")
#           .load(path))

#     (df.write
#        .format("delta")
#        .mode("overwrite")
#        .saveAsTable(f"sales_bronze.{name}"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# config for pipeline config file

[
  {
    "name": "customers",
    "run_type": "daily",
    "source_path": "customers",
    "archive_path": "archive/customers",
    "bronze": "sales_bronze",
    "silver": "sales_silver",
    "gold": "sales_gold"
  },
  {
    "name": "order_quantities",
    "run_type": "daily",
    "source_path": "order_quantities",
    "archive_path": "archive/order_quantities",
    "bronze": "sales_bronze",
    "silver": "sales_silver",
    "gold": "sales_gold"
  },
  {
    "name": "order_status",
    "run_type": "daily",
    "source_path": "order_status",
    "archive_path": "archive/order_status",
    "bronze": "sales_bronze",
    "silver": "sales_silver",
    "gold": "sales_gold"
  },
  {
    "name": "regions",
    "run_type": "daily",
    "source_path": "regions",
    "archive_path": "archive/regions",
    "bronze": "sales_bronze",
    "silver": "sales_silver",
    "gold": "sales_gold"
  },
  {
    "name": "sales_reps",
    "run_type": "daily",
    "source_path": "sales_reps",
    "archive_path": "archive/sales_reps",
    "bronze": "sales_bronze",
    "silver": "sales_silver",
    "gold": "sales_gold"
  },
  {
    "name": "products",
    "run_type": "daily",
    "source_path": "products",
    "archive_path": "archive/products",
    "bronze": "sales_bronze",
    "silver": "sales_silver",
    "gold": "sales_gold"
  }
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
