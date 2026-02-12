-- Auto Generated (Do not modify) CA524807B6AD26B44E73B916A0A3A4DC4C2654E83EA22EA96EC8D141D86B10BD
-- /* Run this in sales_warehouse_dev */
-- /* BRONZE views (Lakehouse schema: sales_bronze) */
-- CREATE OR ALTER VIEW sales_bronze.vw_customers AS
-- SELECT * FROM sales_lakehouse_dev.sales_bronze.customers
-- GO

-- CREATE OR ALTER VIEW sales_bronze.vw_products AS
-- SELECT * FROM sales_lakehouse_dev.sales_bronze.products
-- GO

-- CREATE OR ALTER VIEW sales_bronze.vw_regions AS
-- SELECT * FROM sales_lakehouse_dev.sales_bronze.regions
-- GO

-- CREATE OR ALTER VIEW sales_bronze.vw_order_status AS
-- SELECT * FROM sales_lakehouse_dev.sales_bronze.order_status
-- GO

-- CREATE OR ALTER VIEW sales_bronze.vw_order_quantities AS
-- SELECT * FROM sales_lakehouse_dev.sales_bronze.order_quantities
-- GO

-- CREATE OR ALTER VIEW sales_bronze.vw_sales_reps AS
-- SELECT * FROM sales_lakehouse_dev.sales_bronze.sales_reps
-- GO

-- CREATE OR ALTER VIEW sales_bronze.vw_audit AS
-- SELECT * FROM sales_lakehouse_dev.sales_bronze.audit
-- GO


-- /* SILVER views (Lakehouse schema: sales_silver) */
-- CREATE OR ALTER VIEW sales_silver.vw_customers AS
-- SELECT * FROM sales_lakehouse_dev.sales_silver.customers
-- GO

-- CREATE OR ALTER VIEW sales_silver.vw_products AS
-- SELECT * FROM sales_lakehouse_dev.sales_silver.products
-- GO

-- CREATE OR ALTER VIEW sales_silver.vw_regions AS
-- SELECT * FROM sales_lakehouse_dev.sales_silver.regions
-- GO

-- CREATE OR ALTER VIEW sales_silver.vw_order_status AS
-- SELECT * FROM sales_lakehouse_dev.sales_silver.order_status
-- GO

CREATE   VIEW sales_silver.vw_ordered_items AS
SELECT * FROM sales_lakehouse_dev.sales_silver.ordered_items