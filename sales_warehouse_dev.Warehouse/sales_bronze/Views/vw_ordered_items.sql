-- Auto Generated (Do not modify) EFE2BA31DDFCC632F37762094925581C10D1C40CE94014E3CE30C18EA177A7F0

CREATE   VIEW sales_bronze.vw_ordered_items AS
SELECT * FROM sales_lakehouse_dev.sales_bronze.ordered_items
-- -- GO


-- -- CREATE OR ALTER VIEW sales_bronze.vw_sales_reps AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_bronze.sales_reps
-- -- GO

-- -- CREATE OR ALTER VIEW sales_bronze.vw_audit AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_bronze.audit
-- -- GO


-- -- /* SILVER views (Lakehouse schema: sales_silver) */
-- -- CREATE OR ALTER VIEW sales_silver.vw_customers AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.customers
-- -- GO

-- -- CREATE OR ALTER VIEW sales_silver.vw_products AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.products
-- -- GO

-- -- CREATE OR ALTER VIEW sales_silver.vw_regions AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.regions
-- -- GO

-- -- CREATE OR ALTER VIEW sales_silver.vw_order_status AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.order_status
-- -- GO

-- -- CREATE OR ALTER VIEW sales_silver.vw_ordered_items AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.ordered_items
-- -- GO

-- -- CREATE OR ALTER VIEW sales_silver.vw_sales_reps AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.sales_reps
-- -- GO


-- -- /* GOLD views (pointing to SILVER Lakehouse tables) */
-- -- CREATE OR ALTER VIEW sales_gold.vw_customers AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.customers
-- -- GO

-- -- CREATE OR ALTER VIEW sales_gold.vw_products AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.products
-- -- GO

-- -- CREATE OR ALTER VIEW sales_gold.vw_regions AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.regions
-- -- GO

-- -- CREATE OR ALTER VIEW sales_gold.vw_order_status AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.order_status
-- -- GO

-- CREATE OR ALTER VIEW sales_gold.vw_ordered_items AS
-- SELECT * FROM sales_lakehouse_dev.sales_silver.ordered_items
-- -- GO

-- -- CREATE OR ALTER VIEW sales_gold.vw_sales_reps AS
-- -- SELECT * FROM sales_lakehouse_dev.sales_silver.sales_reps
-- -- GO