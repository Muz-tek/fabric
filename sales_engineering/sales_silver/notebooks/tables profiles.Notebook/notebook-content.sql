-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "4ebd26eb-1475-418b-99a7-cb40fa7db777",
-- META       "default_lakehouse_name": "sales_lakehouse_dev",
-- META       "default_lakehouse_workspace_id": "f11271a2-a297-421d-8f17-0036535e8757",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "4ebd26eb-1475-418b-99a7-cb40fa7db777"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- ### general profiling and checks
-- - pk and fk relationships, potential need for surrogate keys
-- - orphaned records
-- - strange looking ids or nulls


-- CELL ********************

---- Customers table

/*
Notes things to check with business:
customer_id C-999 may not represent an order.
PK-FK: customers --> ordered_items 1 to 0 or many relationships 
*/

SELECT 
    *
FROM sales_silver.customers c
LEFT JOIN sales_silver.order_status os ON c.customer_id = os.customer_id
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_orders
FROM sales_silver.order_status os
LEFT JOIN sales_silver.customers c ON os.customer_id = c.customer_id
WHERE os.customer_id IS NOT NULL AND c.customer_id IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_orders
FROM sales_silver.order_status os
LEFT JOIN sales_silver.customers c ON os.customer_id = c.customer_id
WHERE os.customer_id IS NOT NULL AND c.customer_id IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    COUNT(*) AS customers_with_no_orders,
    c.customer_id
FROM sales_silver.customers c
LEFT JOIN sales_silver.order_status os ON c.customer_id = os.customer_id
WHERE os.order_id IS NULL
GROUP BY c.customer_id
;
SELECT 
    *
    -- COUNT(*) AS customers_with_no_orders
FROM sales_silver.customers c
LEFT JOIN sales_silver.order_status os ON c.customer_id = os.customer_id
-- WHERE os.order_id IS NULL;
WHERE os.order_id IS NOT NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

---- Order_status table

/*
Notes things to check with business:
- An order_id should typically have 1 or more ordered_items (1 to 1..many).
PK-FK: order_status --> ordered_items 1 to 0 or many relationships
PK-FK: order_status --> customers (many orders to 1 customer)
PK-FK: order_status --> sales_reps (many orders to 1 rep)
*/

SELECT 
    *
FROM sales_silver.order_status os
LEFT JOIN sales_silver.ordered_items oi ON os.order_id = oi.order_id
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_order_lines
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.order_status os ON oi.order_id = os.order_id
WHERE oi.order_id IS NOT NULL AND os.order_id IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_order_lines
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.order_status os ON oi.order_id = os.order_id
WHERE oi.order_id IS NOT NULL AND os.order_id IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    -- COUNT(*) AS orders_with_no_lines,
    *
FROM sales_silver.order_status os
LEFT JOIN sales_silver.ordered_items oi ON os.order_id = oi.order_id
WHERE oi.order_id IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orders_with_no_lines
FROM sales_silver.order_status os
LEFT JOIN sales_silver.ordered_items oi ON os.order_id = oi.order_id
WHERE oi.order_id IS NULL;
-- WHERE oi.order_id IS NOT NULL
;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


---- Ordered_items table

/*
Notes things to check with business:
- ordered_items should have valid order_id and product_id for all records.
- If (order_id, product_id) is intended to be unique (quantity holds multiples), confirm duplicates are not valid.
PK-FK: ordered_items --> order_status (many ordered_items to 1 order)
PK-FK: ordered_items --> products (many ordered_items to 1 product)
*/

SELECT 
    *
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.order_status os ON oi.order_id = os.order_id
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_order_lines
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.order_status os ON oi.order_id = os.order_id
WHERE oi.order_id IS NOT NULL AND os.order_id IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_order_lines
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.order_status os ON oi.order_id = os.order_id
WHERE oi.order_id IS NOT NULL AND os.order_id IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    COUNT(*) AS orders_with_no_lines,
    os.order_id
FROM sales_silver.order_status os
LEFT JOIN sales_silver.ordered_items oi ON os.order_id = oi.order_id
WHERE oi.order_id IS NULL
GROUP BY os.order_id
;
SELECT 
    *
    -- COUNT(*) AS orders_with_no_lines
FROM sales_silver.order_status os
LEFT JOIN sales_silver.ordered_items oi ON os.order_id = oi.order_id
-- WHERE oi.order_id IS NULL;
WHERE oi.order_id IS NOT NULL
;

SELECT 
    *
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.products p ON oi.product_id = p.product_id
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_order_lines
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.products p ON oi.product_id = p.product_id
WHERE oi.product_id IS NOT NULL AND p.product_id IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_order_lines
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.products p ON oi.product_id = p.product_id
WHERE oi.product_id IS NOT NULL AND p.product_id IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    COUNT(*) AS products_with_no_lines,
    p.product_id
FROM sales_silver.products p
LEFT JOIN sales_silver.ordered_items oi ON p.product_id = oi.product_id
WHERE oi.product_id IS NULL
GROUP BY p.product_id
;
SELECT 
    *
    -- COUNT(*) AS products_with_no_lines
FROM sales_silver.products p
LEFT JOIN sales_silver.ordered_items oi ON p.product_id = oi.product_id
-- WHERE oi.product_id IS NULL;
WHERE oi.product_id IS NOT NULL
;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************



---- Products table

/*
Notes things to check with business:
- product_id should exist for all products; null/blank product_id should be quarantined or assigned a new id.
PK-FK: products --> ordered_items 1 to 0 or many relationships
*/

SELECT 
    *
FROM sales_silver.products p
LEFT JOIN sales_silver.ordered_items oi ON p.product_id = oi.product_id
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_order_lines
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.products p ON oi.product_id = p.product_id
WHERE oi.product_id IS NOT NULL AND p.product_id IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_order_lines
FROM sales_silver.ordered_items oi
LEFT JOIN sales_silver.products p ON oi.product_id = p.product_id
WHERE oi.product_id IS NOT NULL AND p.product_id IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    COUNT(*) AS products_with_no_lines,
    p.product_id
FROM sales_silver.products p
LEFT JOIN sales_silver.ordered_items oi ON p.product_id = oi.product_id
WHERE oi.product_id IS NULL
GROUP BY p.product_id
;
SELECT 
    *
    -- COUNT(*) AS products_with_no_lines
FROM sales_silver.products p
LEFT JOIN sales_silver.ordered_items oi ON p.product_id = oi.product_id
-- WHERE oi.product_id IS NULL;
WHERE oi.product_id IS NOT NULL
;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


---- Regions table

/*
Notes things to check with business:
- regions.state is the reference list of valid states. Confirm whether all customers and reps must map to a region.
PK-FK: regions --> customers 1 to 0 or many relationships
PK-FK: regions --> sales_reps 1 to 0 or many relationships
*/

SELECT 
    *
FROM sales_silver.regions rg
LEFT JOIN sales_silver.customers c ON rg.state = c.state
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_customers
FROM sales_silver.customers c
LEFT JOIN sales_silver.regions rg ON c.state = rg.state
WHERE c.state IS NOT NULL AND rg.state IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_customers
FROM sales_silver.customers c
LEFT JOIN sales_silver.regions rg ON c.state = rg.state
WHERE c.state IS NOT NULL AND rg.state IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    COUNT(*) AS regions_with_no_customers,
    rg.state
FROM sales_silver.regions rg
LEFT JOIN sales_silver.customers c ON rg.state = c.state
WHERE c.customer_id IS NULL
GROUP BY rg.state
;
SELECT 
    *
    -- COUNT(*) AS regions_with_no_customers
FROM sales_silver.regions rg
LEFT JOIN sales_silver.customers c ON rg.state = c.state
-- WHERE c.customer_id IS NULL;
WHERE c.customer_id IS NOT NULL
;

SELECT 
    *
FROM sales_silver.regions rg
LEFT JOIN sales_silver.sales_reps r ON rg.state = r.region
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_reps
FROM sales_silver.sales_reps r
LEFT JOIN sales_silver.regions rg ON r.region = rg.state
WHERE r.region IS NOT NULL AND rg.state IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_reps
FROM sales_silver.sales_reps r
LEFT JOIN sales_silver.regions rg ON r.region = rg.state
WHERE r.region IS NOT NULL AND rg.state IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    COUNT(*) AS regions_with_no_reps,
    rg.state
FROM sales_silver.regions rg
LEFT JOIN sales_silver.sales_reps r ON rg.state = r.region
WHERE r.rep_id IS NULL
GROUP BY rg.state
;
SELECT 
    *
    -- COUNT(*) AS regions_with_no_reps
FROM sales_silver.regions rg
LEFT JOIN sales_silver.sales_reps r ON rg.state = r.region
-- WHERE r.rep_id IS NULL;
WHERE r.rep_id IS NOT NULL
;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

---- Sales_reps table

/*
Notes things to check with business:
- sales_reps.region should map to regions.state for consistency.
PK-FK: sales_reps --> order_status 1 to 0 or many relationships
PK-FK: sales_reps --> regions (many reps to 1 region)
*/

SELECT 
    *
FROM sales_silver.sales_reps r
LEFT JOIN sales_silver.order_status os ON r.rep_id = os.sales_rep_id
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_orders
FROM sales_silver.order_status os
LEFT JOIN sales_silver.sales_reps r ON os.sales_rep_id = r.rep_id
WHERE os.sales_rep_id IS NOT NULL AND r.rep_id IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_orders
FROM sales_silver.order_status os
LEFT JOIN sales_silver.sales_reps r ON os.sales_rep_id = r.rep_id
WHERE os.sales_rep_id IS NOT NULL AND r.rep_id IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    COUNT(*) AS reps_with_no_orders,
    r.rep_id
FROM sales_silver.sales_reps r
LEFT JOIN sales_silver.order_status os ON r.rep_id = os.sales_rep_id
WHERE os.order_id IS NULL
GROUP BY r.rep_id
;
SELECT 
    *
    -- COUNT(*) AS reps_with_no_orders
FROM sales_silver.sales_reps r
LEFT JOIN sales_silver.order_status os ON r.rep_id = os.sales_rep_id
-- WHERE os.order_id IS NULL;
WHERE os.order_id IS NOT NULL
;

SELECT 
    *
FROM sales_silver.sales_reps r
LEFT JOIN sales_silver.regions rg ON r.region = rg.state
LIMIT 100
;
-- pk checks
SELECT 
    COUNT(*) AS orphan_reps
FROM sales_silver.sales_reps r
LEFT JOIN sales_silver.regions rg ON r.region = rg.state
WHERE r.region IS NOT NULL AND rg.state IS NULL
;
SELECT 
    *
    -- COUNT(*) AS orphan_reps
FROM sales_silver.sales_reps r
LEFT JOIN sales_silver.regions rg ON r.region = rg.state
WHERE r.region IS NOT NULL AND rg.state IS NULL
;
-- fk check for parent records with 0 children
SELECT 
    COUNT(*) AS regions_with_no_reps,
    rg.state
FROM sales_silver.regions rg
LEFT JOIN sales_silver.sales_reps r ON rg.state = r.region
WHERE r.rep_id IS NULL
GROUP BY rg.state
;
SELECT 
    *
    -- COUNT(*) AS regions_with_no_reps
FROM sales_silver.regions rg
LEFT JOIN sales_silver.sales_reps r ON rg.state = r.region
-- WHERE r.rep_id IS NULL;
WHERE r.rep_id IS NOT NULL
;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
