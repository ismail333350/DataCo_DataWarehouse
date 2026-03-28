SELECT 'staging.stg_orders'       AS table_name, COUNT(*) AS rows FROM staging.stg_orders
UNION ALL
SELECT 'warehouse.dim_customer',   COUNT(*) FROM warehouse.dim_customer
UNION ALL
SELECT 'warehouse.dim_product',    COUNT(*) FROM warehouse.dim_product
UNION ALL
SELECT 'warehouse.dim_date',       COUNT(*) FROM warehouse.dim_date
UNION ALL
SELECT 'warehouse.dim_geography',  COUNT(*) FROM warehouse.dim_geography
UNION ALL
SELECT 'warehouse.dim_shipping',   COUNT(*) FROM warehouse.dim_shipping
UNION ALL
SELECT 'warehouse.fact_orders',    COUNT(*) FROM warehouse.fact_orders
ORDER BY table_name;