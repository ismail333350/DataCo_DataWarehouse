DROP TABLE IF EXISTS warehouse.dim_shipping;

CREATE TABLE warehouse.dim_shipping AS
SELECT
    ROW_NUMBER() OVER ()    AS shipping_id,
    shipping_mode,
    delivery_status,
    days_for_shipment_scheduled,
    days_for_shipping_real,
    (days_for_shipping_real - days_for_shipment_scheduled) AS delay_days
FROM (
    SELECT DISTINCT
        shipping_mode,
        delivery_status,
        days_for_shipment_scheduled,
        days_for_shipping_real
    FROM staging.stg_orders
) sub;

ALTER TABLE warehouse.dim_shipping ADD PRIMARY KEY (shipping_id);