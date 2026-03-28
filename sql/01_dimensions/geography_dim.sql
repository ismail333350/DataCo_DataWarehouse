DROP TABLE IF EXISTS warehouse.dim_geography;

CREATE TABLE warehouse.dim_geography AS
SELECT
    ROW_NUMBER() OVER ()    AS geography_id,
    order_city,
    order_state,
    order_country,
    order_region,
    market,
    latitude,
    longitude
FROM (
    SELECT DISTINCT
        order_city,
        order_state,
        order_country,
        order_region,
        market,
        latitude,
        longitude
    FROM staging.stg_orders
) sub;

ALTER TABLE warehouse.dim_geography ADD PRIMARY KEY (geography_id);
CREATE INDEX idx_geo_lookup ON warehouse.dim_geography(order_city, order_country, order_region);