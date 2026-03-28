CREATE INDEX idx_stg_customer ON staging.stg_orders(customer_id);
CREATE INDEX idx_stg_product  ON staging.stg_orders(product_card_id);
CREATE INDEX idx_stg_city     ON staging.stg_orders(order_city, order_country, order_region);
CREATE INDEX idx_stg_shipping ON staging.stg_orders(shipping_mode, delivery_status, days_for_shipment_scheduled, days_for_shipping_real);

CREATE INDEX idx_cust_id_curr ON warehouse.dim_customer(customer_id, is_current, effective_from, effective_to);
CREATE INDEX idx_prod_id_curr ON warehouse.dim_product(product_id, is_current, effective_from, effective_to);

DROP TABLE IF EXISTS warehouse.fact_orders;

CREATE TABLE warehouse.fact_orders (
    order_item_id       INT ,
    order_id            INT,
    customer_sk         INT REFERENCES warehouse.dim_customer(customer_sk),
    product_sk          INT REFERENCES warehouse.dim_product(product_sk),
    date_id             INT REFERENCES warehouse.dim_date(date_id),
    geography_id        INT REFERENCES warehouse.dim_geography(geography_id),
    shipping_id         INT REFERENCES warehouse.dim_shipping(shipping_id),
    quantity            INT,
    unit_price          NUMERIC(10,2),
    discount_amount     NUMERIC(10,2),
    discount_rate       NUMERIC(6,4),
    line_total          NUMERIC(12,2),
    sales               NUMERIC(12,2),
    benefit             NUMERIC(12,2),
    profit              NUMERIC(12,2),
    profit_ratio        NUMERIC(6,4),
    late_delivery_risk  INT,
    order_status        VARCHAR(50),
    payment_type        VARCHAR(50)
);

INSERT INTO warehouse.fact_orders
SELECT
    o.order_item_id,
    o.order_id,
    c.customer_sk,
    p.product_sk,
    TO_CHAR(o.order_date_dateorders::date, 'YYYYMMDD')::int,
    g.geography_id,
    s.shipping_id,
    o.order_item_quantity,
    o.order_item_product_price,
    o.order_item_discount,
    o.order_item_discount_rate,
    o.order_item_total,
    o.sales,
    o.benefit_per_order,
    o.order_profit_per_order,
    o.order_item_profit_ratio,
    o.late_delivery_risk,
    o.order_status,
    o.type
FROM staging.stg_orders o

JOIN warehouse.dim_customer c
    ON  c.customer_id  = o.customer_id
    AND o.order_date_dateorders::date >= c.effective_from
    AND (c.effective_to IS NULL OR o.order_date_dateorders::date <= c.effective_to)

JOIN warehouse.dim_product p
    ON  p.product_id   = o.product_card_id
    AND o.order_date_dateorders::date >= p.effective_from
    AND (p.effective_to IS NULL OR o.order_date_dateorders::date <= p.effective_to)

LEFT JOIN warehouse.dim_geography g
    ON  g.order_city    = o.order_city
    AND g.order_country = o.order_country
    AND g.order_region  = o.order_region

LEFT JOIN warehouse.dim_shipping s
    ON  s.shipping_mode               = o.shipping_mode
    AND s.delivery_status             = o.delivery_status
    AND s.days_for_shipment_scheduled = o.days_for_shipment_scheduled
    AND s.days_for_shipping_real      = o.days_for_shipping_real;

CREATE INDEX idx_fact_customer  ON warehouse.fact_orders(customer_sk);
CREATE INDEX idx_fact_product   ON warehouse.fact_orders(product_sk);
CREATE INDEX idx_fact_date      ON warehouse.fact_orders(date_id);
CREATE INDEX idx_fact_geography ON warehouse.fact_orders(geography_id);
CREATE INDEX idx_fact_shipping  ON warehouse.fact_orders(shipping_id);