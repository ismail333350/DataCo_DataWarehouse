DROP TABLE IF EXISTS warehouse.dim_customer;

CREATE TABLE warehouse.dim_customer (
    customer_sk     SERIAL PRIMARY KEY,
    customer_id     INT NOT NULL,
    customer_name   VARCHAR(200),
    segment         VARCHAR(100),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    zipcode         VARCHAR(20),
    effective_from  DATE    NOT NULL,
    effective_to    DATE,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_customer_nk      ON warehouse.dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON warehouse.dim_customer(customer_id, is_current);

INSERT INTO warehouse.dim_customer (
    customer_id, customer_name, segment,
    city, state, country, zipcode,
    effective_from, effective_to, is_current
)
SELECT
    customer_id,
    customer_name,
    segment,
    city,
    state,
    country,
    zipcode,
    '2015-01-01'::date,
    NULL::date,
    TRUE
FROM (
    SELECT
        customer_id,
        customer_fname || ' ' || customer_lname   AS customer_name,
        customer_segment                           AS segment,
        customer_city                              AS city,
        customer_state                             AS state,
        customer_country                           AS country,
        customer_zipcode::text                     AS zipcode,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date_dateorders DESC
        ) AS rn
    FROM staging.stg_orders
    WHERE customer_id IS NOT NULL
) ranked
WHERE rn = 1;
