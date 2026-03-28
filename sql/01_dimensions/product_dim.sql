DROP TABLE IF EXISTS warehouse.dim_product;

CREATE TABLE warehouse.dim_product (
    product_sk      SERIAL PRIMARY KEY,
    product_id      INT NOT NULL,
    product_name    VARCHAR(200),
    category_name   VARCHAR(100),
    department_name VARCHAR(100),
    product_price   NUMERIC(10,2),
    effective_from  DATE    NOT NULL,
    effective_to    DATE,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_product_nk      ON warehouse.dim_product(product_id);
CREATE INDEX idx_dim_product_current ON warehouse.dim_product(product_id, is_current);

INSERT INTO warehouse.dim_product (
    product_id, product_name, category_name,
    department_name, product_price,
    effective_from, effective_to, is_current
)
SELECT DISTINCT
    product_card_id,
    product_name,
    category_name,
    department_name,
    product_price,
    '2015-01-01'::Date,
    NULL::Date,
    TRUE
FROM staging.stg_orders
WHERE product_card_id IS NOT NULL;