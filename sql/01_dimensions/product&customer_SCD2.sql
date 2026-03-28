DROP PROCEDURE IF EXISTS warehouse.upsert_dim_customer(date);
DROP PROCEDURE IF EXISTS warehouse.upsert_dim_product(date);

CREATE OR REPLACE PROCEDURE warehouse.upsert_dim_customer(load_date DATE)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE warehouse.dim_customer AS dim
    SET
        effective_to = load_date - INTERVAL '1 day',
        is_current   = FALSE
    FROM (
        SELECT DISTINCT
            customer_id,
            customer_segment       AS segment,
            customer_city          AS city,
            customer_state         AS state,
            customer_country       AS country,
            customer_zipcode::text AS zipcode
        FROM staging.stg_orders
    ) AS src
    WHERE dim.customer_id = src.customer_id
      AND dim.is_current  = TRUE
      AND (
            dim.segment  IS DISTINCT FROM src.segment  OR
            dim.city     IS DISTINCT FROM src.city     OR
            dim.state    IS DISTINCT FROM src.state    OR
            dim.country  IS DISTINCT FROM src.country  OR
            dim.zipcode  IS DISTINCT FROM src.zipcode
          );

    INSERT INTO warehouse.dim_customer (
        customer_id, customer_name, segment,
        city, state, country, zipcode,
        effective_from, effective_to, is_current
    )
    SELECT DISTINCT
        src.customer_id,
        src.customer_fname || ' ' || src.customer_lname,
        src.customer_segment,
        src.customer_city,
        src.customer_state,
        src.customer_country,
        src.customer_zipcode::text,
        load_date,
        NULL::date,
        TRUE
    FROM staging.stg_orders src
    WHERE NOT EXISTS (
        SELECT 1 FROM warehouse.dim_customer dim
        WHERE dim.customer_id = src.customer_id
          AND dim.is_current  = TRUE
          AND dim.segment     = src.customer_segment
          AND dim.city        = src.customer_city
          AND dim.state       = src.customer_state
          AND dim.country     = src.customer_country
          AND COALESCE(dim.zipcode,'') = COALESCE(src.customer_zipcode::text,'')
    );
END;
$$;

CREATE OR REPLACE PROCEDURE warehouse.upsert_dim_product(load_date DATE)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE warehouse.dim_product AS dim
    SET
        effective_to = load_date - INTERVAL '1 day',
        is_current   = FALSE
    FROM (
        SELECT DISTINCT
            product_card_id   AS product_id,
            product_name,
            category_name,
            department_name,
            product_price
        FROM staging.stg_orders
    ) AS src
    WHERE dim.product_id    = src.product_id
      AND dim.is_current    = TRUE
      AND (
            dim.product_name    IS DISTINCT FROM src.product_name    OR
            dim.category_name   IS DISTINCT FROM src.category_name   OR
            dim.department_name IS DISTINCT FROM src.department_name OR
            dim.product_price   IS DISTINCT FROM src.product_price
          );

    INSERT INTO warehouse.dim_product (
        product_id, product_name, category_name,
        department_name, product_price,
        effective_from, effective_to, is_current
    )
    SELECT DISTINCT
        src.product_card_id,
        src.product_name,
        src.category_name,
        src.department_name,
        src.product_price,
        load_date,
        NULL::date,
        TRUE
    FROM staging.stg_orders src
    WHERE NOT EXISTS (
        SELECT 1 FROM warehouse.dim_product dim
        WHERE dim.product_id      = src.product_card_id
          AND dim.is_current      = TRUE
          AND dim.product_name    = src.product_name
          AND dim.category_name   = src.category_name
          AND dim.department_name = src.department_name
          AND dim.product_price   = src.product_price
    );
END;
$$;

-- Now run them
CALL warehouse.upsert_dim_customer(CURRENT_DATE);
CALL warehouse.upsert_dim_product(CURRENT_DATE);