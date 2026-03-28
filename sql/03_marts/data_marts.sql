CREATE MATERIALIZED VIEW marts.mart_delivery_performance AS
SELECT
    g.market,
    g.order_region,
    s.shipping_mode,
    COUNT(*)                                            AS total_orders,
    SUM(f.late_delivery_risk)                           AS late_orders,
    ROUND(AVG(f.late_delivery_risk::numeric) * 100, 1)  AS late_rate_pct,
    ROUND(AVG(s.delay_days), 2)                         AS avg_delay_days
FROM warehouse.fact_orders f
JOIN warehouse.dim_geography g ON f.geography_id = g.geography_id
JOIN warehouse.dim_shipping  s ON f.shipping_id  = s.shipping_id
GROUP BY g.market, g.order_region, s.shipping_mode;

CREATE MATERIALIZED VIEW marts.mart_product_profitability AS
SELECT
    p.category_name,
    p.department_name,
    COUNT(*)                            AS total_orders,
    ROUND(SUM(f.profit), 2)             AS total_profit,
    ROUND(AVG(f.profit), 2)             AS avg_profit_per_order,
    ROUND(SUM(f.sales), 2)              AS total_revenue,
    ROUND(AVG(f.profit_ratio) * 100, 1) AS avg_margin_pct
FROM warehouse.fact_orders f
JOIN warehouse.dim_product p
    ON f.product_sk = p.product_sk
   AND p.is_current = TRUE
GROUP BY p.category_name, p.department_name
ORDER BY total_profit DESC;

CREATE MATERIALIZED VIEW marts.mart_monthly_revenue AS
SELECT
    d.year,
    d.month,
    d.month_name,
    g.market,
    COUNT(DISTINCT f.order_id)            AS total_orders,
    ROUND(SUM(f.sales), 2)                AS total_revenue,
    ROUND(SUM(f.profit), 2)               AS total_profit,
    ROUND(AVG(f.profit_ratio) * 100, 1)   AS avg_margin_pct
FROM warehouse.fact_orders f
JOIN warehouse.dim_date      d ON f.date_id      = d.date_id
JOIN warehouse.dim_geography g ON f.geography_id = g.geography_id
GROUP BY d.year, d.month, d.month_name, g.market
ORDER BY d.year, d.month;