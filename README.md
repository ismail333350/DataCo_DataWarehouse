# DataCo Supply Chain — Data Warehouse Project

> A full end-to-end Data Warehouse built on PostgreSQL using the DataCo Supply Chain dataset (186,000 rows), implementing dimensional modeling, SCD Type 2, and analytical data marts.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Dataset](#dataset)
- [Architecture](#architecture)
- [Star Schema Design](#star-schema-design)
- [SCD Type 2 Implementation](#scd-type-2-implementation)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Data Marts & Business Questions](#data-marts--business-questions)
- [Design Decisions](#design-decisions)
- [Key Findings](#key-findings)

---

## Project Overview

This project demonstrates a production-pattern Data Warehouse built from scratch using PostgreSQL. It covers:

- Raw data ingestion into a **staging layer**
- Dimensional modeling with a **star schema**
- **SCD Type 2** for slowly changing dimensions (customer, product)
- **SCD Type 1** for stable reference dimensions (geography, shipping, date)
- Aggregated **data marts** for business analytics
- Performance **indexes** on all join columns

---

## Dataset

| Property | Value |
|---|---|
| Source | DataCo Supply Chain Dataset (Kaggle) |
| Rows | 186,000 order line items |
| Columns | 53 |
| Domain | Supply chain — orders, customers, products, shipping |
| Date Range | 2015 – 2018 |
| Markets | LATAM, Europe, Pacific Asia, USCA, Africa |

**Known data quality issues handled:**
- `Product Description` — 100% empty, dropped at ingestion
- `Product Image` — URL column, dropped at ingestion
- `Order Zipcode` — 86% missing, retained as nullable

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Source Layer                      │
│         DataCoSupplyChainDataset.csv (96 MB)        │
└────────────────────┬────────────────────────────────┘
                     │  Python (pandas + sqlalchemy)
                     ▼
┌─────────────────────────────────────────────────────┐
│              Staging Schema                         │
│              staging.stg_orders                     │
│         (raw data, no transformations)              │
└────────────────────┬────────────────────────────────┘
                     │  SQL transformations
                     ▼
┌─────────────────────────────────────────────────────┐
│             Warehouse Schema                        │
│   dim_customer (SCD2)   dim_product (SCD2)          │
│   dim_date (static)     dim_geography (SCD1)        │
│   dim_shipping (SCD1)   fact_orders (central fact)  │
└────────────────────┬────────────────────────────────┘
                     │  Materialized views
                     ▼
┌─────────────────────────────────────────────────────┐
│               Marts Schema                          │
│   mart_delivery_performance                         │
│   mart_product_profitability                        │
│   mart_monthly_revenue                              │
└─────────────────────────────────────────────────────┘
```

---

## Star Schema Design

### Fact Table — `warehouse.fact_orders`

| Column | Type | Description |
|---|---|---|
| order_item_id | INT (PK) | Grain — one row per order line item |
| order_id | INT | Parent order |
| customer_sk | INT (FK) | Surrogate key → dim_customer |
| product_sk | INT (FK) | Surrogate key → dim_product |
| date_id | INT (FK) | → dim_date |
| geography_id | INT (FK) | → dim_geography |
| shipping_id | INT (FK) | → dim_shipping |
| quantity | INT | Items ordered |
| unit_price | NUMERIC | Price per item |
| discount_amount | NUMERIC | Discount in $ |
| discount_rate | NUMERIC | Discount as ratio |
| line_total | NUMERIC | quantity × unit_price |
| sales | NUMERIC | Actual revenue |
| profit | NUMERIC | Profit per order |
| profit_ratio | NUMERIC | Profit / sales |
| late_delivery_risk | INT | 1 = at risk, 0 = on time |
| order_status | VARCHAR | COMPLETE, CANCELED, etc. |
| payment_type | VARCHAR | DEBIT, TRANSFER, etc. |

**Grain:** One row per order line item (order_item_id).

### Dimension Tables

| Dimension | SCD Type | Natural Key | Tracked Attributes |
|---|---|---|---|
| dim_customer | SCD2 | customer_id | segment, city, state, country, zipcode |
| dim_product | SCD2 | product_card_id | product_name, category, department, price |
| dim_date | Static | date_id (YYYYMMDD) | year, month, quarter, day_of_week |
| dim_geography | SCD1 | geography_id | city, state, country, region, market |
| dim_shipping | SCD1 | shipping_id | shipping_mode, delivery_status, delay_days |

---

## SCD Type 2 Implementation

SCD2 is applied to `dim_customer` and `dim_product` because customer segments and product prices can change over time and we want to preserve history.

### Extra columns added to SCD2 dimensions

```sql
customer_sk     SERIAL PRIMARY KEY   -- surrogate key (used as FK in fact table)
effective_from  DATE NOT NULL        -- when this version became active
effective_to    DATE                 -- NULL means currently active record
is_current      BOOLEAN              -- TRUE = current version
```

### How a change is tracked

When a customer moves from segment `Consumer` to `Corporate`:

| customer_sk | customer_id | segment | effective_from | effective_to | is_current |
|---|---|---|---|---|---|
| 1 | 1001 | Consumer | 2015-01-01 | 2024-03-27 | FALSE |
| 2 | 1001 | Corporate | 2024-03-28 | NULL | TRUE |

### Fact table uses surrogate keys

The fact table stores `customer_sk` (not `customer_id`), so each historical order automatically points to the correct version of the customer that was active at order time.

```sql
-- SCD2 join in fact table population
JOIN warehouse.dim_customer c
    ON  c.customer_id  = o.customer_id
    AND o.order_date::date >= c.effective_from
    AND (c.effective_to IS NULL OR o.order_date::date <= c.effective_to)
```

### Querying SCD2 correctly

```sql
-- Current state only
SELECT * FROM warehouse.dim_customer WHERE is_current = TRUE;

-- Historical state at order time (surrogate key already resolves this)
SELECT f.order_id, c.segment AS segment_at_order_time
FROM warehouse.fact_orders f
JOIN warehouse.dim_customer c ON f.customer_sk = c.customer_sk;
```

---

## Project Structure

```
dataco-dwh/
├── data/
│   └── DataCoSupplyChainDataset.csv          # raw source file (not committed)
├── python/
│   └── load_staging.py                       # ingestion script
├── sql/
│   ├── 01_staging/                           # (handled by Python)
│   ├── 02_dimensions/
│   │   ├── dim_date.sql
│   │   ├── dim_geography.sql
│   │   ├── dim_shipping.sql
│   │   ├── dim_customer.sql
│   │   ├── dim_product.sql
│   │   └── scd2_procedures.sql
│   ├── 03_facts/
│   │   └── fact_orders.sql
│   └── 04_marts/
│       └── marts.sql
└── README.md
```

---

## How to Run

### Prerequisites

- Python 3.x with virtual environment
- PostgreSQL installed and running
- Dataset CSV downloaded from Kaggle

### Step 1 — Set up environment

```bash
cd C:\Users\EGY10\dataco-dwh
python -m venv venv
venv\Scripts\activate
pip install pandas sqlalchemy psycopg2-binary
```

### Step 2 — Create the database

```sql
CREATE DATABASE dataco_dwh;
```

### Step 3 — Load staging

```bash
python python\load_staging.py
```

Expected output:
```
Reading CSV...
  Loaded 186,000 rows, 53 columns
Cleaning column names...
Parsing dates...
Dropping empty columns...
Creating staging schema if not exists...
Loading 186,000 rows into staging.stg_orders...
  Rows in staging.stg_orders: 186,000
```

### Step 4 — Run SQL scripts in order

Open pgAdmin, connect to `dataco_dwh`, and run each script in this order:

```
1. sql/02_dimensions/dim_date.sql
2. sql/02_dimensions/dim_geography.sql
3. sql/02_dimensions/dim_shipping.sql
4. sql/02_dimensions/dim_customer.sql
5. sql/02_dimensions/dim_product.sql
6. sql/02_dimensions/scd2_procedures.sql
   → then run: CALL warehouse.upsert_dim_customer(CURRENT_DATE);
   → then run: CALL warehouse.upsert_dim_product(CURRENT_DATE);
7. sql/03_facts/fact_orders.sql
8. sql/04_marts/marts.sql
```

### Step 5 — Sanity check

```sql
SELECT 'staging.stg_orders'      AS table_name, COUNT(*) AS rows FROM staging.stg_orders
UNION ALL
SELECT 'warehouse.dim_customer',  COUNT(*) FROM warehouse.dim_customer
UNION ALL
SELECT 'warehouse.dim_product',   COUNT(*) FROM warehouse.dim_product
UNION ALL
SELECT 'warehouse.dim_date',      COUNT(*) FROM warehouse.dim_date
UNION ALL
SELECT 'warehouse.dim_geography', COUNT(*) FROM warehouse.dim_geography
UNION ALL
SELECT 'warehouse.dim_shipping',  COUNT(*) FROM warehouse.dim_shipping
UNION ALL
SELECT 'warehouse.fact_orders',   COUNT(*) FROM warehouse.fact_orders
ORDER BY table_name;

-- Should return 0
SELECT COUNT(*) AS orphaned_rows
FROM warehouse.fact_orders
WHERE customer_sk IS NULL OR product_sk IS NULL;
```

---

## Data Marts & Business Questions

### mart_delivery_performance

**Business question:** Which markets and shipping modes have the highest late delivery rates?

```sql
SELECT market, shipping_mode, late_rate_pct, avg_delay_days
FROM marts.mart_delivery_performance
ORDER BY late_rate_pct DESC;
```

### mart_product_profitability

**Business question:** Which product categories generate the most profit, and what is the average margin?

```sql
SELECT category_name, total_profit, avg_profit_per_order, avg_margin_pct
FROM marts.mart_product_profitability
ORDER BY total_profit DESC;
```

### mart_monthly_revenue

**Business question:** What is the month-over-month revenue and profit trend per market?

```sql
SELECT year, month, market, total_revenue, total_profit, avg_margin_pct
FROM marts.mart_monthly_revenue
ORDER BY year, month, total_revenue DESC;
```

---

## Design Decisions

**Why SCD2 for customer and product?**
Customer segments and product prices are business-critical attributes that change over time. Using SCD2 ensures that historical orders are always analyzed against the segment/price that was active at the time of the order — not the current value. This prevents incorrect profitability and segmentation analysis.

**Why SCD1 for geography and shipping?**
Geographic reference data and shipping lookup values are stable reference data. If a region name changes, we want all records (past and present) to reflect the corrected value. SCD1 (overwrite) is the correct pattern here.

**Why surrogate keys in the fact table?**
The fact table joins to dimension surrogate keys, not natural keys. This is the correct pattern for SCD2 — it locks each fact row to the exact version of the dimension that was active at transaction time, without needing date range logic at query time.

**Why materialized views for marts?**
Materialized views are pre-computed and stored, making analytical queries fast without re-running expensive aggregations. Run `REFRESH MATERIALIZED VIEW` when the warehouse is updated.

**Why drop Product Description and Product Image?**
`Product Description` was 100% null across all 186,000 rows — zero analytical value. `Product Image` contained URLs with no relevance to supply chain analytics. Dropping both reduced noise and improved load performance.

---

## Key Findings

- **54.9%** of orders carry a late delivery risk — a major operational concern
- **Second Class** shipping has the worst average delay at +2.07 days despite being a paid upgrade
- **Standard Class** is the most reliable, averaging nearly on-time delivery (-0.02 days)
- **Fishing** is the most profitable category by total profit; **Cardio Equipment** leads in average profit per order
- **LATAM and Europe** are the top two markets, virtually tied in revenue at ~$575K
- **2.1%** of orders are flagged as suspected fraud, with above-average profit per order — worth investigating

---

*Built with PostgreSQL · Python · pandas · sqlalchemy*
