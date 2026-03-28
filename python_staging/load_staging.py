import pandas as pd
from sqlalchemy import create_engine, text

# ── Change these to match your PostgreSQL credentials ──
DB_USER     = "postgres"
DB_PASSWORD = "23654835i"
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "dataco_dwh"
CSV_PATH    = r"C:\Users\EGY10\dataco-dwh\data\DataCoSupplyChainDataset.csv"
# ───────────────────────────────────────────────────────

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── Step 1: Read CSV ──
print("Reading CSV...")
df = pd.read_csv(CSV_PATH, encoding='latin-1')
print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")

# ── Step 2: Clean column names ──
print("Cleaning column names...")
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(' ', '_', regex=False)
    .str.replace(r'[()\/]', '', regex=True)
)

# ── Step 3: Parse date columns ──
print("Parsing dates...")
df['order_date_dateorders'] = pd.to_datetime(
    df['order_date_dateorders'], errors='coerce'
)
df['shipping_date_dateorders'] = pd.to_datetime(
    df['shipping_date_dateorders'], errors='coerce'
)

# ── Step 4: Drop useless columns ──
print("Dropping empty columns...")
cols_to_drop = ['product_description', 'product_image']
cols_to_drop = [c for c in cols_to_drop if c in df.columns]
df.drop(columns=cols_to_drop, inplace=True)

# ── Step 5: Create schema if not exists ──
print("Creating staging schema if not exists...")
with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse;"))
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS marts;"))
    conn.commit()

# ── Step 6: Load into staging ──
print(f"Loading {len(df):,} rows into staging.stg_orders...")
print("  This may take 1-2 minutes...")

df.to_sql(
    name      = 'stg_orders',
    con       = engine,
    schema    = 'staging',
    if_exists = 'replace',
    index     = False,
    chunksize = 5000,
    method    = 'multi'
)

# ── Step 7: Verify ──
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM staging.stg_orders"))
    count = result.scalar()

print(f"")
print(f"  Staging load complete!")
print(f"  Rows in staging.stg_orders: {count:,}")
print(f"")
print(f"Next step: run your SQL dimension scripts in pgAdmin.")
