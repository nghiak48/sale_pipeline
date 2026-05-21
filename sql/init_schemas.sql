-- Xóa sạch và tạo lại
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- Giữ các bảng operational (không thuộc medallion)
CREATE TABLE IF NOT EXISTS products (
    product_id      INT PRIMARY KEY,
    product_name    VARCHAR(100),
    category        VARCHAR(50),
    price           NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS inventory (
    product_id          INT PRIMARY KEY REFERENCES products(product_id),
    stock_qty           INT NOT NULL DEFAULT 0,
    restock_threshold   INT NOT NULL DEFAULT 50,
    last_updated        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_alerts (
    alert_id    SERIAL PRIMARY KEY,
    product_id  INT REFERENCES products(product_id),
    stock_qty   INT,
    threshold   INT,
    alert_time  TIMESTAMP DEFAULT NOW(),
    resolved    BOOLEAN DEFAULT FALSE
);

-- ===== SILVER =====
CREATE TABLE silver.sales_enriched (
    sale_id         VARCHAR(50),
    customer_id     INT,
    store_id        INT,
    product_id      INT,
    product_name    VARCHAR(100),
    category        VARCHAR(50),
    amount          NUMERIC(10,2),
    sale_date       TIMESTAMP,
    sale_hour       INT,
    sale_date_only  DATE,
    processed_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE silver.rfm_snapshot (
    customer_id     INT,
    last_purchase   TIMESTAMP,
    frequency       INT,
    monetary        NUMERIC(12,2),
    recency_days    INT,
    segment         VARCHAR(50),
    snapshot_date   DATE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ===== GOLD =====
CREATE TABLE gold.mart_sales (
    store_id        INT,
    category        VARCHAR(50),
    sale_hour       TIMESTAMP,
    total_revenue   NUMERIC(12,2),
    order_count     INT,
    avg_order_value NUMERIC(10,2),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE gold.mart_account (
    customer_id     INT,
    segment         VARCHAR(50),
    frequency       INT,
    monetary        NUMERIC(12,2),
    recency_days    INT,
    snapshot_date   DATE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE gold.mart_inventory (
    product_id      INT,
    product_name    VARCHAR(100),
    category        VARCHAR(50),
    stock_qty       INT,
    restock_threshold INT,
    status          VARCHAR(20),
    alert_count     INT DEFAULT 0,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- mart_sales: unique theo store + category + giờ
ALTER TABLE gold.mart_sales
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
CREATE UNIQUE INDEX IF NOT EXISTS uix_mart_sales_hour
    ON gold.mart_sales(store_id, category, sale_hour);

-- mart_account: unique theo customer + ngày snapshot
CREATE UNIQUE INDEX IF NOT EXISTS uix_mart_account_snapshot
    ON gold.mart_account(customer_id, snapshot_date);

-- mart_inventory: unique theo product
CREATE UNIQUE INDEX IF NOT EXISTS uix_mart_inventory_product
    ON gold.mart_inventory(product_id);