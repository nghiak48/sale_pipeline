-- Thêm vào init_schemas.sql hoặc chạy thủ công

-- FP-Growth: sản phẩm hay mua cùng nhau
CREATE TABLE IF NOT EXISTS gold.mart_product_affinity (
    antecedent      VARCHAR(100),   -- sản phẩm A
    consequent      VARCHAR(100),   -- thường mua kèm sản phẩm B
    support         NUMERIC(8,4),   -- % customers mua cả A và B
    confidence      NUMERIC(8,4),   -- P(B|A): nếu mua A thì bao nhiêu % mua B
    lift            NUMERIC(8,4),   -- lift > 1 = có tương quan thật sự
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- CF: gợi ý sản phẩm cho từng khách
CREATE TABLE IF NOT EXISTS gold.mart_cf_recommendations (
    customer_id     INT,
    product_id      INT,
    product_name    VARCHAR(100),
    score           NUMERIC(10,4),  -- predicted spending score
    rank            INT,            -- 1 = gợi ý số 1
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Index để query nhanh
CREATE INDEX IF NOT EXISTS idx_affinity_antecedent
    ON gold.mart_product_affinity(antecedent);

CREATE INDEX IF NOT EXISTS idx_cf_customer
    ON gold.mart_cf_recommendations(customer_id);

CREATE INDEX IF NOT EXISTS idx_cf_rank
    ON gold.mart_cf_recommendations(customer_id, rank);
