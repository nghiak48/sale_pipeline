"""
Product Affinity Model
======================
Kết hợp 2 approaches:
  1. FP-Growth   → tìm sản phẩm hay mua cùng nhau (item-item rules)
  2. Collaborative Filtering (SVD) → gợi ý dựa trên khách tương tự

Input:  silver.sales_enriched
Output: gold.mart_product_affinity
        gold.mart_cf_recommendations
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from mlxtend.frequent_patterns import fpgrowth, association_rules
from scipy.sparse import csr_matrix
from scipy.sparse.linalg import svds
import warnings
warnings.filterwarnings("ignore")

DB_URL = "postgresql+psycopg2://postgres:admin@postgres:5432/retail_db"

# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────

def load_sales(engine) -> pd.DataFrame:
    """Load toàn bộ lịch sử mua hàng từ Silver."""
    return pd.read_sql("""
        SELECT customer_id, product_id, product_name, category,
               amount, sale_date
        FROM silver.sales_enriched
        ORDER BY customer_id, sale_date
    """, engine)


def ensure_gold_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.mart_product_affinity (
                antecedent      VARCHAR(100),
                consequent      VARCHAR(100),
                support         NUMERIC(8,4),
                confidence      NUMERIC(8,4),
                lift            NUMERIC(8,4),
                updated_at      TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.mart_cf_recommendations (
                customer_id     INT,
                product_id      INT,
                product_name    VARCHAR(100),
                score           NUMERIC(10,4),
                rank            INT,
                updated_at      TIMESTAMP DEFAULT NOW()
            )
        """))
    print("[ML] Gold tables ensured.")


# ─────────────────────────────────────────────
# APPROACH 1: FP-GROWTH
# ─────────────────────────────────────────────

def build_customer_basket(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group purchases by customer → binary matrix
    Row = customer, Col = product_name, Value = 1/0

    Đây là "basket" theo customer thay vì theo đơn hàng
    vì data chỉ có 1 sản phẩm/đơn.
    """
    basket = (
        df.groupby(["customer_id", "product_name"])["amount"]
        .count()
        .unstack(fill_value=0)
    )
    # Convert sang boolean
    basket = basket.map(lambda x: 1 if x > 0 else 0).astype(bool)
    return basket


def run_fpgrowth(df: pd.DataFrame, min_support=0.05, min_confidence=0.3):
    """
    Chạy FP-Growth trên customer basket.
    Trả về association rules: sản phẩm A → sản phẩm B.
    """
    print("[FP-Growth] Building customer basket...")
    basket = build_customer_basket(df)

    print(f"[FP-Growth] Basket shape: {basket.shape} (customers x products)")
    print(f"[FP-Growth] Running with min_support={min_support}...")

    frequent_items = fpgrowth(
        basket,
        min_support=min_support,
        use_colnames=True,
        max_len=2          # chỉ lấy pair để đơn giản
    )

    if frequent_items.empty:
        print("[FP-Growth] No frequent itemsets found. Try lowering min_support.")
        return pd.DataFrame()

    rules = association_rules(
        frequent_items,
        metric="confidence",
        min_threshold=min_confidence
    )

    # Chỉ lấy rules có 1 antecedent và 1 consequent
    rules = rules[
        rules["antecedents"].apply(len) == 1
    ].copy()

    rules["antecedent"] = rules["antecedents"].apply(lambda x: list(x)[0])
    rules["consequent"] = rules["consequents"].apply(lambda x: list(x)[0])

    result = rules[["antecedent", "consequent", "support", "confidence", "lift"]]
    result = result.sort_values("lift", ascending=False)

    print(f"[FP-Growth] Found {len(result)} rules")
    return result


def save_fpgrowth_results(rules: pd.DataFrame, engine):
    if rules.empty:
        print("[FP-Growth] Nothing to save.")
        return
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE gold.mart_product_affinity"))
    rules.to_sql(
        "mart_product_affinity", engine,
        schema="gold", if_exists="append", index=False
    )
    print(f"[FP-Growth] Saved {len(rules)} rules to gold.mart_product_affinity")


# ─────────────────────────────────────────────
# APPROACH 2: COLLABORATIVE FILTERING (SVD)
# ─────────────────────────────────────────────

def build_user_item_matrix(df: pd.DataFrame):
    """
    Tạo user-item matrix.
    Value = tổng amount (implicit rating dựa trên spending).
    """
    matrix = df.pivot_table(
        index="customer_id",
        columns="product_id",
        values="amount",
        aggfunc="sum",
        fill_value=0
    )
    return matrix


def run_svd(matrix: pd.DataFrame, n_factors=20):
    """
    SVD decomposition để tìm latent factors.
    Reconstruct ma trận → predict score cho các sản phẩm chưa mua.
    """
    print(f"[SVD] Matrix shape: {matrix.shape}")
    print(f"[SVD] Running SVD with {n_factors} factors...")

    sparse_matrix = csr_matrix(matrix.values, dtype=np.float64)

    # Giới hạn n_factors không vượt quá min dimension
    max_factors = min(matrix.shape) - 1
    k = min(n_factors, max_factors)

    U, sigma, Vt = svds(sparse_matrix, k=k)

    # Reconstruct predicted ratings
    sigma_diag = np.diag(sigma)
    predicted = np.dot(np.dot(U, sigma_diag), Vt)

    predicted_df = pd.DataFrame(
        predicted,
        index=matrix.index,
        columns=matrix.columns
    )
    return predicted_df


def get_top_recommendations(
    predicted_df: pd.DataFrame,
    actual_df: pd.DataFrame,
    product_names: dict,
    top_n: int = 5
) -> pd.DataFrame:
    """
    Với mỗi customer, lấy top N sản phẩm:
    - Chưa từng mua
    - Có predicted score cao nhất
    """
    results = []

    # Set sản phẩm đã mua của từng customer
    already_bought = (
        actual_df.groupby("customer_id")["product_id"]
        .apply(set)
        .to_dict()
    )

    for customer_id in predicted_df.index:
        bought = already_bought.get(customer_id, set())
        scores = predicted_df.loc[customer_id]

        # Filter sản phẩm chưa mua
        not_bought = scores[~scores.index.isin(bought)]
        top = not_bought.nlargest(top_n)

        for rank, (product_id, score) in enumerate(top.items(), start=1):
            results.append({
                "customer_id":  customer_id,
                "product_id":   int(product_id),
                "product_name": product_names.get(int(product_id), f"Product_{product_id}"),
                "score":        round(float(score), 4),
                "rank":         rank
            })

    return pd.DataFrame(results)


def save_cf_results(recs: pd.DataFrame, engine):
    if recs.empty:
        print("[SVD] Nothing to save.")
        return
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE gold.mart_cf_recommendations"))
    recs.to_sql(
        "mart_cf_recommendations", engine,
        schema="gold", if_exists="append", index=False
    )
    print(f"[SVD] Saved {len(recs)} recommendations to gold.mart_cf_recommendations")


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def run_product_affinity():
    engine = create_engine(DB_URL)
    ensure_gold_tables(engine)

    print("\n[ML] Loading sales data from silver.sales_enriched...")
    df = load_sales(engine)
    print(f"[ML] Loaded {len(df)} rows, {df['customer_id'].nunique()} customers, "
          f"{df['product_id'].nunique()} products")

    if df.empty:
        print("[ML] No data found. Run ingestion DAGs first.")
        return

    # Product name lookup
    product_names = df.drop_duplicates("product_id").set_index("product_id")["product_name"].to_dict()

    # ── FP-Growth ──────────────────────────────
    print("\n" + "="*50)
    print("STEP 1: FP-Growth Association Rules")
    print("="*50)

    # Tự động điều chỉnh min_support nếu data ít
    n_customers = df["customer_id"].nunique()
    min_support = max(0.01, 10 / n_customers)  # ít nhất 10 customers
    print(f"[FP-Growth] Auto min_support = {min_support:.3f} ({int(min_support*n_customers)} customers)")

    rules = run_fpgrowth(df, min_support=min_support, min_confidence=0.2)
    save_fpgrowth_results(rules, engine)

    if not rules.empty:
        print("\n[FP-Growth] Top 5 rules by lift:")
        print(rules.head(5)[["antecedent", "consequent", "confidence", "lift"]].to_string(index=False))

    # ── Collaborative Filtering ────────────────
    print("\n" + "="*50)
    print("STEP 2: Collaborative Filtering (SVD)")
    print("="*50)

    matrix = build_user_item_matrix(df)
    predicted_df = run_svd(matrix, n_factors=20)
    recs = get_top_recommendations(predicted_df, df, product_names, top_n=5)
    save_cf_results(recs, engine)

    if not recs.empty:
        print("\n[SVD] Sample recommendations for customer_id =", recs["customer_id"].iloc[0])
        sample = recs[recs["customer_id"] == recs["customer_id"].iloc[0]]
        print(sample[["product_name", "score", "rank"]].to_string(index=False))

    print("\n[ML] Product affinity pipeline completed!")


if __name__ == "__main__":
    run_product_affinity()
