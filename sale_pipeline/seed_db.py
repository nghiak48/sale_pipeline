from sqlalchemy import create_engine, text
import random

DB_URL = "postgresql+psycopg2://postgres:admin@postgres:5432/retail_db"

def seed_products_and_inventory(conn):
    categories = ['Electronics', 'Clothing', 'Food', 'Beauty', 'Sports']

    for pid in range(1, 101):
        name = f"Product_{pid:03d}"
        category = categories[pid % len(categories)]
        price = round(random.uniform(50, 500), 2)

        conn.execute(text("""
            INSERT INTO products (product_id, product_name, category, price)
            VALUES (:pid, :name, :cat, :price)
            ON CONFLICT (product_id) DO NOTHING
        """), {"pid": pid, "name": name, "cat": category, "price": price})

        conn.execute(text("""
            INSERT INTO inventory (product_id, stock_qty, restock_threshold)
            VALUES (:pid, :qty, :threshold)
            ON CONFLICT (product_id) DO NOTHING
        """), {"pid": pid, "qty": random.randint(20, 200), "threshold": 50})

    print("[Seed] Products and inventory seeded!")


def run_seed():
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        #init_schemas(conn)
        seed_products_and_inventory(conn)


if __name__ == "__main__":
    run_seed()