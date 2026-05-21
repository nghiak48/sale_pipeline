#!/usr/bin/env python3
"""
Metabase auto-setup script.
Chạy sau khi Metabase khởi động xong (~3-5 phút).

Usage:
    python metabase_setup.py
"""

import requests
import json
import time
import sys

METABASE_URL = "http://localhost:3001"
ADMIN_EMAIL  = "admin@retail.com"
ADMIN_PASS   = "admin1234"

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB   = "retail_db"
PG_USER = "postgres"
PG_PASS = "admin"


def wait_for_metabase(timeout=300):
    print("[Setup] Waiting for Metabase...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print("[Setup] Metabase is ready!")
                return True
        except Exception:
            pass
        time.sleep(5)
    raise TimeoutError("Metabase did not start in time")


def setup_admin():
    """Tạo admin account lần đầu."""
    r = requests.get(f"{METABASE_URL}/api/session/properties")
    setup_token = r.json().get("setup-token")

    if not setup_token:
        print("[Setup] Admin already configured, skipping.")
        return

    payload = {
        "token": setup_token,
        "user": {
            "first_name": "Admin",
            "last_name": "Retail",
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASS,
            "site_name": "Retail Analytics"
        },
        "prefs": {
            "site_name": "Retail Analytics",
            "allow_tracking": False
        }
    }
    r = requests.post(f"{METABASE_URL}/api/setup", json=payload)
    if r.status_code == 200:
        print("[Setup] Admin account created!")
    else:
        print(f"[Setup] Setup failed: {r.text}")


def get_token():
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": ADMIN_EMAIL, "password": ADMIN_PASS}
    )
    return r.json()["id"]


def add_database(token):
    headers = {"X-Metabase-Session": token}

    # Kiểm tra đã có DB chưa
    dbs = requests.get(f"{METABASE_URL}/api/database", headers=headers).json()
    for db in dbs.get("data", []):
        if db["name"] == "Retail DB":
            print(f"[Setup] Database already exists (id={db['id']})")
            return db["id"]

    payload = {
        "name": "Retail DB",
        "engine": "postgres",
        "details": {
            "host": PG_HOST,
            "port": PG_PORT,
            "dbname": PG_DB,
            "user": PG_USER,
            "password": PG_PASS,
            "ssl": False,
            "schema-filters-type": "all"
        },
        "auto_run_queries": True,
        "is_full_sync": True
    }
    r = requests.post(f"{METABASE_URL}/api/database", headers=headers, json=payload)
    db_id = r.json()["id"]
    print(f"[Setup] Database added (id={db_id})")

    # Chờ sync schema
    print("[Setup] Waiting for schema sync...")
    time.sleep(15)
    return db_id


def get_table_id(token, db_id, schema, table_name):
    headers = {"X-Metabase-Session": token}
    r = requests.get(f"{METABASE_URL}/api/database/{db_id}/metadata", headers=headers)
    for schema_data in r.json().get("schemas", {}).values():
        for t in schema_data:
            if t["schema"] == schema and t["name"] == table_name:
                return t["id"]
    # Fallback: list tables
    tables = requests.get(f"{METABASE_URL}/api/table", headers=headers).json()
    for t in tables:
        if t.get("schema") == schema and t["name"] == table_name:
            return t["id"]
    return None


def create_collection(token, name):
    headers = {"X-Metabase-Session": token}
    # Kiểm tra đã có chưa
    cols = requests.get(f"{METABASE_URL}/api/collection", headers=headers).json()
    for c in cols:
        if c.get("name") == name:
            print(f"[Setup] Collection '{name}' already exists (id={c['id']})")
            return c["id"]
    r = requests.post(
        f"{METABASE_URL}/api/collection",
        headers=headers,
        json={"name": name, "color": "#509EE3"}
    )
    col_id = r.json()["id"]
    print(f"[Setup] Collection '{name}' created (id={col_id})")
    return col_id


def create_card(token, collection_id, name, description, sql, display="table", viz_settings=None):
    headers = {"X-Metabase-Session": token}

    # Kiểm tra đã có chưa
    cards = requests.get(f"{METABASE_URL}/api/card", headers=headers).json()
    for c in cards:
        if c.get("name") == name:
            print(f"[Setup] Card '{name}' already exists, skipping.")
            return c["id"]

    payload = {
        "name": name,
        "description": description,
        "collection_id": collection_id,
        "display": display,
        "dataset_query": {
            "type": "native",
            "native": {
                "query": sql,
                "template-tags": {}
            },
            "database": None
        },
        "visualization_settings": viz_settings or {}
    }

    # Lấy db_id
    dbs = requests.get(f"{METABASE_URL}/api/database", headers=headers).json()
    for db in dbs.get("data", []):
        if db["name"] == "Retail DB":
            payload["dataset_query"]["database"] = db["id"]
            break

    r = requests.post(f"{METABASE_URL}/api/card", headers=headers, json=payload)
    if r.status_code in (200, 202):
        card_id = r.json()["id"]
        print(f"[Setup] Card '{name}' created (id={card_id})")
        return card_id
    else:
        print(f"[Setup] Failed to create card '{name}': {r.text[:200]}")
        return None


def create_dashboard(token, collection_id, name, card_ids):
    headers = {"X-Metabase-Session": token}

    dashes = requests.get(f"{METABASE_URL}/api/dashboard", headers=headers).json()
    
    # Fix: handle cả list string lẫn list dict
    existing_id = None
    for d in dashes:
        if isinstance(d, dict) and d.get("name") == name:
            existing_id = d["id"]
            break

    if existing_id:
        # Dashboard đã có nhưng có thể trống — add cards lại
        dash_id = existing_id
        print(f"[Setup] Dashboard '{name}' exists (id={dash_id}), re-adding cards...")
    else:
        r = requests.post(
            f"{METABASE_URL}/api/dashboard",
            headers=headers,
            json={"name": name, "collection_id": collection_id}
        )
        dash_id = r.json()["id"]
        print(f"[Setup] Dashboard '{name}' created (id={dash_id})")

    # Xóa cards cũ trước khi add
    current = requests.get(
        f"{METABASE_URL}/api/dashboard/{dash_id}", headers=headers
    ).json()
    existing_cards = current.get("dashcards", [])
    if existing_cards:
        requests.put(
            f"{METABASE_URL}/api/dashboard/{dash_id}/cards",
            headers=headers,
            json={"cards": []}
        )

    # Add cards mới
    positions = [
        {"row": 0,  "col": 0,  "size_x": 12, "size_y": 8},
        {"row": 0,  "col": 12, "size_x": 12, "size_y": 8},
        {"row": 8,  "col": 0,  "size_x": 12, "size_y": 8},
        {"row": 8,  "col": 12, "size_x": 12, "size_y": 8},
    ]

    cards_payload = []
    for i, card_id in enumerate(card_ids):
        if card_id and i < len(positions):
            pos = positions[i]
            cards_payload.append({
                "cardId": card_id,
                "row": pos["row"], "col": pos["col"],
                "size_x": pos["size_x"], "size_y": pos["size_y"],
                "parameter_mappings": [],
                "visualization_settings": {}
            })

    if cards_payload:
        r = requests.put(
            f"{METABASE_URL}/api/dashboard/{dash_id}/cards",
            headers=headers,
            json={"cards": cards_payload}
        )
        print(f"[Setup] Added {len(cards_payload)} cards to '{name}'")

    return dash_id


def main():
    wait_for_metabase()
    setup_admin()
    token = get_token()
    print(f"[Setup] Logged in, token={token[:8]}...")

    add_database(token)
    col_id = create_collection(token, "Retail Analytics")

    # ─── SALES CARDS ────────────────────────────────────────────
    sales_cards = []

    sales_cards.append(create_card(token, col_id,
        "Doanh thu theo store (7 ngày)",
        "Tổng doanh thu từng store trong 7 ngày gần nhất",
        """
        SELECT store_id, SUM(total_revenue) AS total_revenue, SUM(order_count) AS orders
        FROM gold.mart_sales
        WHERE sale_hour >= NOW() - INTERVAL '7 days'
        GROUP BY store_id ORDER BY total_revenue DESC
        """,
        display="bar",
        viz_settings={
            "graph.dimensions": ["store_id"],
            "graph.metrics": ["total_revenue"],
            "graph.x_axis.title_text": "Store",
            "graph.y_axis.title_text": "Revenue (USD)"
        }
    ))

    sales_cards.append(create_card(token, col_id,
        "Sale over time (30 ngày)",
        "Doanh thu và số đơn theo ngày trong 30 ngày",
        """
        SELECT date_trunc('day', sale_hour) AS date,
               SUM(total_revenue) AS total_revenue,
               SUM(order_count)   AS orders
        FROM gold.mart_sales
        WHERE sale_hour >= NOW() - INTERVAL '30 days'
        GROUP BY 1 ORDER BY 1
        """,
        display="line",
        viz_settings={
            "graph.dimensions": ["date"],
            "graph.metrics": ["total_revenue", "orders"]
        }
    ))

    sales_cards.append(create_card(token, col_id,
        "Top 10 sản phẩm bán chạy",
        "Sản phẩm có doanh thu cao nhất từ silver layer",
        """
        SELECT se.product_name,
               se.category,
               COUNT(*)             AS order_count,
               SUM(se.amount)       AS total_revenue,
               ROUND(AVG(se.amount),2) AS avg_price
        FROM silver.sales_enriched se
        WHERE se.sale_date >= NOW() - INTERVAL '7 days'
        GROUP BY se.product_name, se.category
        ORDER BY total_revenue DESC
        LIMIT 10
        """,
        display="table"
    ))

    sales_cards.append(create_card(token, col_id,
        "Doanh thu theo category",
        "Phân bổ doanh thu theo category sản phẩm",
        """
        SELECT category,
               SUM(total_revenue)   AS total_revenue,
               SUM(order_count)     AS orders,
               ROUND(AVG(avg_order_value), 2) AS avg_order
        FROM gold.mart_sales
        WHERE sale_hour >= NOW() - INTERVAL '7 days'
        GROUP BY category ORDER BY total_revenue DESC
        """,
        display="pie",
        viz_settings={
            "pie.dimension": "category",
            "pie.metric": "total_revenue"
        }
    ))

    # ─── ACCOUNT / RFM CARDS ────────────────────────────────────
    rfm_cards = []

    rfm_cards.append(create_card(token, col_id,
        "RFM segment distribution",
        "Phân bổ khách hàng theo segment",
        """
        SELECT segment,
               COUNT(*) AS customers,
               ROUND(AVG(monetary), 2) AS avg_spend,
               ROUND(AVG(frequency), 1) AS avg_orders,
               ROUND(AVG(recency_days), 0) AS avg_recency_days
        FROM gold.mart_account
        GROUP BY segment ORDER BY avg_spend DESC
        """,
        display="bar",
        viz_settings={
            "graph.dimensions": ["segment"],
            "graph.metrics": ["customers"]
        }
    ))

    rfm_cards.append(create_card(token, col_id,
        "Top customers — Champions",
        "Danh sách khách hàng Champions chi tiêu cao nhất",
        """
        SELECT customer_id,
               ROUND(monetary, 2)   AS total_spent,
               frequency            AS total_orders,
               recency_days         AS days_since_last_buy
        FROM gold.mart_account
        WHERE segment = 'Champions'
        ORDER BY monetary DESC
        LIMIT 20
        """,
        display="table"
    ))

    rfm_cards.append(create_card(token, col_id,
        "At Risk customers",
        "Khách hàng cần retention campaign",
        """
        SELECT customer_id,
               ROUND(monetary, 2) AS total_spent,
               frequency          AS total_orders,
               recency_days       AS days_inactive
        FROM gold.mart_account
        WHERE segment = 'At Risk'
        ORDER BY monetary DESC
        LIMIT 20
        """,
        display="table"
    ))

    rfm_cards.append(create_card(token, col_id,
        "Monetary vs Frequency scatter",
        "Tương quan giữa chi tiêu và tần suất mua",
        """
        SELECT customer_id, segment,
               ROUND(monetary, 0)  AS monetary,
               frequency
        FROM gold.mart_account
        ORDER BY monetary DESC
        LIMIT 200
        """,
        display="scatter",
        viz_settings={
            "scatter.bubble": "frequency",
            "graph.dimensions": ["monetary"],
            "graph.metrics": ["frequency"]
        }
    ))

    # ─── INVENTORY CARDS ────────────────────────────────────────
    inv_cards = []

    inv_cards.append(create_card(token, col_id,
        "Inventory status overview",
        "Tổng quan tồn kho tất cả sản phẩm",
        """
        SELECT product_name, category, stock_qty,
               restock_threshold, status, alert_count
        FROM gold.mart_inventory
        ORDER BY CASE status
            WHEN 'CRITICAL' THEN 1
            WHEN 'LOW'      THEN 2
            ELSE 3
        END, stock_qty ASC
        """,
        display="table"
    ))

    inv_cards.append(create_card(token, col_id,
        "Products by inventory status",
        "Số lượng sản phẩm theo trạng thái tồn kho",
        """
        SELECT status, COUNT(*) AS product_count
        FROM gold.mart_inventory
        GROUP BY status ORDER BY product_count DESC
        """,
        display="pie",
        viz_settings={
            "pie.dimension": "status",
            "pie.metric": "product_count"
        }
    ))

    # ─── DASHBOARDS ─────────────────────────────────────────────
    create_dashboard(token, col_id, "Sales Overview", sales_cards)
    create_dashboard(token, col_id, "Customer Insights (RFM)", rfm_cards)
    create_dashboard(token, col_id, "Inventory Management", inv_cards)

    print("\n[Setup] All done!")
    print(f"  Metabase: {METABASE_URL}")
    print(f"  Login: {ADMIN_EMAIL} / {ADMIN_PASS}")
    print("  Dashboards: Sales Overview | Customer Insights | Inventory Management")


if __name__ == "__main__":
    main()
