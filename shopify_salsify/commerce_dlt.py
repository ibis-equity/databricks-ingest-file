import dlt
import pyspark.sql.functions as F
import requests
import time
import json

# ---------- Shopify ingestion helpers ----------

SHOPIFY_API_KEY = dlt.secrets.get("shopify", "api_key")
SHOPIFY_PASSWORD = dlt.secrets.get("shopify", "password")
SHOPIFY_STORE = "your-store-name"

base_url = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_PASSWORD}@{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01"

def fetch_shopify_orders():
    url = f"{base_url}/orders.json"
    results = []
    page_info = None

    while True:
        params = {"limit": 250}
        if page_info:
            params["page_info"] = page_info

        resp = requests.get(url, params=params)

        if resp.status_code == 429:
            time.sleep(2)
            continue

        resp.raise_for_status()
        data = resp.json()
        orders = data.get("orders", [])
        results.extend(orders)

        link = resp.headers.get("Link")
        if link and 'rel="next"' in link:
            page_info = link.split("page_info=")[1].split(">")[0]
        else:
            break

    return results

# ---------- Salsify ingestion helpers ----------

SALSIFY_TOKEN = dlt.secrets.get("salsify", "token")
SALSIFY_URL = "https://app.salsify.com/api/graphql"

SALSIFY_QUERY = """
{
  products(first: 500) {
    edges {
      node {
        id
        title
        brand
        properties {
          name
          value
        }
      }
    }
  }
}
"""

def fetch_salsify_products():
    headers = {
        "Authorization": f"Bearer {SALSIFY_TOKEN}",
        "Content-Type": "application/json"
    }
    resp = requests.post(SALSIFY_URL, json={"query": SALSIFY_QUERY}, headers=headers)
    resp.raise_for_status()
    return resp.json()["data"]["products"]["edges"]

# ---------- Bronze: raw Shopify orders ----------

@dlt.table(
    comment="Raw Shopify orders (Bronze)"
)
def bronze_shopify_orders():
    orders = fetch_shopify_orders()
    return (
        spark.read.json(spark.sparkContext.parallelize([json.dumps(orders)]))
    )

# ---------- Bronze: raw Salsify products ----------

@dlt.table(
    comment="Raw Salsify products (Bronze)"
)
def bronze_salsify_products():
    products = fetch_salsify_products()
    return (
        spark.read.json(spark.sparkContext.parallelize([json.dumps(products)]))
    )

# ---------- Silver: normalized Shopify orders ----------

@dlt.table(
    comment="Normalized Shopify orders (Silver)"
)
def silver_shopify_orders():
    df = dlt.read("bronze_shopify_orders")

    df_exp = df.select(
        "id",
        "created_at",
        "customer.email",
        "customer.first_name",
        "customer.last_name",
        F.explode_outer("line_items").alias("line_item")
    )

    return (
        df_exp
        .withColumn("product_id", F.col("line_item.product_id"))
        .withColumn("quantity", F.col("line_item.quantity"))
        .withColumn("price", F.col("line_item.price"))
        .drop("line_item")
    )

# ---------- Silver: normalized Salsify products ----------

@dlt.table(
    comment="Normalized Salsify products (Silver)"
)
def silver_salsify_products():
    df = dlt.read("bronze_salsify_products")

    return (
        df
        .withColumn("product_id", F.col("node.id"))
        .withColumn("title", F.col("node.title"))
        .withColumn("brand", F.col("node.brand"))
        .withColumn("properties", F.col("node.properties"))
        .drop("node")
    )

# ---------- Gold: unified commerce orders ----------

@dlt.table(
    comment="Unified commerce orders (Gold)"
)
def gold_commerce_orders():
    orders = dlt.read("silver_shopify_orders")
    products = dlt.read("silver_salsify_products")

    return (
        orders.join(products, "product_id", "left")
        .select(
            orders["id"].alias("order_id"),
            "created_at",
            "email",
            "first_name",
            "last_name",
            "product_id",
            "title",
            "brand",
            "quantity",
            "price"
        )
    )
