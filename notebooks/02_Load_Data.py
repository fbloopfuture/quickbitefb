# Databricks notebook source
# MAGIC %md
# MAGIC # 🛵 QuickBite · 02 – Load the data into the tables
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Purpose** | Fill the tables created in `01_DDL_Create_Tables` with the raw files of the volume |
# MAGIC | **Layer** | Bronze / raw (+ `order_items`, the first cleaned table) |
# MAGIC | **Owner** | Francisco Rosa Belo |
# MAGIC | **Compute** | Serverless |
# MAGIC | **Run after** | `00_Setup_and_Load_Raw_Files` → `01_DDL_Create_Tables` |
# MAGIC | **Validate with** | `99_Validate_Tables` (parts A + B) |
# MAGIC
# MAGIC ### Load strategy
# MAGIC | Table | Source | Method | Why |
# MAGIC |---|---|---|---|
# MAGIC | `restaurants`, `couriers`, `ratings` | CSV | `INSERT OVERWRITE ... FROM read_files()` | small files sent **complete** every day → replace everything |
# MAGIC | `orders` | `orders_*.json` | `COPY INTO` | new files arrive twice a day → load **only new files** (idempotent) |
# MAGIC | `order_items` | `orders` | `INSERT OVERWRITE ... explode(items)` | 1 row per order × dish, no duplicates, no test orders, price as a number |
# MAGIC
# MAGIC ---
# MAGIC ## Change History
# MAGIC | Version | Date | Author | Change | Reason |
# MAGIC |---|---|---|---|---|
# MAGIC | 1.0 | 2026-11-19 | Francisco Rosa Belo | Initial version: load of the 5 QuickBite tables | QuickBite training – day 4 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 1. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema",  "quickbite", "Schema")
dbutils.widgets.text("volume",  "raw",       "Volume")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
volume  = dbutils.widgets.get("volume")

raw_path = f"/Volumes/{catalog}/{schema}/{volume}/"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print("Using:", spark.sql("SELECT current_catalog(), current_schema()").first())
print("Files from:", raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 2. Load the reference tables (CSV)
# MAGIC The table has **types** (e.g. `stars INT`), the CSV has **text** → `CAST` in the `SELECT`.
# MAGIC `INSERT OVERWRITE` replaces all rows → running it twice gives the same result.

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE restaurants
SELECT restaurant_id, name, city, cuisine
FROM read_files('{raw_path}restaurants.csv', format => 'csv', header => true)
""")

spark.sql(f"""
INSERT OVERWRITE couriers
SELECT courier_id, name, vehicle
FROM read_files('{raw_path}couriers.csv', format => 'csv', header => true)
""")

spark.sql(f"""
INSERT OVERWRITE ratings
SELECT order_id, CAST(stars AS INT) AS stars, comment
FROM read_files('{raw_path}ratings.csv', format => 'csv', header => true)
""")

for t in ["restaurants", "couriers", "ratings"]:
    print(f"{t:12} {spark.table(t).count():>5} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 3. Load the orders (JSON) with `COPY INTO`
# MAGIC - Only files matching `orders_*.json`
# MAGIC - `customer` and `items` are rebuilt with `named_struct` / `transform` so the fields match the DDL order and types
# MAGIC - Dates converted from text to `TIMESTAMP`
# MAGIC - 2 traceability columns: `source_file`, `ingested_at`

# COMMAND ----------

copy_result = spark.sql(f"""
COPY INTO orders
FROM (
  SELECT order_id,
         named_struct('id', customer.id, 'email', customer.email, 'city', customer.city)                    AS customer,
         restaurant_id,
         courier_id,
         transform(items, i -> named_struct('dish', i.dish, 'qty', CAST(i.qty AS INT), 'price', i.price))  AS items,
         status,
         CAST(created_at   AS TIMESTAMP) AS created_at,
         CAST(delivered_at AS TIMESTAMP) AS delivered_at,
         _metadata.file_name             AS source_file,
         current_timestamp()             AS ingested_at
  FROM '{raw_path}'
)
FILEFORMAT = JSON
PATTERN = 'orders_*.json'
""")
display(copy_result)   # num_inserted_rows: 786 the 1st time, 0 when re-run (same files)

# COMMAND ----------

# MAGIC %md
# MAGIC > 🛟 **Plan B** – only if the `COPY INTO` above fails because of `named_struct` / `transform`:
# MAGIC > run the cell below instead (same result, but **not idempotent**: running it twice duplicates the rows).

# COMMAND ----------

# PLAN B – uncomment only if COPY INTO fails
# spark.sql(f"""
# INSERT INTO orders
# SELECT order_id,
#        named_struct('id', customer.id, 'email', customer.email, 'city', customer.city),
#        restaurant_id, courier_id,
#        transform(items, i -> named_struct('dish', i.dish, 'qty', CAST(i.qty AS INT), 'price', i.price)),
#        status,
#        CAST(created_at AS TIMESTAMP), CAST(delivered_at AS TIMESTAMP),
#        _metadata.file_name, current_timestamp()
# FROM read_files('{raw_path}orders_*.json', format => 'json')
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🍽️ 4. Load `order_items` (1 row per order × dish)
# MAGIC In one statement:
# MAGIC 1. **remove test orders** (`@quickbite.test`) and **duplicates** (`ROW_NUMBER ... = 1`)
# MAGIC 2. **explode** the `items` array → one row per dish
# MAGIC 3. **convert the price** `"12,50 €"` / `"12.50 €"` / `"€12.50"` → `DECIMAL(10,2)`

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE order_items
# MAGIC WITH one_row_per_order AS (
# MAGIC   SELECT order_id, items
# MAGIC   FROM orders
# MAGIC   WHERE customer.email NOT LIKE '%@quickbite.test'
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_file) = 1
# MAGIC ),
# MAGIC exploded AS (
# MAGIC   SELECT order_id,
# MAGIC          item.dish AS dish,
# MAGIC          item.qty  AS qty,
# MAGIC          CAST(replace(regexp_replace(item.price, '[^0-9,.]', ''), ',', '.') AS DECIMAL(10,2)) AS unit_price
# MAGIC   FROM one_row_per_order
# MAGIC   LATERAL VIEW explode(items) AS item
# MAGIC )
# MAGIC SELECT order_id,
# MAGIC        dish,
# MAGIC        qty,
# MAGIC        unit_price,
# MAGIC        CAST(qty * unit_price AS DECIMAL(12,2)) AS line_total
# MAGIC FROM exploded;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick look: the 3 price formats are now numbers
# MAGIC SELECT * FROM order_items WHERE order_id = 'Q00001';

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 5. Validation
# MAGIC Expected for **batch 1**: restaurants 15 · couriers 12 · ratings 587 · orders 786 · order_items 1394

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'restaurants' AS table_name, COUNT(*) AS rows, 15   AS expected FROM restaurants UNION ALL
# MAGIC SELECT 'couriers',    COUNT(*), 12   FROM couriers    UNION ALL
# MAGIC SELECT 'ratings',     COUNT(*), 587  FROM ratings     UNION ALL
# MAGIC SELECT 'orders',      COUNT(*), 786  FROM orders      UNION ALL
# MAGIC SELECT 'order_items', COUNT(*), 1394 FROM order_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Every load is a new version of the table
# MAGIC DESCRIBE HISTORY orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 Try it
# MAGIC 1. Run the **COPY INTO** cell (section 3) again. How many rows were inserted? Why?
# MAGIC 2. Run section 2 again. Do the counts change? Why not?
# MAGIC 3. Why does `orders` have **786** rows but `order_items` is built from only **709** orders?
# MAGIC 4. `SELECT * FROM orders WHERE order_id = 'Q00001'` vs `SELECT * FROM order_items WHERE order_id = 'Q00001'` – what changed?
# MAGIC
# MAGIC ➡️ Next: run **`99_Validate_Tables`**, then notebook **`03_Views`**.
