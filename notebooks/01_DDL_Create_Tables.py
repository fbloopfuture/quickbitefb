# Databricks notebook source
# MAGIC %md
# MAGIC # 🛵 QuickBite · 01 – DDL: create the tables (no data yet)
# MAGIC
# MAGIC **DDL** = *Data Definition Language* → the SQL that **defines** a table: name, columns, types, rules (`CREATE`, `ALTER`, `DROP`).
# MAGIC We design the tables **first**; the data comes in notebook `02_Load_Data`.
# MAGIC
# MAGIC | Table | Source file(s) | Columns |
# MAGIC |---|---|---|
# MAGIC | `restaurants` | `restaurants.csv` | restaurant_id, name, city, cuisine |
# MAGIC | `couriers` | `couriers.csv` | courier_id, name, vehicle |
# MAGIC | `ratings` | `ratings.csv` | order_id, stars, comment |
# MAGIC | `orders` | `orders_*.json` | order_id, customer (**struct**), restaurant_id, courier_id, items (**array of struct**), status, created_at, delivered_at |
# MAGIC
# MAGIC **Design decisions to discuss**
# MAGIC - **Types:** `STRING`, `INT`, `TIMESTAMP`, `STRUCT<...>`, `ARRAY<STRUCT<...>>`
# MAGIC - **`NOT NULL`** only on the keys
# MAGIC - **`price` stays `STRING`** here: it arrives in 3 formats (`"12,50 €"`, `"12.50 €"`, `"€12.50"`) → we convert it later in a view
# MAGIC - **`delivered_at` can be NULL:** cancelled orders are never delivered
# MAGIC - **No `CHECK (stars BETWEEN 1 AND 5)` yet:** the file has 0 and 6 stars → the load would fail. Decide first, enforce later
# MAGIC - **`COMMENT`** on every table (documentation lives with the data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Parameters (same as notebook 00)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "quickbite")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print("Using:", spark.sql("SELECT current_catalog(), current_schema()").first())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · `restaurants`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS restaurants (
# MAGIC   restaurant_id STRING NOT NULL COMMENT 'Restaurant key, e.g. R01',
# MAGIC   name          STRING,
# MAGIC   city          STRING          COMMENT 'Raw city as received (not cleaned)',
# MAGIC   cuisine       STRING
# MAGIC )
# MAGIC COMMENT 'QuickBite restaurants – loaded from restaurants.csv';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · `couriers`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS couriers (
# MAGIC   courier_id STRING NOT NULL COMMENT 'Courier key, e.g. K01',
# MAGIC   name       STRING,
# MAGIC   vehicle    STRING          COMMENT 'bike / car / scooter'
# MAGIC )
# MAGIC COMMENT 'QuickBite couriers – loaded from couriers.csv';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · `ratings`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ratings (
# MAGIC   order_id STRING NOT NULL,
# MAGIC   stars    INT             COMMENT 'Should be 1-5 (raw data also has 0 and 6)',
# MAGIC   comment  STRING
# MAGIC )
# MAGIC COMMENT 'Customer ratings per order – loaded from ratings.csv';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · `orders` – the nested one
# MAGIC - `customer` → **STRUCT** (one object with id, email, city)
# MAGIC - `items` → **ARRAY of STRUCT** (a list of dishes, each with dish, qty, price)
# MAGIC - 2 extra columns for traceability: `source_file`, `ingested_at`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS orders (
# MAGIC   order_id      STRING NOT NULL                                   COMMENT 'Order key, e.g. Q00001 (may arrive twice)',
# MAGIC   customer      STRUCT<id: STRING, email: STRING, city: STRING>   COMMENT 'Contains PII (email)',
# MAGIC   restaurant_id STRING,
# MAGIC   courier_id    STRING,
# MAGIC   items         ARRAY<STRUCT<dish: STRING, qty: INT, price: STRING>> COMMENT 'price kept as text: 3 formats in the source',
# MAGIC   status        STRING,
# MAGIC   created_at    TIMESTAMP,
# MAGIC   delivered_at  TIMESTAMP                                         COMMENT 'NULL for cancelled orders',
# MAGIC   source_file   STRING                                            COMMENT 'File the row came from',
# MAGIC   ingested_at   TIMESTAMP                                         COMMENT 'When the row was loaded'
# MAGIC )
# MAGIC COMMENT 'QuickBite orders as received – loaded from orders_*.json';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Check the tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find: the comment, the type of `items`, Type = MANAGED, the Location
# MAGIC DESCRIBE TABLE EXTENDED orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many rows? Why?
# MAGIC SELECT 'restaurants' AS table_name, COUNT(*) AS rows FROM restaurants UNION ALL
# MAGIC SELECT 'couriers',  COUNT(*) FROM couriers UNION ALL
# MAGIC SELECT 'ratings',   COUNT(*) FROM ratings  UNION ALL
# MAGIC SELECT 'orders',    COUNT(*) FROM orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check yourself
# MAGIC 1. Why is every table at **0 rows**?
# MAGIC 2. Why is `price` a `STRING` and not a `DECIMAL`?
# MAGIC 3. Why can `delivered_at` NOT be `NOT NULL`?
# MAGIC 4. What would happen in notebook 02 if `ratings` had `CHECK (stars BETWEEN 1 AND 5)`?
# MAGIC 5. Are these tables **managed** or **external**? How do you know?
# MAGIC
# MAGIC 💡 Bonus: try `INSERT INTO ratings VALUES (NULL, 5, 'test')` – what happens and why?
# MAGIC
# MAGIC ➡️ Next notebook: **`02_Load_Data`**
