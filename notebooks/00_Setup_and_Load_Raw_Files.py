# Databricks notebook source
# MAGIC %md
# MAGIC # 🛵 QuickBite · 00 – Setup and load the raw files
# MAGIC
# MAGIC **Goal:** prepare the place where the data will live and put the raw files there.
# MAGIC
# MAGIC ```
# MAGIC  Git repo (files)  ──copy──►  Volume  workspace.quickbite.raw  ──►  next notebooks (DDL, load, views)
# MAGIC ```
# MAGIC
# MAGIC | Cell | What it does |
# MAGIC |---|---|
# MAGIC | 1 | parameters: catalog, schema, volume |
# MAGIC | 2 | create the schema and the volume |
# MAGIC | 3 | find the data files in this Git folder |
# MAGIC | 4 | copy them to the volume |
# MAGIC | 5 | check: list the volume |
# MAGIC | 6 | first look at the data |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Parameters
# MAGIC Widgets let us run the same notebook in another catalog/schema (and later from a **job**).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "quickbite")
dbutils.widgets.text("volume", "raw")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
volume  = dbutils.widgets.get("volume")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}/"
print("Target volume:", volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Create the schema and the volume
# MAGIC `IF NOT EXISTS` → the cell can run many times without errors (idempotent).

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'QuickBite training project'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} COMMENT 'Raw files as received from the QuickBite app'")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print("Using:", spark.sql("SELECT current_catalog(), current_schema()").first())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Find the data files in this Git folder
# MAGIC The data files are in the **root of the repo**; this notebook is in the `notebooks/` folder → the repo root is **one level up**.

# COMMAND ----------

import os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))   # .../quickbite

data_files = sorted(f for f in os.listdir(repo_root) if f.endswith((".json", ".csv")))
print("Repo root:", repo_root)
print(len(data_files), "data files found:")
for f in data_files:
    print("  ", f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Copy the files to the volume
# MAGIC Why copy? Files in a Git folder are **code-side** files. The **volume** is the governed place for data in Unity Catalog
# MAGIC (permissions, lineage) and the place COPY INTO / Auto Loader read from.

# COMMAND ----------

import shutil

for f in data_files:
    shutil.copy(os.path.join(repo_root, f), os.path.join(volume_path, f))
print(f"Copied {len(data_files)} files to {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Check: list the volume
# MAGIC Expected: **17 files** – 14 `orders_*.json` + `restaurants.csv`, `couriers.csv`, `ratings.csv`.

# COMMAND ----------

files = dbutils.fs.ls(volume_path)
display(files)
print(len(files), "files in the volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · First look at the data (before writing any DDL)
# MAGIC For each source, write down: **format · columns · types · anything strange?**

# COMMAND ----------

for name in ["restaurants", "couriers", "ratings"]:
    print(f"--- {name}.csv ---")
    display(spark.sql(f"SELECT * FROM read_files('{volume_path}{name}.csv', format => 'csv', header => true) LIMIT 5"))

# COMMAND ----------

orders_preview = spark.sql(f"SELECT * FROM read_files('{volume_path}orders_*.json', format => 'json')")
orders_preview.printSchema()
display(orders_preview.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Your notes (double-click to edit)
# MAGIC | Source | Format | Columns | Something strange? |
# MAGIC |---|---|---|---|
# MAGIC | `restaurants.csv` | | | |
# MAGIC | `couriers.csv` | | | |
# MAGIC | `ratings.csv` | | | |
# MAGIC | `orders_*.json` | | | |
# MAGIC
# MAGIC ➡️ Next notebook: **`01_DDL_Create_Tables`**
