# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ QuickBite · 99 – Validate the tables
# MAGIC
# MAGIC Run this notebook **after `01_DDL_Create_Tables`** (part A) and **after `02_Load_Data`** (part B).
# MAGIC Every check returns **PASS** or **FAIL**. The last cell **fails the notebook** if any check failed – so it can also run as the last task of a job.
# MAGIC
# MAGIC | Part | What it checks |
# MAGIC |---|---|
# MAGIC | A · Structure | tables exist · managed · have a comment · every expected column exists with the right **type** and **nullability** · no unexpected columns |
# MAGIC | B · Data | row counts · keys not NULL · keys unique · no orphan orders |
# MAGIC | C · Result | summary + stop with an error if anything failed |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 1. Parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE OR REPLACE VARIABLE v_catalog STRING DEFAULT 'workspace';
# MAGIC DECLARE OR REPLACE VARIABLE v_schema  STRING DEFAULT 'quickbite';
# MAGIC
# MAGIC USE CATALOG IDENTIFIER(v_catalog);
# MAGIC USE SCHEMA  IDENTIFIER(v_schema);
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ 2. Part A – Structure (run after the DDL)
# MAGIC ### 2.1 Expected design
# MAGIC The single source of truth for the checks below. Change it here if the design changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW expected_columns AS
# MAGIC SELECT * FROM VALUES
# MAGIC   -- table,        column,          type (lower case, no spaces),                         nullable
# MAGIC   ('restaurants', 'restaurant_id', 'string',                                              'NO'),
# MAGIC   ('restaurants', 'name',          'string',                                              'YES'),
# MAGIC   ('restaurants', 'city',          'string',                                              'YES'),
# MAGIC   ('restaurants', 'cuisine',       'string',                                              'YES'),
# MAGIC   ('couriers',    'courier_id',    'string',                                              'NO'),
# MAGIC   ('couriers',    'name',          'string',                                              'YES'),
# MAGIC   ('couriers',    'vehicle',       'string',                                              'YES'),
# MAGIC   ('ratings',     'order_id',      'string',                                              'NO'),
# MAGIC   ('ratings',     'stars',         'int',                                                 'YES'),
# MAGIC   ('ratings',     'comment',       'string',                                              'YES'),
# MAGIC   ('orders',      'order_id',      'string',                                              'NO'),
# MAGIC   ('orders',      'customer',      'struct<id:string,email:string,city:string>',          'YES'),
# MAGIC   ('orders',      'restaurant_id', 'string',                                              'YES'),
# MAGIC   ('orders',      'courier_id',    'string',                                              'YES'),
# MAGIC   ('orders',      'items',         'array<struct<dish:string,qty:int,price:string>>',     'YES'),
# MAGIC   ('orders',      'status',        'string',                                              'YES'),
# MAGIC   ('orders',      'created_at',    'timestamp',                                           'YES'),
# MAGIC   ('orders',      'delivered_at',  'timestamp',                                           'YES'),
# MAGIC   ('orders',      'source_file',   'string',                                              'YES'),
# MAGIC   ('orders',      'ingested_at',   'timestamp',                                           'YES'),
# MAGIC   ('order_items', 'order_id',      'string',                                              'NO'),
# MAGIC   ('order_items', 'dish',          'string',                                              'YES'),
# MAGIC   ('order_items', 'qty',           'int',                                                 'YES'),
# MAGIC   ('order_items', 'unit_price',    'decimal(10,2)',                                       'YES'),
# MAGIC   ('order_items', 'line_total',    'decimal(12,2)',                                       'YES')
# MAGIC AS t(table_name, column_name, expected_type, expected_nullable);
# MAGIC
# MAGIC SELECT table_name, COUNT(*) AS expected_columns FROM expected_columns GROUP BY table_name ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Actual columns (from `information_schema`)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW actual_columns AS
# MAGIC SELECT table_name,
# MAGIC        column_name,
# MAGIC        regexp_replace(lower(full_data_type), '\\s', '') AS actual_type,
# MAGIC        is_nullable                                      AS actual_nullable
# MAGIC FROM IDENTIFIER(v_catalog || '.information_schema.columns')
# MAGIC WHERE table_schema = v_schema
# MAGIC   AND table_name IN (SELECT DISTINCT table_name FROM expected_columns);
# MAGIC
# MAGIC SELECT * FROM actual_columns ORDER BY table_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Column checks: missing · wrong type · wrong nullability · unexpected

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW check_columns AS
# MAGIC SELECT 'A-column'                                   AS check_group,
# MAGIC        coalesce(e.table_name, a.table_name)        AS table_name,
# MAGIC        coalesce(e.column_name, a.column_name)      AS item,
# MAGIC        CASE
# MAGIC          WHEN a.column_name IS NULL                         THEN 'FAIL'
# MAGIC          WHEN e.column_name IS NULL                         THEN 'FAIL'
# MAGIC          WHEN a.actual_type     <> e.expected_type          THEN 'FAIL'
# MAGIC          WHEN a.actual_nullable <> e.expected_nullable      THEN 'FAIL'
# MAGIC          ELSE 'PASS'
# MAGIC        END                                          AS status,
# MAGIC        CASE
# MAGIC          WHEN a.column_name IS NULL                         THEN 'missing column'
# MAGIC          WHEN e.column_name IS NULL                         THEN 'unexpected column'
# MAGIC          WHEN a.actual_type     <> e.expected_type          THEN concat('type is ', a.actual_type, ', expected ', e.expected_type)
# MAGIC          WHEN a.actual_nullable <> e.expected_nullable      THEN concat('nullable is ', a.actual_nullable, ', expected ', e.expected_nullable)
# MAGIC          ELSE 'ok'
# MAGIC        END                                          AS detail
# MAGIC FROM expected_columns e
# MAGIC FULL OUTER JOIN actual_columns a
# MAGIC   ON e.table_name = a.table_name AND e.column_name = a.column_name;
# MAGIC
# MAGIC SELECT * FROM check_columns ORDER BY status, table_name, item;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Table checks: exists · managed · has a comment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW check_tables AS
# MAGIC WITH expected AS (SELECT DISTINCT table_name FROM expected_columns),
# MAGIC actual AS (
# MAGIC   SELECT table_name, table_type, comment
# MAGIC   FROM IDENTIFIER(v_catalog || '.information_schema.tables')
# MAGIC   WHERE table_schema = v_schema
# MAGIC )
# MAGIC SELECT 'A-table' AS check_group, e.table_name, 'exists' AS item,
# MAGIC        IF(a.table_name IS NOT NULL, 'PASS', 'FAIL') AS status,
# MAGIC        IF(a.table_name IS NOT NULL, 'ok', 'table not found') AS detail
# MAGIC FROM expected e LEFT JOIN actual a USING (table_name)
# MAGIC UNION ALL
# MAGIC SELECT 'A-table', e.table_name, 'managed',
# MAGIC        IF(a.table_type = 'MANAGED', 'PASS', 'FAIL'),
# MAGIC        concat('table_type = ', coalesce(a.table_type, 'n/a'))
# MAGIC FROM expected e LEFT JOIN actual a USING (table_name)
# MAGIC UNION ALL
# MAGIC SELECT 'A-table', e.table_name, 'has comment',
# MAGIC        IF(length(trim(a.comment)) > 0, 'PASS', 'FAIL'),
# MAGIC        coalesce(a.comment, 'no comment')
# MAGIC FROM expected e LEFT JOIN actual a USING (table_name);
# MAGIC
# MAGIC SELECT * FROM check_tables ORDER BY status, table_name, item;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 3. Part B – Data (run after `02_Load_Data`)
# MAGIC Skip this part if the tables were only created (they are empty after the DDL).
# MAGIC Expected numbers are for **batch 1**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW check_data AS
# MAGIC -- row counts
# MAGIC SELECT 'B-count' AS check_group, 'restaurants' AS table_name, 'row count = 15'  AS item,
# MAGIC        IF(COUNT(*) = 15,  'PASS', 'FAIL') AS status, concat('rows = ', COUNT(*)) AS detail FROM restaurants
# MAGIC UNION ALL
# MAGIC SELECT 'B-count', 'couriers', 'row count = 12',
# MAGIC        IF(COUNT(*) = 12,  'PASS', 'FAIL'), concat('rows = ', COUNT(*)) FROM couriers
# MAGIC UNION ALL
# MAGIC SELECT 'B-count', 'ratings', 'row count = 587',
# MAGIC        IF(COUNT(*) = 587, 'PASS', 'FAIL'), concat('rows = ', COUNT(*)) FROM ratings
# MAGIC UNION ALL
# MAGIC SELECT 'B-count', 'orders', 'row count = 786 (raw, with duplicates)',
# MAGIC        IF(COUNT(*) = 786, 'PASS', 'FAIL'), concat('rows = ', COUNT(*)) FROM orders
# MAGIC UNION ALL
# MAGIC SELECT 'B-count', 'order_items', 'row count = 1394',
# MAGIC        IF(COUNT(*) = 1394, 'PASS', 'FAIL'), concat('rows = ', COUNT(*)) FROM order_items
# MAGIC -- keys not null
# MAGIC UNION ALL
# MAGIC SELECT 'B-key', 'orders', 'order_id not null',
# MAGIC        IF(COUNT_IF(order_id IS NULL) = 0, 'PASS', 'FAIL'), concat('nulls = ', COUNT_IF(order_id IS NULL)) FROM orders
# MAGIC -- keys unique
# MAGIC UNION ALL
# MAGIC SELECT 'B-key', 'restaurants', 'restaurant_id unique',
# MAGIC        IF(COUNT(*) = COUNT(DISTINCT restaurant_id), 'PASS', 'FAIL'),
# MAGIC        concat('rows = ', COUNT(*), ', distinct = ', COUNT(DISTINCT restaurant_id)) FROM restaurants
# MAGIC UNION ALL
# MAGIC SELECT 'B-key', 'couriers', 'courier_id unique',
# MAGIC        IF(COUNT(*) = COUNT(DISTINCT courier_id), 'PASS', 'FAIL'),
# MAGIC        concat('rows = ', COUNT(*), ', distinct = ', COUNT(DISTINCT courier_id)) FROM couriers
# MAGIC -- referential integrity
# MAGIC UNION ALL
# MAGIC SELECT 'B-ref', 'orders', 'every restaurant_id exists in restaurants',
# MAGIC        IF(COUNT(*) = 0, 'PASS', 'FAIL'), concat('orphans = ', COUNT(*))
# MAGIC FROM orders o LEFT ANTI JOIN restaurants r ON o.restaurant_id = r.restaurant_id
# MAGIC UNION ALL
# MAGIC SELECT 'B-ref', 'orders', 'every courier_id exists in couriers',
# MAGIC        IF(COUNT(*) = 0, 'PASS', 'FAIL'), concat('orphans = ', COUNT(*))
# MAGIC FROM orders o LEFT ANTI JOIN couriers c ON o.courier_id = c.courier_id
# MAGIC -- order_items quality
# MAGIC UNION ALL
# MAGIC SELECT 'B-quality', 'order_items', 'unit_price parsed (no NULL)',
# MAGIC        IF(COUNT_IF(unit_price IS NULL) = 0, 'PASS', 'FAIL'), concat('nulls = ', COUNT_IF(unit_price IS NULL)) FROM order_items
# MAGIC UNION ALL
# MAGIC SELECT 'B-quality', 'order_items', 'line_total = qty x unit_price',
# MAGIC        IF(COUNT_IF(line_total <> qty * unit_price) = 0, 'PASS', 'FAIL'),
# MAGIC        concat('wrong rows = ', COUNT_IF(line_total <> qty * unit_price)) FROM order_items;
# MAGIC
# MAGIC SELECT * FROM check_data ORDER BY status, check_group, table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 4. Part C – Result
# MAGIC Summary of all checks. The last cell **stops with an error** if any check failed.
# MAGIC 💡 If you only ran the DDL (no data yet), remove `check_data` from the `UNION ALL` below.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW all_checks AS
# MAGIC SELECT * FROM check_tables
# MAGIC UNION ALL SELECT * FROM check_columns
# MAGIC UNION ALL SELECT * FROM check_data;
# MAGIC
# MAGIC SELECT check_group,
# MAGIC        COUNT(*)                   AS checks,
# MAGIC        COUNT_IF(status = 'PASS')  AS passed,
# MAGIC        COUNT_IF(status = 'FAIL')  AS failed
# MAGIC FROM all_checks
# MAGIC GROUP BY check_group
# MAGIC ORDER BY check_group;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Only the problems (empty result = all good)
# MAGIC SELECT * FROM all_checks WHERE status = 'FAIL' ORDER BY check_group, table_name, item;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stops the notebook (and the job) if anything failed
# MAGIC SELECT assert_true(
# MAGIC          (SELECT COUNT_IF(status = 'FAIL') FROM all_checks) = 0,
# MAGIC          'QuickBite validation FAILED – see the FAIL rows above'
# MAGIC        ) AS all_checks_passed;
