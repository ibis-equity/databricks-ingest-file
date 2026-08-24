# Databricks notebook source
# MAGIC %md
# MAGIC # Flatten Nested JSON to Delta
# MAGIC
# MAGIC This notebook reads nested JSON from a Unity Catalog Volume, flattens nested fields,
# MAGIC and writes to a Delta table.

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, expr

# COMMAND ----------

# Widgets for reusable runs.
dbutils.widgets.text("input_path", "/Volumes/workspace/default/raw_filings/people_nested_1000.json")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("table", "people_flat_1000")
dbutils.widgets.dropdown("mode", "overwrite", ["overwrite", "append"])

input_path = dbutils.widgets.get("input_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
mode = dbutils.widgets.get("mode")

print(f"Input path: {input_path}")
print(f"Target table: {catalog}.{schema}.{table}")
print(f"Mode: {mode}")

# COMMAND ----------

# Read nested JSON array from file.
source_df = spark.read.option("multiline", "true").json(input_path)

# Flatten structs and array values for analytics-friendly columns.
flat_df = (
    source_df.withColumn("first_name", col("name.first"))
    .withColumn("last_name", col("name.last"))
    .withColumn("email", col("contact.email"))
    .withColumn("mobile_phone", expr("filter(contact.phones, x -> x.type = 'mobile')[0].number"))
    .withColumn("work_phone", expr("filter(contact.phones, x -> x.type = 'work')[0].number"))
    .withColumn("street", col("address.street"))
    .withColumn("city", col("address.city"))
    .withColumn("state", col("address.state"))
    .withColumn("zip", col("address.zip"))
    .withColumn("company", col("employment.company"))
    .withColumn("role", col("employment.role"))
    .withColumn("skills", col("employment.skills"))
    .withColumn("skills_csv", concat_ws(",", col("employment.skills")))
    .drop("name", "contact", "address", "employment")
)

# COMMAND ----------

target_schema = f"`{catalog}`.`{schema}`"
target_table = f"{target_schema}.`{table}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

(
    flat_df.write.format("delta")
    .mode(mode)
    .option("overwriteSchema", "true" if mode == "overwrite" else "false")
    .saveAsTable(target_table)
)

print(f"Wrote {flat_df.count()} rows to {catalog}.{schema}.{table}")

# COMMAND ----------

# Preview output
display(spark.table(f"{catalog}.{schema}.{table}").limit(20))

