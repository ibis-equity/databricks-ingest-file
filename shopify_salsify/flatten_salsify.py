from pyspark.sql.functions import col, explode_outer

# Read Bronze
df = spark.read.format("delta").load("/mnt/bronze/salsify_products")

# Step 1: Extract the node object from edges
df_nodes = df.select(explode_outer("edges").alias("edge")) \
             .select("edge.node.*")

# Step 2: Explode the properties array
df_props = df_nodes.withColumn("property", explode_outer("properties"))

# Step 3: Flatten name/value pairs
df_flat = (
    df_props
    .withColumn("property_name", col("property.name"))
    .withColumn("property_value", col("property.value"))
    .drop("properties", "property")
)

df_flat.show(truncate=False)
df_flat.write.format("delta").mode("overwrite").save("/mnt/silver/salsify_products_flat")
