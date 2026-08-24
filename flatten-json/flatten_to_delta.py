from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, expr


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flatten nested people JSON and write a Delta table.")
    parser.add_argument(
        "--input-path",
        default=r"C:\Users\desha\PycharmProjects\databricks-ingest-file\flatten-json\people_nested_1000.json",
        help="Input JSON path. Supports local file paths, file:// URIs, dbfs:/, /Volumes/...",
    )
    parser.add_argument("--catalog", default="workspace", help="Target Unity Catalog catalog")
    parser.add_argument("--schema", default="default", help="Target schema")
    parser.add_argument("--table", default="people_flat_1000", help="Target table name")
    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Write mode for the Delta table",
    )
    return parser.parse_args()


def normalize_input_path(input_path: str) -> str:
    lowered = input_path.lower()
    if lowered.startswith(("dbfs:/", "file:/")) or input_path.startswith(("/Volumes/", "/mnt/", "/dbfs/")):
        return input_path

    path_obj = Path(input_path)
    if path_obj.exists():
        return path_obj.resolve().as_uri()

    # Keep unknown paths as-is so Spark can resolve cluster-accessible locations.
    return input_path


def flatten_people(df: DataFrame) -> DataFrame:
    # Extract nested structs/arrays into scalar columns for analytics-friendly Delta schema.
    return (
        df.withColumn("first_name", col("name.first"))
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


def main() -> int:
    args = parse_args()

    spark = SparkSession.builder.appName("flatten-people-json-to-delta").getOrCreate()

    input_path = normalize_input_path(args.input_path)
    print(f"Reading input from: {input_path}")

    source_df = spark.read.option("multiline", "true").json(input_path)
    flat_df = flatten_people(source_df)

    target_schema = f"{args.catalog}.{args.schema}"
    target_table = f"{target_schema}.{args.table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{args.catalog}`.`{args.schema}`")

    (
        flat_df.write.format("delta")
        .mode(args.mode)
        .option("overwriteSchema", "true" if args.mode == "overwrite" else "false")
        .saveAsTable(target_table)
    )

    row_count = flat_df.count()
    print(f"Wrote {row_count} rows to Delta table: {target_table}")
    spark.table(target_table).show(5, truncate=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

