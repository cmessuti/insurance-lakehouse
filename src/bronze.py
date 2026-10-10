"""Metadata-driven Bronze ingestion for the local insurance lakehouse demo."""

import json
from pathlib import Path

from pyspark.sql import functions as F


def load_json(path):
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def load_bronze_source(spark, project_root, source):
    """Read one configured source and add technical ingestion metadata."""
    project_root = Path(project_root)
    source_name = source["source_name"]
    file_path = project_root / "data" / "source" / source["file_name"]

    if not file_path.is_file():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    return (
        spark.read
        .format(source["file_format"])
        .option("header", source.get("header", True))
        .option("delimiter", source.get("delimiter", ","))
        .load(str(file_path))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_name", F.lit(source_name))
        .withColumn("_source_file", F.lit(source["file_name"]))
    )


def load_bronze_datasets(spark, project_root):
    """Load active sources as in-memory DataFrames; fail fast on errors."""
    project_root = Path(project_root)
    config = load_json(project_root / "config" / "bronze_sources.json")
    bronze_dfs = {}

    for source in config["sources"]:
        if not source.get("active", True):
            continue

        source_name = source["source_name"]
        if source_name in bronze_dfs:
            raise ValueError(f"Duplicate source configuration: {source_name}")

        bronze_dfs[source_name] = load_bronze_source(
            spark, project_root, source
        )

    return bronze_dfs
