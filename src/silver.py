from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .bronze import load_json


def deduplicate_by_primary_key(df, primary_keys):
    if not primary_keys:
        return df

    # Deterministic tie-breaker for the local demo.
    # A production pipeline should use a source update timestamp
    # or another authoritative record-ordering field.
    source_columns = sorted(
        column for column in df.columns
        if column not in {
            "_ingestion_timestamp",
            "_source_name",
            "_source_file"
        }
    )

    tie_breaker = F.sha2(
        F.to_json(
            F.struct(
                *[F.col(column) for column in source_columns]
            ),
            {"ignoreNullFields": "false"}
        ),
        256
    )

    window = (
        Window.partitionBy(*primary_keys)
        .orderBy(tie_breaker.desc())
    )

    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def apply_silver_transformations(df, rules):
    for column, data_type in rules.get("casts", {}).items():
        df = df.withColumn(
            column,
            F.col(column).cast(data_type)
        )

    for column, date_format in rules.get("dates", {}).items():
        df = df.withColumn(
            column,
            F.to_date(F.col(column), date_format)
        )

    for column in rules.get("trim", []):
        df = df.withColumn(column, F.trim(F.col(column)))

    for column in rules.get("lower", []):
        df = df.withColumn(column, F.lower(F.col(column)))

    for column in rules.get("upper", []):
        df = df.withColumn(column, F.upper(F.col(column)))

    for column in rules.get("initcap", []):
        df = df.withColumn(column, F.initcap(F.col(column)))

    return deduplicate_by_primary_key(
        df,
        rules.get("primary_keys", [])
    )


def build_quality_conditions(df, rules):
    conditions = []

    for column in rules.get("not_null", []):
        conditions.append((
            f"{column}_NOT_NULL",
            F.col(column).isNotNull()
        ))

    for column in rules.get("positive", []):
        conditions.append((
            f"{column}_POSITIVE",
            F.col(column).isNull() | (F.col(column) > 0)
        ))

    for column in rules.get("non_negative", []):
        conditions.append((
            f"{column}_NON_NEGATIVE",
            F.col(column).isNull() | (F.col(column) >= 0)
        ))

    for column, allowed in rules.get(
        "allowed_values", {}
    ).items():
        conditions.append((
            f"{column}_ALLOWED_VALUES",
            F.col(column).isin(allowed)
        ))

    for comparison in rules.get("column_comparisons", []):
        left = comparison["left"]
        right = comparison["right"]
        operator = comparison["operator"]

        operators = {
            ">=": lambda a, b: a >= b,
            ">": lambda a, b: a > b,
            "<=": lambda a, b: a <= b,
            "<": lambda a, b: a < b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b
        }

        if operator not in operators:
            raise ValueError(
                f"Unsupported comparison operator: {operator}"
            )

        conditions.append((
            f"{left}_{operator}_{right}",
            operators[operator](
                F.col(left),
                F.col(right)
            )
        ))

    return conditions


def apply_quality_rules(df, rules):
    conditions = build_quality_conditions(df, rules)

    if not conditions:
        return df.withColumn(
            "_dq_errors",
            F.expr("cast(array() as array<string>)")
        )

    errors = [
        F.when(condition, F.lit(None))
        .otherwise(F.lit(rule_name))
        for rule_name, condition in conditions
    ]

    return df.withColumn(
        "_dq_errors",
        F.array_compact(F.array(*errors))
    )


def apply_referential_integrity(
    child_df,
    parent_valid_df,
    child_column,
    parent_column
):
    parent_keys = (
        parent_valid_df
        .select(
            F.col(parent_column).alias("_parent_key")
        )
        .distinct()
    )

    return (
        child_df
        .join(
            parent_keys,
            F.col(child_column) == F.col("_parent_key"),
            "left"
        )
        .withColumn(
            "_dq_errors",
            F.when(
                F.col("_parent_key").isNull(),
                F.concat(
                    F.col("_dq_errors"),
                    F.array(
                        F.lit(
                            f"{child_column}_REFERENTIAL_INTEGRITY"
                        )
                    )
                )
            ).otherwise(F.col("_dq_errors"))
        )
        .drop("_parent_key")
    )


def build_silver_datasets(bronze_dfs, project_root):
    from pathlib import Path

    project_root = Path(project_root)

    transformations = load_json(
        project_root / "config" / "silver_transformations.json"
    )

    quality_config = load_json(
        project_root / "config" / "silver_quality_rules.json"
    )

    validated_dfs = {}

    for dataset_name, bronze_df in bronze_dfs.items():
        if dataset_name not in transformations:
            raise ValueError(
                f"Missing Silver transformations: {dataset_name}"
            )

        transformed = apply_silver_transformations(
            bronze_df,
            transformations[dataset_name]
        )

        validated_dfs[dataset_name] = apply_quality_rules(
            transformed,
            quality_config.get(dataset_name, {})
        )

    relationships = quality_config.get("relationships", [])
    pending = list(relationships)
    processed = set()

    # Process relationships in dependency order.
    # Parent datasets must be validated before child datasets.
    while pending:
        progress = False

        for relationship in pending[:]:
            child = relationship["child_dataset"]
            parent = relationship["parent_dataset"]

            if child not in validated_dfs or parent not in validated_dfs:
                raise ValueError(
                    f"Unknown relationship dataset: {child} -> {parent}"
                )

            parent_has_pending_relationship = any(
                other["child_dataset"] == parent
                for other in pending
            )

            if parent_has_pending_relationship:
                continue

            parent_valid = validated_dfs[parent].filter(
                F.size("_dq_errors") == 0
            )

            validated_dfs[child] = apply_referential_integrity(
                validated_dfs[child],
                parent_valid,
                relationship["child_column"],
                relationship["parent_column"]
            )

            pending.remove(relationship)
            processed.add(child)
            progress = True

        if not progress:
            raise ValueError(
                "Circular or unresolved dataset relationships"
            )

    valid_dfs = {}
    rejected_dfs = {}

    for dataset_name, df in validated_dfs.items():
        valid_dfs[dataset_name] = (
            df.filter(F.size("_dq_errors") == 0)
            .drop("_dq_errors")
        )

        rejected_dfs[dataset_name] = (
            df.filter(F.size("_dq_errors") > 0)
        )

    return valid_dfs, rejected_dfs