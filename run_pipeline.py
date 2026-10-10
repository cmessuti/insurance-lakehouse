"""Run the local, in-memory insurance Lakehouse demonstration.

From the repository root:
    python run_pipeline.py

Requires the project's existing PySpark environment. Does not write files.
"""

from pathlib import Path
from decimal import Decimal
import sys
import time

from pyspark.sql import SparkSession, functions as F

from src.bronze import load_bronze_datasets
from src.silver import build_silver_datasets
from src.gold import build_gold_datasets


PROJECT_ROOT = Path(__file__).resolve().parent
EXPECTED_SILVER = {
    "customers": (8, 1),
    "policies": (8, 2),
    "claims": (7, 2),
}
EXPECTED_GOLD = {
    "gold_policy_portfolio": 4,
    "gold_claims_summary": 3,
    "gold_customer_360": 8,
}


def total(df, column):
    value = df.agg(F.sum(F.col(column))).first()[0]
    return Decimal(str(value or 0))


def run(spark):
    started = time.perf_counter()
    print("\nINSURANCE LAKEHOUSE | LOCAL PIPELINE")
    print("=" * 66)
    print(f"Project root: {PROJECT_ROOT}")
    print("Storage: in-memory Spark DataFrames (no Delta/Parquet writes)")

    print("\n[1/3] BRONZE INGESTION")
    bronze = load_bronze_datasets(spark, PROJECT_ROOT)
    if not bronze:
        raise ValueError("Bronze returned no datasets")
    bronze_counts = {name: df.count() for name, df in bronze.items()}
    for name, count in bronze_counts.items():
        print(f"  {name:<24} records={count}")

    print("\n[2/3] SILVER TRANSFORMATION & QUALITY")
    valid, rejected = build_silver_datasets(bronze, PROJECT_ROOT)
    for name in bronze:
        if name not in valid or name not in rejected:
            raise ValueError(f"Missing Silver output for {name}")
        good, bad = valid[name].count(), rejected[name].count()
        print(f"  {name:<24} valid={good:<4} rejected={bad}")
        if name in EXPECTED_SILVER and (good, bad) != EXPECTED_SILVER[name]:
            raise AssertionError(
                f"Silver {name}: expected {EXPECTED_SILVER[name]}, got {(good, bad)}"
            )

    print("\n[3/3] GOLD ANALYTICS")
    gold = build_gold_datasets(valid)
    for name, expected in EXPECTED_GOLD.items():
        if name not in gold:
            raise ValueError(f"Missing Gold dataset: {name}")
        count = gold[name].count()
        print(f"  {name:<24} records={count}")
        if count != expected:
            raise AssertionError(f"Gold {name}: expected {expected}, got {count}")

    customers, policies, claims = (valid[n] for n in ("customers", "policies", "claims"))
    portfolio = gold["gold_policy_portfolio"]
    summary = gold["gold_claims_summary"]
    customer_360 = gold["gold_customer_360"]
    checks = [
        ("Customer count", customer_360.count(), customers.count()),
        ("Policy count", total(portfolio, "policy_count"), policies.count()),
        ("Claim count", total(summary, "claim_count"), claims.count()),
        ("Policy premium total", total(portfolio, "total_premium"), total(policies, "premium")),
        ("Claim estimated amount", total(summary, "total_estimated_amount"), total(claims, "estimated_amount")),
        ("Claim paid amount", total(summary, "total_paid_amount"), total(claims, "paid_amount")),
        ("Customer 360 premium total", total(customer_360, "total_premium"), total(policies, "premium")),
        ("Customer 360 claim count", total(customer_360, "claim_count"), claims.count()),
    ]
    print("\nGOLD RECONCILIATION")
    print("-" * 66)
    failures = []
    for label, actual, expected in checks:
        passed = Decimal(str(actual)) == Decimal(str(expected))
        print(f"{'PASS' if passed else 'FAIL'} | {label} | actual={actual} | expected={expected}")
        if not passed:
            failures.append(label)
    if failures:
        raise AssertionError(f"Gold reconciliation failed: {', '.join(failures)}")

    print("\nPIPELINE SUMMARY")
    print("-" * 66)
    print(f"Bronze records  : {sum(bronze_counts.values())}")
    print(f"Silver valid    : {sum(df.count() for df in valid.values())}")
    print(f"Silver rejected : {sum(df.count() for df in rejected.values())}")
    print(f"Gold checks     : {len(checks)}/{len(checks)} passed")
    print(f"Duration        : {time.perf_counter() - started:.2f}s")
    print("SUCCESS | Local pipeline completed (in memory)")


def main():
    for relative in ("config/bronze_sources.json", "src/bronze.py", "src/silver.py", "src/gold.py"):
        if not (PROJECT_ROOT / relative).is_file():
            print(f"ERROR | Missing project file: {relative}", file=sys.stderr)
            return 1
    spark = None
    try:
        spark = SparkSession.builder.appName("InsuranceLakehousePipeline").master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        run(spark)
        return 0
    except Exception as exc:
        print(f"\nFAILED | {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
