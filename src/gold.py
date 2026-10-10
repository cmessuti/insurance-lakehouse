from pyspark.sql import functions as F


def build_gold_datasets(silver_valid_dfs):
    required = {"customers", "policies", "claims"}

    missing = required - set(silver_valid_dfs)

    if missing:
        raise ValueError(
            f"Missing Silver datasets: {sorted(missing)}"
        )

    customers = silver_valid_dfs["customers"]
    policies = silver_valid_dfs["policies"]
    claims = silver_valid_dfs["claims"]

    gold_dfs = {}

    # Policy portfolio
    gold_dfs["gold_policy_portfolio"] = (
        policies
        .groupBy("product", "status")
        .agg(
            F.countDistinct("policy_id").alias("policy_count"),
            F.sum("premium").alias("total_premium"),
            F.avg("premium").alias("average_premium")
        )
        .orderBy("product", "status")
    )

    # Enrich claims with policy information
    claims_enriched = (
        claims.alias("c")
        .join(
            policies.alias("p"),
            F.col("c.policy_id") == F.col("p.policy_id"),
            "inner"
        )
        .select(
            F.col("c.claim_id"),
            F.col("c.policy_id"),
            F.col("p.product"),
            F.col("c.status").alias("claim_status"),
            F.col("c.estimated_amount"),
            F.col("c.paid_amount")
        )
    )

    # Claims summary
    gold_dfs["gold_claims_summary"] = (
        claims_enriched
        .groupBy("product", "claim_status")
        .agg(
            F.countDistinct("claim_id").alias("claim_count"),
            F.sum("estimated_amount").alias(
                "total_estimated_amount"
            ),
            F.coalesce(
                F.sum("paid_amount"),
                F.lit(0).cast("decimal(18,2)")
            ).alias("total_paid_amount"),
            F.count("paid_amount").alias(
                "claims_with_payment_amount"
            )
        )
        .orderBy("product", "claim_status")
    )

    # Customer policy metrics
    customer_policy_metrics = (
        policies
        .groupBy("customer_id")
        .agg(
            F.countDistinct("policy_id").alias("policy_count"),
            F.sum("premium").alias("total_premium")
        )
    )

    # Customer claim metrics
    customer_claim_metrics = (
        claims_enriched
        .join(
            policies.select("policy_id", "customer_id"),
            on="policy_id",
            how="inner"
        )
        .groupBy("customer_id")
        .agg(
            F.countDistinct("claim_id").alias("claim_count"),
            F.sum("estimated_amount").alias(
                "total_estimated_amount"
            ),
            F.sum("paid_amount").alias("total_paid_amount")
        )
    )

    # Customer 360
    gold_dfs["gold_customer_360"] = (
        customers
        .join(
            customer_policy_metrics,
            on="customer_id",
            how="left"
        )
        .join(
            customer_claim_metrics,
            on="customer_id",
            how="left"
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "province",
            F.coalesce(
                F.col("policy_count"),
                F.lit(0)
            ).alias("policy_count"),
            F.coalesce(
                F.col("total_premium"),
                F.lit(0).cast("decimal(18,2)")
            ).alias("total_premium"),
            F.coalesce(
                F.col("claim_count"),
                F.lit(0)
            ).alias("claim_count"),
            F.coalesce(
                F.col("total_estimated_amount"),
                F.lit(0).cast("decimal(18,2)")
            ).alias("total_estimated_amount"),
            F.coalesce(
                F.col("total_paid_amount"),
                F.lit(0).cast("decimal(18,2)")
            ).alias("total_paid_amount")
        )
        .orderBy("customer_id")
    )

    return gold_dfs