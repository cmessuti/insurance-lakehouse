# Insurance Lakehouse

A metadata-driven Data Engineering portfolio project for a fictional insurance company, built with **Python and PySpark**. It demonstrates a modular Bronze–Silver–Gold data pipeline, configurable ingestion and quality rules, referential integrity checks, and analytical datasets.

> **Implementation status:** The current demo runs locally using synthetic CSV data and in-memory Spark DataFrames. It does **not** persist Parquet or Delta tables, deploy Azure resources, or publish Power BI reports. Azure Databricks and Delta Lake are part of the proposed target architecture.

## Business scenario

An insurance company needs to consolidate customer, policy, and claim information from operational systems. The goal is to validate and standardize incoming data, enforce relationships between entities, and provide consistent datasets for downstream reporting.

The implemented sample includes three sources: `customers`, `policies`, and `claims`. All records are synthetic and are provided for demonstration purposes.

## Architecture

```text
Synthetic CSV files (data/source/)
             |
             v
    Bronze ingestion (PySpark)
             |
             v
    Silver transformation
    - configurable transformations
    - deduplication
    - data quality rules
    - referential integrity
    - valid / rejected datasets
             |
             v
      Gold analytics
    - policy portfolio
    - claims summary
    - customer 360
```

The code is organized into reusable Python modules under `src/`. The notebooks provide executable demonstrations and validation output without duplicating the pipeline logic.

### Proposed cloud architecture (not deployed)

A future Azure implementation could use Azure Data Factory for orchestration, Azure Data Lake Storage for storage, Azure Databricks for Spark processing, Delta Lake for managed Lakehouse tables, and Power BI for reporting. These services are **not required** to run the current local demonstration.

## Repository structure

```text
insurance-lakehouse/
├── config/
│   ├── bronze_sources.json
│   ├── silver_transformations.json
│   └── silver_quality_rules.json
├── data/
│   └── source/
│       ├── customers.csv
│       ├── policies.csv
│       └── claims.csv
├── docs/
│   └── architecture.md
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformation.ipynb
│   └── 03_gold_analytics.ipynb
├── src/
│   ├── __init__.py
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── run_pipeline.py
└── README.md
```

## Pipeline layers

**Bronze — ingestion:** Reads the active sources defined in `config/bronze_sources.json`, loads CSV records into Spark DataFrames, and adds ingestion metadata. The ingestion demonstration logs source-level status, record counts, and elapsed time.

**Silver — trusted data:** Applies transformations and data-quality rules configured in JSON, deduplicates records, and validates references against accepted parent records. It produces separate valid and rejected DataFrames, including rejection reasons.

**Gold — analytical datasets:** Builds three reusable outputs from validated Silver data:

- `gold_policy_portfolio` — policy-level portfolio metrics.
- `gold_claims_summary` — claims aggregation.
- `gold_customer_360` — customer-level policy and claim metrics.

These are logical datasets held in memory during the local run, not persisted database tables.

## Validated sample results

The following results were obtained from the included synthetic sample:

| Layer | Validation | Result |
| --- | --- | ---: |
| Bronze | Sources processed successfully | 3 / 3 |
| Bronze | Input records read | 30 |
| Silver | Records after deduplication | 28 |
| Silver | Valid records | 23 |
| Silver | Rejected records | 5 |
| Gold | Automated validation checks passed | 8 / 8 |

Silver valid records comprise 8 customers, 8 policies, and 7 claims. Rejection-reason counts can exceed rejected-record counts because a record may violate more than one rule.

Gold validations include record-count checks and reconciliations of policy premiums, estimated claim amounts, paid claim amounts, and Customer 360 aggregates. Sample financial amounts are fictional and do not represent real insurance data.

## Running locally

**Prerequisites:** Python, Java, and PySpark installed and configured so a local Spark session can start. Jupyter Notebook is optional for interactive exploration. The validated development environment used Python 3.12, Java 17, and PySpark 4.2.0 on Windows.

### Full pipeline (recommended)

From the repository root, run:

```bash
python run_pipeline.py
```

The script initializes a local Spark session and executes Bronze ingestion, Silver transformations and quality checks, and Gold analytics in sequence using the reusable modules in `src/`. It reports dataset counts, rejection totals, financial reconciliation checks, and overall execution status. It then stops the Spark session. No notebook server or external scheduler is required.

**Validated sample run:** 30 Bronze rows; 23 valid and 5 rejected Silver rows; all 8 Gold reconciliation checks passed. Execution time varies by machine.

### Individual notebooks (optional)

To explore each layer interactively, start Jupyter Notebook from the repository root:

```bash
python -m notebook
```

Open `notebooks/01_bronze_ingestion.ipynb`, `notebooks/02_silver_transformation.ipynb`, or `notebooks/03_gold_analytics.ipynb`. Each notebook uses the reusable modules in `src/` and can initialize its own processing flow; `%run` chaining is not required.

**Local storage limitation:** This demo intentionally does not attempt local Parquet or Delta writes. On the validated Windows machine, local Spark file output was blocked by missing Hadoop Windows utilities (`winutils.exe`). Rather than requiring system-level changes, the project demonstrates processing and checks in memory. Warnings about missing Windows Hadoop utilities may appear even when the in-memory run succeeds.

## Metadata-driven configuration

The `config/` directory separates source definitions, transformation rules, and data-quality checks from the processing code. This makes the sample easier to extend with additional sources or rules without rewriting the notebook workflow. New source schemas and dependencies still require appropriate configuration and testing.

## Current scope and next steps

**Implemented and validated:** Local CSV ingestion, metadata-driven processing, modular PySpark transformations, deduplication, quality checks, referential integrity, rejection reporting, Gold aggregations, and automated sample reconciliations.

**Potential future work:** Persistent Delta tables in Databricks, cloud orchestration and storage, automated test suites and CI, incremental ingestion, monitoring, and a reporting layer.

See [`docs/architecture.md`](docs/architecture.md) for additional architecture details.
