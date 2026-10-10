# Insurance Lakehouse Architecture

## Overview

Insurance Lakehouse is a portfolio project demonstrating a **metadata-driven Medallion pipeline** for a fictional insurance company. The implemented demo runs locally with Python, PySpark, synthetic CSV inputs, JSON configuration, and reusable processing modules.

The **target architecture** uses Azure Data Factory, Azure Data Lake Storage, Azure Databricks, Delta Lake, and Unity Catalog. These cloud services are design targets, **not deployed components** of the current demo.

## End-to-End Data Flow

```text
Synthetic CSV files (data/source/)
           |
           v
JSON metadata (config/bronze_sources.json)
           |
           v
Bronze ingestion (src/bronze.py)
  - source-specific ingestion metadata
  - per-source status, row count, duration
           |
           v
Silver processing (src/silver.py)
  - transformations and type enforcement
  - deterministic deduplication
  - configurable quality checks
  - referential integrity
  - valid and rejected DataFrames
           |
           v
Gold analytics (src/gold.py)
  - policy portfolio
  - claims summary
  - customer 360
           |
           v
Validation and optional notebook previews
```

**All three layers are represented by in-memory PySpark DataFrames.** The current demo does not persist Parquet or Delta tables.

## Local Pipeline Execution

The project includes `run_pipeline.py` at the repository root. It is a **local entry point**, not a production orchestrator. Running `python run_pipeline.py` invokes the reusable Bronze, Silver, and Gold modules in dependency order within a single Spark session, reports source and quality counts, executes eight Gold reconciliation checks, and closes the Spark session.

```text
python run_pipeline.py
         |
         v
  Bronze -> Silver -> Gold -> Reconciliation summary
         (one local Spark execution; in-memory DataFrames)
```

The three notebooks remain available for interactive exploration and can execute independently by invoking the same reusable modules. There is no local scheduler, task retry engine, or persistent handoff between stages. A future production orchestration design would persist datasets between tasks, typically in Delta Lake, before scheduling independent Bronze, Silver, and Gold jobs.

## Repository Components

| Component | Role |
|---|---|
| `data/source/customers.csv` | Synthetic customer input |
| `data/source/policies.csv` | Synthetic policy input |
| `data/source/claims.csv` | Synthetic claim input |
| `config/bronze_sources.json` | Source definitions for generic Bronze ingestion |
| `config/silver_transformations.json` | Silver transformations and schema rules |
| `config/silver_quality_rules.json` | Silver data-quality rules |
| `src/bronze.py` | Reusable Bronze ingestion functions |
| `src/silver.py` | Reusable Silver transformation and validation functions |
| `src/gold.py` | Reusable Gold analytical transformations |
| `run_pipeline.py` | One-command local pipeline execution and reconciliation |
| `notebooks/01_bronze_ingestion.ipynb` | Bronze execution and operational summary |
| `notebooks/02_silver_transformation.ipynb` | Silver execution and quality summary |
| `notebooks/03_gold_analytics.ipynb` | Gold execution and automated reconciliation checks |

## Source and Configuration Layers

### Implemented locally

The demo reads three synthetic CSV datasets. Source definitions live in `config/bronze_sources.json` rather than in separate hardcoded ingestion branches. Silver transformations and quality checks are also driven by JSON configuration.

Adding another CSV source can reuse the generic Bronze ingestion mechanism after supplying a compatible metadata definition. Extending the full Bronze-to-Gold pipeline to a new business entity may also require new Silver rules and Gold transformations.

### Proposed cloud extension

Potential sources include SFTP, APIs, and operational databases. Azure Data Lake Storage can host landing and curated data. Cosmos DB or metadata tables could replace local JSON configuration, subject to adapting the configuration access layer. **These connectors and repositories are not implemented in this repository.**

## Bronze Layer

Bronze reads configured CSV sources into PySpark DataFrames, preserving source fields and adding technical metadata:

- `_ingestion_timestamp`
- `_source_name`
- `_source_file`

The ingestion workflow reports each source's status, record count, and execution duration, as well as overall totals and failures. It does not currently write Bronze data to persistent storage.

## Silver Layer

Silver applies configured transformations, type conversions, deduplication, and data-quality rules. It also checks referential integrity against **valid parent datasets**, so a reference to a rejected parent does not count as valid.

The layer produces separate valid and rejected DataFrames. A rejected row can have **multiple rejection reasons**, so the number of reported reasons can exceed the number of rejected rows. The synthetic data includes duplicates, missing values, invalid amounts or dates, and broken references to exercise these checks.

Deduplication uses a deterministic tie-breaker for the demo; a production solution should define source-specific business precedence and change-data-capture semantics.

## Gold Layer

Gold builds three analytics-oriented DataFrames from validated Silver data:

- `gold_policy_portfolio`: portfolio-level policy metrics.
- `gold_claims_summary`: aggregated claim metrics.
- `gold_customer_360`: customer-centric policy and claim metrics.

The Gold notebook performs eight automated checks covering customer, policy, and claim counts, premium totals, estimated and paid claim amounts, and Customer 360 reconciliation.

## Validated Demo Results

These are the results observed with the included synthetic input files:

| Stage | Result |
|---|---|
| Bronze | 3 successful sources, 30 input rows, 0 failed sources |
| Silver customers | 8 valid, 1 rejected after deduplication |
| Silver policies | 8 valid, 2 rejected |
| Silver claims | 7 valid, 2 rejected after deduplication |
| Silver total | 23 valid, 5 rejected |
| Gold policy portfolio | 4 rows |
| Gold claims summary | 3 rows |
| Gold customer 360 | 8 rows |
| Gold validation | 8 checks passed, 0 failed |

Validated financial totals from the sample data:

| Metric | Amount |
|---|---:|
| Policy premiums | 3,325,001.25 |
| Estimated claim amounts | 4,555,000.00 |
| Paid claim amounts | 2,600,000.00 |

The Bronze total includes raw source rows; Silver totals are calculated **after deduplication**. Gold datasets are analytical views and should not be added to the Silver record count.

## Local Implementation vs. Target Architecture

| Concern | Implemented locally | Proposed Azure / Databricks design |
|---|---|---|
| Sources | Synthetic CSV files | ADLS, SFTP, APIs, operational databases |
| Configuration | Local JSON files | Cosmos DB or metadata tables |
| Processing | Local PySpark | Azure Databricks |
| Bronze / Silver / Gold | In-memory DataFrames | Persisted Delta Lake tables |
| Orchestration | `run_pipeline.py` (manual CLI) or individual notebooks | ADF and/or Databricks Workflows |
| Governance | Repository conventions | Unity Catalog |
| Analytics consumption | Notebook summaries and validations | BI / reporting layer, e.g. Power BI |

## Local Environment and Persistence Limitation

The project intentionally avoids changing global workstation settings or installing additional Hadoop-specific Windows utilities. On the development workstation, local Spark filesystem persistence requires extra Windows/Hadoop configuration. Consequently, the demonstrated pipeline processes data in memory rather than writing Parquet or Delta files.

This limitation does **not** prevent validation of the transformation logic, data-quality rules, modular design, or aggregate reconciliation. Persisted Delta tables, **scheduled/cloud orchestration**, monitoring, and cloud governance remain future implementation work. The local CLI runner is implemented but does not provide scheduling or workflow infrastructure.

## Future Enhancements

1. Run the pipeline on Databricks and persist Bronze, Silver, and Gold as Delta tables.
2. Add orchestration, incremental ingestion, retries, and operational monitoring.
3. Introduce metadata storage outside local JSON files where appropriate.
4. Add automated tests and continuous integration beyond the current notebook checks.
5. Implement cloud access controls, secrets management, and Unity Catalog governance.

See the repository `README.md` for local execution instructions and project status.
