# Insurance Lakehouse

End-to-end Data Engineering project that demonstrates the design and implementation of a Lakehouse architecture for a fictional insurance company.

## Business Scenario

The company receives data from multiple sources, including policy, customer, claims, and payment systems.

The objective is to build a scalable data platform that centralizes this information, improves data quality, and prepares trusted datasets for analytics and reporting.

## Proposed Architecture

The target cloud architecture is based on:

- Azure Data Factory
- Azure Data Lake Storage
- Azure Databricks
- PySpark
- Delta Lake
- Medallion Architecture (Bronze, Silver, Gold)
- Power BI

The technical implementation in this repository runs locally using synthetic data, PySpark, and Delta Lake, allowing the project to be demonstrated without requiring cloud infrastructure.

## Data Architecture

Source Systems → Ingestion → Bronze → Silver → Gold → Analytics

## Project Status

🚧 Project under development.