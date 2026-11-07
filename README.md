# Aisle & Co. ETL pipeline (MADT 8102 capstone, Chapter 6)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/hhej/aisleco-etl-pipeline/blob/main/AisleCo_ETL_Pipeline_Demo_Colab.ipynb)

ETL pipeline from the wedding organizer's CRM (PostgreSQL, OLTP v5) to its data warehouse (DW v7), built for
two business problems: seasonal demand and capacity (Problem 1) and high inquiry, low conversion (Problem 2).
All data in this repository is synthetic: fictional couples and a simulated business year.

## Run it in Google Colab

Open the badge above and choose **Runtime → Run all**. The first two cells clone this repository and install
PostgreSQL 16 inside the Colab session (about 3 minutes); the rest runs in about one minute. The notebook
shows the architecture and schema, both business problems followed run by run, data-quality and error
handling, recovery, the source-to-target check, performance and the batch-cap queue, and all automated tests.

## What is here

| Path | Contents |
|---|---|
| `AisleCo_ETL_Pipeline_Demo_Colab.ipynb` | The demo for Colab (no saved outputs; run it) |
| `AisleCo_ETL_Pipeline_Demo.ipynb` | The same demo for a local run, with its outputs saved |
| `etl/` | The pipeline: extract, stage, data quality, load, runner, command line |
| `sql/` | Schemas (OLTP v5, DW v7), transformations, KPI views |
| `tests/` | 33 pytest tests |
| `scripts/` | Walkthrough loader, simulated-data loader, cross-check, benchmark and stress test, notebook builder |
| `airflow/dags/` | The same steps as an Airflow DAG |
| `docs/` | Section 6 (ETL design), diagrams, stress-test results |
| `data/sim/zones_v3/` | The simulated business year used by the source-to-target check |

`README_current_files.md` describes every file and the local setup (macOS, PostgreSQL 16, Airflow).
