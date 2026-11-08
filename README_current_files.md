# Aisle & Co. capstone: which file is current

Scope from Revision 10: **Problem 1, Seasonal demand and capacity** and **Problem 2, High inquiry, low conversion**.
Revision 11 (November 2026) adds the ETL pipeline, written up in `docs/Section6_ETL_Design.md`.
Revision 12 adds the conversion model (feature store, MLOps) and the dashboard, written up in `docs/Section7_ML_Design.md`.

## Current

| File | What it is |
|---|---|
| `Wedding_Organizer_Capstone_Rev10.docx` | Report, Sections 1–5 + Appendices A–B. Paste Section 6 and the new Appendix A row from `docs/Section6_ETL_Design.md` for Rev 11. |
| `sql/WeddingOrganizer_OLTP_v5_dbdiagram.sql` | **OLTP v5**: v4 + `UpdatedAt` on every table. Import into dbdiagram.io as PostgreSQL. |
| `sql/WeddingOrganizer_DW_v7_dbdiagram.sql` | **DW v7**: DW v6 star schema, leaner after review (no stored ratios; inquiry outcome labels in `DimInquiryOutcome`) + `stg` (staging) + `etl` (pipeline control). Import into dbdiagram.io. |
| `sql/oltp_v5_cdc_support.sql`, `sql/dw_v7_functions.sql`, `sql/dw_v7_mart_views.sql` | Trigger + indexes for incremental extraction; helper functions and shared rules; KPI views. Not importable into dbdiagram. |
| `sql/transform/*.sql` | One transformation per dimension / fact |
| `etl/` | The pipeline (Python): extract, stage, quality, load, pipeline runner, CLI |
| `ml/`, `sql/ml_feature_store.sql` | Conversion model: point-in-time feature store (`feature`), training with a validation gate, registry, daily scoring and drift monitoring (`ml`); CLI `python -m ml` |
| `dashboard/app.py` | Streamlit dashboard on the `mart` views: Problem 1, Problem 2, follow-up list, model health |
| `airflow/dags/aisleco_daily_etl.py`, `aisleco_weekly_train.py` | The ETL steps + the model's daily steps as an Airflow DAG; weekly training |
| `AisleCo_ML_Conversion_Demo.ipynb` (+ `_Colab`) | **The Chapter 7 demo**: feature store, training and gate, registry, 15 months of weekly scoring, drift and retraining. Built with `scripts/build_ml_notebook.py [--colab]` |
| `AisleCo_ETL_Pipeline_Demo.ipynb` | **The demo to present**, with outputs saved: architecture and schema, Problem 1 (staffing) and Problem 2 (conversion) followed run by run through Appendix B, dead letter, quality gate, source → target for one inquiry, crash + resume, extract failure and the E/T/L error matrix, idempotency, source-to-target, stress-test results and a live batch-cap (backlog queue) demo, latency vs the 2-minute target, tests |
| `AisleCo_ETL_Pipeline_Demo_Colab.ipynb` | The same demo for Google Colab (GitHub `hhej/aisleco-etl-pipeline`): clones the repo and installs PostgreSQL 16 in the session. Built with `scripts/build_demo_notebook.py --colab`; shipped without outputs |
| `docs/Section6_ETL_Design.md` | Report text: Section 6, the Appendix A rows for Rev 11 and 12, a correction to Appendix B.5 |
| `docs/Section7_ML_Design.md` | Report text: Section 7, the conversion model and MLOps |
| `docs/*.png`, `docs/stress_test.txt` | Architecture (five zones) and DW schema diagrams (`etl_architecture.png`, `dw_schema.png`), the DAG, the Valentine's-week chart, stress-test results (`stress_test.png`), dashboard screenshots (`dashboard_*.png`) |
| `tests/` | 38 pytest tests (rules, Appendix B, incremental = full, idempotency, errors, batch cap, source-to-target, conversion model) |
| `scripts/` | Load the walkthrough or the simulated CRM, cross-check, benchmark, Airflow launcher, notebook builder |
| `AisleCo_Lean_Walkthrough.sql` | Appendix B as SQL; `scripts/load_walkthrough.py` replays it with the ETL runs in between |
| `AisleCo_MultiZone_Architecture_v3.ipynb` | Collection → Bronze → Silver → Gold notebook on simulated data (pandas): three years, 2024–2026, driven by each couple's readiness to book (assumptions in its section 4). Its Gold is the reference for the source-to-target check. |

## Kept as reference (superseded by the pipeline)

| File | Why |
|---|---|
| `WeddingOrganizer_OLTP_v4_lean_dbdiagram.sql`, `WeddingOrganizer_DW_v6_lean_dbdiagram.sql` | Rev 10 schemas; v5 / v7 extend them |
| `WeddingOrganizer_DW_v6_lean_ETL.sql` | The Rev 10 single-script load (same database, truncate + reload). Its rules live on, split per step, in `sql/transform/`. |
| `crosscheck_v3.py` | Rev 10 notebook-vs-SQL check; replaced by `scripts/crosscheck.py` |

## Running it (macOS, local PostgreSQL, no Docker)

```bash
# once
brew install postgresql@16
pg_ctl -D /opt/homebrew/var/postgresql@16 -l /opt/homebrew/var/log/postgresql@16.log start   # (or: brew services start postgresql@16)
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt

# three simulated business years for the source-to-target check and the model (writes data/sim/zones_v3)
mkdir -p data/sim && cp AisleCo_MultiZone_Architecture_v3.ipynb data/sim/ && \
  (cd data/sim && ../../.venv/bin/jupyter nbconvert --to notebook --execute AisleCo_MultiZone_Architecture_v3.ipynb --output executed.ipynb)

# the story, the checks, the demo
.venv/bin/python scripts/load_walkthrough.py      # CRM + DW from scratch, Appendix B with 4 ETL runs
.venv/bin/python -m etl status                    # run log
.venv/bin/python -m pytest tests -q               # 38 tests
ETL_CRM_DSN=dbname=aisleco_crm_bench ETL_DW_DSN=dbname=aisleco_dw_bench ETL_LANDING_DIR=data/landing_bench \
  .venv/bin/python scripts/benchmark.py data/sim/zones_v3 --stress   # offline, about 25 min: writes docs/stress_test.*
.venv/bin/python scripts/build_demo_notebook.py && \
  .venv/bin/jupyter nbconvert --to notebook --execute --inplace AisleCo_ETL_Pipeline_Demo.ipynb
.venv/bin/python scripts/build_ml_notebook.py && \
  .venv/bin/jupyter nbconvert --to notebook --execute --inplace AisleCo_ML_Conversion_Demo.ipynb   # fills aisleco_dw_ml
ETL_DW_DSN=dbname=aisleco_dw_ml .venv/bin/streamlit run dashboard/app.py                          # http://localhost:8501
```

Pipeline commands: `python -m etl run --asof 2027-02-01 [--full-refresh]`, `python -m etl resume RUN_ID`,
`python -m etl status`, `python -m etl init-dw --drop`. Settings are environment variables (`ETL_CRM_DSN`,
`ETL_DW_DSN`, `ETL_OVERLAP_SECONDS`, `ETL_MAX_REJECT_RATE`, `ETL_MAX_ROWS_PER_RUN` (batch cap), … see `etl/config.py`).
Model commands: `python -m ml train --asof D`, `python -m ml daily --asof D` (features, score, monitor), `python -m ml status`.

### Airflow

Airflow 2.10.5 lives in its own venv (`.venv-airflow`); its metadata is in the local PostgreSQL database `airflow`.

```bash
python3.11 -m venv .venv-airflow
.venv-airflow/bin/pip install "apache-airflow[postgres]==2.10.5" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt
createdb airflow && scripts/airflow.sh db migrate
scripts/airflow.sh standalone     # http://localhost:8080, user admin, password in airflow/standalone_admin_password.txt
```

The DAG is paused, so the real-calendar daily schedule doesn't run against the 2027 demo data (such a
run is refused anyway: its as-of date would be earlier than the data already loaded). To show it:
unpause `aisleco_daily_etl`, then *Trigger DAG w/ config* with `{"asof": "2027-02-01"}`.

**macOS fixes already applied in `.venv-airflow`** (redo them after reinstalling Airflow):
1. `setproctitle` replaced by a no-op module (`site-packages/setproctitle/__init__.py`). The real one
   segfaulted in gunicorn's forked workers ("Python quit unexpectedly").
2. `site-packages/aisleco_mp_fork.pth` switches multiprocessing to `fork` when `AISLECO_MP_FORK=1`
   (set by `scripts/airflow.sh`), because LocalExecutor workers cannot start with macOS's default `spawn`.
3. `scripts/airflow.sh` also sets `PGGSSENCMODE=disable`, `NO_PROXY=*` and `OBJC_DISABLE_INITIALIZE_FORK_SAFETY`.

### DBeaver

PostgreSQL, host `localhost`, port `5432`, database `aisleco_dw` (or `aisleco_crm`), user = your macOS user,
no password. Open `etl.Run`, `etl.StepRun`, `etl.RejectedRow`, `etl.Alert`, the `mart` views, and the ER diagram of `dim` + `fact`.

### Databases created

`aisleco_crm` / `aisleco_dw` (demo), `aisleco_crm_p2` / `aisleco_dw_p2` (demo section 6, Problem 2 day by day),
`aisleco_crm_sim` / `aisleco_dw_sim` (simulated years), `aisleco_crm_ml` / `aisleco_dw_ml` (ML notebook, dashboard),
`aisleco_crm_bench` / `aisleco_dw_bench` (benchmark and stress test), `aisleco_*_test` (pytest), `airflow`.
