#!/usr/bin/env bash
# Airflow for this project: metadata in the local PostgreSQL (database "airflow"),
# LocalExecutor (tasks really run in parallel), DAGs from airflow/dags.
#   scripts/airflow.sh standalone        # web UI on http://localhost:8080 (prints the admin password)
#   scripts/airflow.sh dags test aisleco_daily_etl 2027-02-01
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export AISLECO_ROOT="$ROOT"
export AIRFLOW_HOME="$ROOT/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__MP_START_METHOD=fork   # macOS defaults to spawn; LocalExecutor workers need fork
export AISLECO_MP_FORK=1                    # read by aisleco_mp_fork.pth in .venv-airflow (same reason)
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW_DB:-postgresql+psycopg2:///airflow}"
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Bangkok
export AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Asia/Bangkok
export PATH="$ROOT/.venv-airflow/bin:$PATH"
export NO_PROXY="*"            # macOS: avoids a known fork crash in task processes
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export PGGSSENCMODE=disable    # macOS: libpq + GSSAPI crashes in forked workers (SIGSEGV)
exec "$ROOT/.venv-airflow/bin/airflow" "$@"
