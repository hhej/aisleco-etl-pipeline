"""Aisle & Co. weekly training (continuous training, MLOps level 1).

Every Sunday night: build the point-in-time training set from the feature store, train the
candidates, validate them against the baseline and the production model, and register the
result in ml.ModelVersion. Only a model that passes the gate replaces production; the daily
DAG scores with whatever is in production. Trigger it by hand after a drift alert.

Trigger with a config to train as of a past date:  {"asof": "2026-06-01"}
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ.get("AISLECO_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
PY = os.path.join(ROOT, ".venv", "bin", "python")
ENV = {k: v for k, v in os.environ.items() if k.startswith("ETL_")}

with DAG(
    dag_id="aisleco_weekly_train",
    description="Aisle & Co. conversion model: train, validate, register",
    schedule="0 4 * * 0",                 # Sunday 04:00 Bangkok, after the daily load
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["aisleco", "ml", "madt8102"],
    doc_md=__doc__,
) as dag:
    BashOperator(
        task_id="train_validate_register",
        bash_command=f"{PY} -m ml train --asof "
                     "{{ dag_run.conf['asof'] if dag_run and dag_run.conf and dag_run.conf.get('asof') "
                     "else data_interval_end.in_timezone('Asia/Bangkok').strftime('%Y-%m-%d') }}",
        cwd=ROOT, env=ENV, append_env=True,
        doc_md="Point-in-time training set -> 2 candidates -> gate (baseline, production) -> ml.ModelVersion")
