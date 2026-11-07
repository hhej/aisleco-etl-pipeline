"""Aisle & Co. daily ETL: CRM (OLTP v5) -> warehouse (DW v7).

Each task is one pipeline step (etl/pipeline.py), run with the project's own Python, so
Airflow only orchestrates: schedule, dependencies, retries, logs and alerts. The step
code, checkpoints and run log are the same as on the command line.

Trigger with a config to replay a date or rebuild:  {"asof": "2027-02-01", "full_refresh": false}
"""
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ROOT = os.environ.get("AISLECO_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
PY = os.path.join(ROOT, ".venv", "bin", "python")
ENV = {k: v for k, v in os.environ.items() if k.startswith("ETL_")}

# the same graph as etl.pipeline.DEPENDS (kept here so the DAG file has no project imports)
DEPENDS = {
    "extract": [], "stage": ["extract"], "dq_gate": ["stage"],
    "dim_date": ["dq_gate"], "dim_package": ["dq_gate"], "dim_employee": ["dq_gate"],
    "fact_inquiry": ["dim_date", "dim_package", "dim_employee"],
    "fact_event_booking": ["fact_inquiry"],
    "fact_weekly_workload": ["dim_date", "dim_employee"],
    "reconcile": ["fact_event_booking", "fact_weekly_workload"],
    "finish": ["reconcile"],
}
DOCS = {
    "extract": "E: changed rows since the watermark, all tables in parallel from one CRM snapshot -> landing CSV + manifest",
    "stage": "L1: checksum check, COPY into land.*, row-level DQ rules (dead letter), upsert into stg.*, deletes",
    "dq_gate": "Set-level DQ rules and reject-rate limit; a blocking failure stops the run before dims/facts",
    "dim_date": "T+L: DimDate with auspicious flags (upsert)", "dim_package": "T+L: DimPackage (upsert)",
    "dim_employee": "T+L: DimEmployee (upsert)",
    "fact_inquiry": "T: build work.FactInquiry; L: merge with change detection",
    "fact_event_booking": "T: build work.FactEventBooking; L: merge with change detection",
    "fact_weekly_workload": "T: build employee x week grid; L: merge from the as-of week, earlier weeks frozen",
    "reconcile": "Source-to-target reconciliation (counts, hours, keys)",
    "finish": "Advance watermarks, close the run",
}


def on_failure(context):
    """Alerting hook. The failed step has already written etl.Alert; here it is surfaced in
    the Airflow log. Add an EmailOperator / Slack webhook here for real notifications."""
    ti = context["task_instance"]
    logging.getLogger("airflow.task").error(
        "ALERT aisleco_daily_etl: task %s failed (try %s), run_id %s. See etl.Alert / etl.StepRun.",
        ti.task_id, ti.try_number, ti.xcom_pull(task_ids="start_run"))


with DAG(
    dag_id="aisleco_daily_etl",
    description="Aisle & Co. CRM -> data warehouse (Problems 1 and 2)",
    schedule="0 2 * * *",                 # 02:00 Bangkok, after the CRM day closes
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,                    # one load at a time (the steps also take a DB lock)
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True,
                  "on_failure_callback": on_failure},
    tags=["aisleco", "etl", "madt8102"],
    doc_md=__doc__,
) as dag:
    start = BashOperator(
        task_id="start_run",
        bash_command=f"{PY} -m etl start --triggered-by airflow "
                     # as-of = the Bangkok date the run is for (Airflow's ds is a UTC date)
                     "--asof {{ dag_run.conf['asof'] if dag_run and dag_run.conf and dag_run.conf.get('asof') "
                     "else data_interval_end.in_timezone('Asia/Bangkok').strftime('%Y-%m-%d') }}"
                     "{{ ' --full-refresh' if dag_run and dag_run.conf and dag_run.conf.get('full_refresh') else '' }}",
        cwd=ROOT, env=ENV, append_env=True, retries=0,
        doc_md="Creates the run in etl.Run and passes its RunID to every step (XCom)")
    tasks = {
        name: BashOperator(
            task_id=name,
            bash_command=f"{PY} -m etl step {name} --run-id {{{{ ti.xcom_pull(task_ids='start_run') }}}}",
            cwd=ROOT, env=ENV, append_env=True, doc_md=DOCS[name])
        for name in DEPENDS
    }
    for name, ups in DEPENDS.items():
        if not ups:
            start >> tasks[name]
        for up in ups:
            tasks[up] >> tasks[name]
