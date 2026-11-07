"""Run, step (checkpoint), watermark and alert bookkeeping in the etl schema.
All writes here go through an autocommit connection, so a failure is logged even when
the step's own transaction is rolled back."""
import json
import logging

from psycopg.types.json import Jsonb

log = logging.getLogger("etl")


def start_run(conn, asof, mode, triggered_by) -> int:
    row = conn.execute(
        'INSERT INTO etl."Run"("AsOfDate","Mode","TriggeredBy","Status","StartedAt") '
        "VALUES (%s,%s,%s,'running',clock_timestamp()) RETURNING \"RunID\"", (asof, mode, triggered_by)).fetchone()
    return row["RunID"]


def get_run(conn, run_id) -> dict:
    row = conn.execute('SELECT * FROM etl."Run" WHERE "RunID"=%s', (run_id,)).fetchone()
    if row is None:
        raise ValueError(f"run {run_id} does not exist")
    return row


def latest_run_id(conn):
    row = conn.execute('SELECT max("RunID") AS r FROM etl."Run"').fetchone()
    return row["r"]


def set_run_status(conn, run_id, status, message=None):
    conn.execute('UPDATE etl."Run" SET "Status"=%s, "Message"=%s, '
                 '"FinishedAt"=CASE WHEN %s::text IN (\'success\',\'failed\') THEN clock_timestamp() END WHERE "RunID"=%s',
                 (status, message, status, run_id))


def step_done(conn, run_id, step) -> bool:
    return conn.execute('SELECT 1 FROM etl."StepRun" WHERE "RunID"=%s AND "Step"=%s AND "Status"=\'success\'',
                        (run_id, step)).fetchone() is not None


def step_start(conn, run_id, step) -> int:
    row = conn.execute(
        'INSERT INTO etl."StepRun"("RunID","Step","Attempt","Status","StartedAt") '
        'SELECT %s, %s, coalesce(max("Attempt"),0)+1, \'running\', clock_timestamp() FROM etl."StepRun" '
        'WHERE "RunID"=%s AND "Step"=%s RETURNING "StepRunID"', (run_id, step, run_id, step)).fetchone()
    return row["StepRunID"]


def step_end(conn, step_run_id, status, metrics=None, error=None):
    metrics = metrics or {}
    conn.execute(
        'UPDATE etl."StepRun" SET "Status"=%s, "FinishedAt"=clock_timestamp(), '
        '"DurationSec"=round(extract(epoch FROM clock_timestamp()-"StartedAt")::numeric,3), '
        '"RowsIn"=%s, "RowsOut"=%s, "Metrics"=%s, "Error"=%s WHERE "StepRunID"=%s',
        (status, metrics.get("rows_in"), metrics.get("rows_out"), Jsonb(json.loads(json.dumps(metrics, default=str))),
         error, step_run_id))


def alert(conn, run_id, severity, message):
    """Alerting: stored in etl.Alert (shown in the demo / DBeaver) and written to the log.
    In Airflow the failure callback does the same, so a failed task is visible in the UI."""
    conn.execute('INSERT INTO etl."Alert"("RunID","Severity","Message","CreatedAt") VALUES (%s,%s,%s,clock_timestamp())',
                 (run_id, severity, message))
    (log.error if severity in ("error", "critical") else log.warning)("ALERT [%s] run %s: %s", severity, run_id, message)


def watermarks(conn) -> dict:
    """{table: (LastUpdatedAt, ExtractedAt)}"""
    return {r["SourceTable"]: (r["LastUpdatedAt"], r["ExtractedAt"])
            for r in conn.execute('SELECT "SourceTable","LastUpdatedAt","ExtractedAt" FROM etl."Watermark"').fetchall()}
