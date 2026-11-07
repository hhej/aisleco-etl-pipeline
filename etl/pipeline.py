"""The pipeline: an ordered list of single-purpose steps, and the runner that executes them.

Each step runs in its own transaction on the warehouse and is idempotent, so a step that
fails leaves nothing half-written and can simply be run again. etl.StepRun records every
attempt and is the checkpoint: resuming a run skips the steps that already succeeded.
The same step names are the Airflow tasks (airflow/dags/aisleco_daily_etl.py).
"""
import logging
from dataclasses import dataclass
from datetime import date, timedelta

from . import db, extract, load, meta, quality, stage
from .config import SQL_DIR, Config

log = logging.getLogger("etl")
LOCK_ID = 8102          # advisory lock: two steps never write the warehouse at the same time
TRANSFORM = SQL_DIR / "transform"
WEEK_SCOPE = '{a}."WeekStartDateKey" >= etl.dk((SELECT asof_week FROM work.run_ctx))'


@dataclass
class Ctx:
    cfg: Config
    run_id: int
    asof: date
    mode: str           # incremental | full


def _sql_step(filename):
    def fn(ctx, conn):
        r = db.run_sql_file(conn, TRANSFORM / filename)
        return {"rows_out": (r.get("inserted") or 0) + (r.get("updated") or 0), **r}
    return fn


def _fact_step(filename, table, keys, scope=None, cascade=(), frozen_cols=None):
    def fn(ctx, conn):
        built = db.run_sql_file(conn, TRANSFORM / filename)["rows_built"]          # T: build work.<fact>
        r = load.merge(conn, table, keys, scope, cascade, frozen_cols)             # L: merge into fact.<fact>
        if scope:
            # Late changes: the CRM now gives different hours for weeks that have ended. They
            # stay frozen; recorded every run, alerted only when the number goes up.
            prev = conn.execute('''SELECT d."RowsFailed" FROM etl."DqResult" d JOIN etl."DqRule" r USING ("RuleID")
                                    WHERE r."RuleName"='frozen_week_late_change' AND d."RunID" < %s
                                    ORDER BY d."RunID" DESC LIMIT 1''', (ctx.run_id,)).fetchone()
            quality.record(conn, ctx.run_id, "frozen_week_late_change", built, r["frozen_changed"])
            if r["frozen_changed"] > (prev["RowsFailed"] if prev else 0):
                meta.alert(conn, ctx.run_id, "warning",
                           f"{table}: {r['frozen_changed']} row(s) of weeks that have ended no longer match the CRM "
                           "(late change); kept frozen. Run a full rebuild on purpose if they must be corrected")
        return {"rows_in": built, "rows_out": r["inserted"] + r["updated"] + r["deleted"], **r}
    return fn


def _finish(ctx, conn):
    """Advance the watermarks (only now that every step has succeeded) and close the run."""
    if meta.latest_run_id(conn) != ctx.run_id:
        raise RuntimeError(f"run {ctx.run_id} is not the latest run; it cannot move the watermarks")
    tables = stage.manifest(ctx)["tables"]
    for table, m in tables.items():
        if m["max_updated_at"]:
            conn.execute('''INSERT INTO etl."Watermark" AS w ("SourceTable","LastUpdatedAt","LastRunID","ChangedAt")
                            VALUES (%s, %s, %s, clock_timestamp())
                            ON CONFLICT ("SourceTable") DO UPDATE SET
                              "LastUpdatedAt" = greatest(w."LastUpdatedAt", EXCLUDED."LastUpdatedAt"),
                              "LastRunID" = EXCLUDED."LastRunID", "ChangedAt" = EXCLUDED."ChangedAt"''',
                         (table, m["max_updated_at"], ctx.run_id))
    # every table was read from this snapshot, so the late-commit overlap reaches back from here
    conn.execute('UPDATE etl."Watermark" SET "ExtractedAt" = %s::timestamptz::timestamp',
                 (stage.manifest(ctx)["extracted_at"],))
    return {"watermarks_moved": sum(1 for m in tables.values() if m["max_updated_at"])}


# name -> function(ctx, conn) -> metrics. Order = execution order on the CLI.
STEPS = {
    "extract": extract.run,                       # E   CRM -> landing CSV files (Bronze)
    "stage": stage.run,                           # L1  landing -> stg (Silver) + row rules / dead letter
    "dq_gate": quality.gate,                      #     set rules; a blocking failure stops the run here
    "dim_date": _sql_step("10_dim_date.sql"),     # T+L dimensions (upsert, keys stable)
    "dim_package": _sql_step("11_dim_package.sql"),
    "dim_employee": _sql_step("12_dim_employee.sql"),
    "fact_inquiry": _fact_step("20_fact_inquiry.sql", "FactInquiry", ["InquiryID"],
                               cascade=[("FactEventBooking", "InquiryID")]),
    "fact_event_booking": _fact_step("21_fact_event_booking.sql", "FactEventBooking", ["EventID"]),
    "fact_weekly_workload": _fact_step("22_fact_weekly_workload.sql", "FactEmployeeWeeklyWorkload",
                                       ["EmployeeKey", "WeekStartDateKey"], scope=WEEK_SCOPE,
                                       frozen_cols=["PlanningHours", "EventDayHours", "TotalAssignedHours", "ActiveEvents"]),
    "reconcile": quality.reconcile,               #     source-to-target checks
    "finish": _finish,                            #     watermarks forward
}
# Dependencies, used by Airflow to run independent steps in parallel
DEPENDS = {
    "extract": [], "stage": ["extract"], "dq_gate": ["stage"],
    "dim_date": ["dq_gate"], "dim_package": ["dq_gate"], "dim_employee": ["dq_gate"],
    "fact_inquiry": ["dim_date", "dim_package", "dim_employee"],
    "fact_event_booking": ["fact_inquiry"],
    "fact_weekly_workload": ["dim_date", "dim_employee"],
    "reconcile": ["fact_event_booking", "fact_weekly_workload"],
    "finish": ["reconcile"],
}


class AsOfError(ValueError):
    pass


def start(cfg, asof, mode="incremental", triggered_by="cli") -> int:
    with db.connect(cfg.dw_dsn, cfg, autocommit=True, purpose="warehouse") as c:
        # The weekly fact freezes weeks before the as-of week. A run with an earlier as-of date
        # than one already loaded would reopen those weeks and rewrite history, so refuse it.
        last = c.execute('SELECT max("AsOfDate") AS d FROM etl."Run" WHERE "Status"=\'success\'').fetchone()["d"]
        if last and asof < last:
            raise AsOfError(f"as-of {asof} is earlier than the last successful run ({last}); "
                            "weeks that are already frozen would be rewritten")
        run_id = meta.start_run(c, asof, mode, triggered_by)
    log.info("run %d started (as of %s, %s)", run_id, asof, mode)
    return run_id


def run_step(cfg, run_id, name):
    """Run one step of a run in its own transaction; log the attempt either way."""
    with db.connect(cfg.dw_dsn, cfg, autocommit=True, purpose="warehouse (log)") as logc:
        run = meta.get_run(logc, run_id)
        ctx = Ctx(cfg, run_id, run["AsOfDate"], run["Mode"])
        if meta.step_done(logc, run_id, name):
            log.info("step %-22s already done in run %d (checkpoint), skipped", name, run_id)
            return None
        sr = meta.step_start(logc, run_id, name)
        try:
            with db.connect(cfg.dw_dsn, cfg, purpose="warehouse") as conn:
                with conn.transaction():
                    conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_ID,))
                    conn.execute("DELETE FROM work.run_ctx")
                    conn.execute("INSERT INTO work.run_ctx VALUES (%s, %s, %s)",
                                 (run_id, ctx.asof, ctx.asof - timedelta(days=ctx.asof.weekday())))
                    metrics = STEPS[name](ctx, conn) or {}
                    if cfg.fail_at == name:
                        raise RuntimeError(f"injected failure in step {name} (ETL_FAIL_AT)")
                    if name == "finish":
                        meta.set_run_status(conn, run_id, "success")
        except Exception as e:
            meta.step_end(logc, sr, "failed", error=f"{type(e).__name__}: {e}")
            meta.set_run_status(logc, run_id, "failed", f"step {name}: {e}")
            meta.alert(logc, run_id, "error", f"step {name} failed: {e}")
            raise
        meta.step_end(logc, sr, "success", metrics)
        log.info("step %-22s ok  %s", name, {k: v for k, v in metrics.items() if not isinstance(v, dict)})
        return metrics


def run(cfg, asof, mode="incremental", triggered_by="cli") -> int:
    run_id = start(cfg, asof, mode, triggered_by)
    for name in STEPS:
        run_step(cfg, run_id, name)
    return run_id


def resume(cfg, run_id) -> int:
    """Continue a failed run from its last checkpoint (the landed files are reused)."""
    with db.connect(cfg.dw_dsn, cfg, autocommit=True, purpose="warehouse") as c:
        run = meta.get_run(c, run_id)
        if run["Status"] == "success":
            raise ValueError(f"run {run_id} already succeeded")
        if meta.latest_run_id(c) != run_id:
            raise ValueError(f"run {run_id} is not the latest run; start a new run instead")
        meta.set_run_status(c, run_id, "running", f"resumed")
    for name in STEPS:
        run_step(cfg, run_id, name)
    return run_id
