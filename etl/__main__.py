"""Command line:

  python -m etl init-dw [--drop]                 create the warehouse (DW v7) and seed the DQ rules
  python -m etl run --asof 2027-01-15 [--full-refresh]
  python -m etl resume RUN_ID                    continue a failed run from its checkpoint
  python -m etl start --asof D [--full-refresh]  (Airflow) create a run, print its id
  python -m etl step NAME --run-id N             (Airflow) run one step
  python -m etl status [-n 10]                   last runs and their steps
"""
import argparse
import logging
import sys
from datetime import date

import psycopg
from psycopg import sql

from . import db, pipeline, quality
from .config import SQL_DIR, Config


def init_dw(cfg, drop):
    dbname = psycopg.conninfo.conninfo_to_dict(cfg.dw_dsn)["dbname"]
    with db.connect(cfg.admin_dsn, cfg, autocommit=True, purpose="postgres") as c:
        exists = c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,)).fetchone()
        if exists and drop:
            c.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(dbname)))
            exists = None
        if exists:
            sys.exit(f"database {dbname} already exists (use --drop to recreate it)")
        c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    with db.connect(cfg.dw_dsn, cfg, purpose="warehouse") as c:
        for f in ("WeddingOrganizer_DW_v7_dbdiagram.sql", "dw_v7_functions.sql", "dw_v7_mart_views.sql"):
            db.run_sql_file(c, SQL_DIR / f)
        quality.seed_rules(c)
    print(f"warehouse {dbname} created (DW v7) with {len(quality.RULES)} data-quality rules")


def status(cfg, n):
    with db.connect(cfg.dw_dsn, cfg, autocommit=True) as c:
        for r in c.execute('SELECT * FROM etl."Run" ORDER BY "RunID" DESC LIMIT %s', (n,)).fetchall()[::-1]:
            print(f'run {r["RunID"]:>4}  as of {r["AsOfDate"]}  {r["Mode"]:<11} {r["Status"]:<8} {r["Message"] or ""}')
            for s in c.execute('SELECT "Step","Attempt","Status","DurationSec","RowsIn","RowsOut" FROM etl."StepRun" '
                               'WHERE "RunID"=%s ORDER BY "StepRunID"', (r["RunID"],)).fetchall():
                print(f'        {s["Step"]:<22} try {s["Attempt"]} {s["Status"]:<8} {s["DurationSec"] or 0:>7}s '
                      f'in {s["RowsIn"] if s["RowsIn"] is not None else "-":>6}  out {s["RowsOut"] if s["RowsOut"] is not None else "-":>6}')


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m etl")
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("init-dw"); a.add_argument("--drop", action="store_true")
    for name in ("run", "start"):
        a = sub.add_parser(name)
        a.add_argument("--asof", type=date.fromisoformat, default=date.today())
        a.add_argument("--full-refresh", action="store_true")
        a.add_argument("--triggered-by", default="cli")
    a = sub.add_parser("resume"); a.add_argument("run_id", type=int)
    a = sub.add_parser("step"); a.add_argument("name", choices=list(pipeline.STEPS)); a.add_argument("--run-id", type=int, required=True)
    a = sub.add_parser("status"); a.add_argument("-n", type=int, default=10)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    cfg = Config()
    try:
        if args.cmd == "init-dw":
            init_dw(cfg, args.drop)
        elif args.cmd == "run":
            run_id = pipeline.run(cfg, args.asof, "full" if args.full_refresh else "incremental", args.triggered_by)
            print(f"run {run_id} succeeded")
        elif args.cmd == "start":
            print(pipeline.start(cfg, args.asof, "full" if args.full_refresh else "incremental", args.triggered_by))
        elif args.cmd == "resume":
            print(f"run {pipeline.resume(cfg, args.run_id)} succeeded")
        elif args.cmd == "step":
            pipeline.run_step(cfg, args.run_id, args.name)
        elif args.cmd == "status":
            status(cfg, args.n)
    except Exception as e:  # noqa: BLE001 - the error is already in etl.StepRun / etl.Alert
        logging.getLogger("etl").error("%s: %s", type(e).__name__, e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
