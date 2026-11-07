"""Database connections with retry and exponential backoff (temporary failures such as
a database restarting or a network blip should not fail the run at the first try)."""
import logging
import time

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger("etl")


def connect(dsn: str, cfg, autocommit: bool = False, purpose: str = "") -> psycopg.Connection:
    last = None
    for attempt in range(1, cfg.connect_retries + 1):
        try:
            return psycopg.connect(dsn, autocommit=autocommit, row_factory=dict_row, connect_timeout=5)
        except psycopg.OperationalError as e:
            last = e
            if attempt == cfg.connect_retries:
                break
            wait = cfg.retry_base_seconds * 2 ** (attempt - 1)
            log.warning("connect %s failed (attempt %d/%d), retrying in %.0fs: %s",
                        purpose or dsn, attempt, cfg.connect_retries, wait, str(e).strip().splitlines()[0])
            time.sleep(wait)
    raise ConnectionError(f"could not connect to {purpose or dsn} after {cfg.connect_retries} attempts: {last}")


def run_sql_file(conn, path) -> dict:
    """Execute a multi-statement SQL file; return the last row of the last result set."""
    with conn.cursor() as cur:
        cur.execute(open(path, encoding="utf-8").read())
        last = None
        while True:
            if cur.description is not None:
                rows = cur.fetchall()
                if rows:
                    last = dict(rows[-1])
            if not cur.nextset():
                break
    return last or {}
