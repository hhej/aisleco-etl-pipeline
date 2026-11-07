"""EXTRACT: CRM (OLTP v5) -> landing files (Bronze).

* Incremental: only rows with "UpdatedAt" later than the table's watermark. To catch rows
  whose transaction committed late, the read starts up to 5 minutes (overlap) before the
  previous run's snapshot time, never later than the watermark. Full refresh: every row.
* Every run also extracts the table's primary keys, which is cheap and is the only way to
  notice rows that were deleted in the CRM (a timestamp cannot show a deleted row).
* Parallel: one worker per table, all reading the SAME database snapshot
  (pg_export_snapshot), so a child row never arrives without the parent version it points
  to, even though the tables are read at the same time by different connections.
* Bulk: COPY ... TO STDOUT straight into CSV files; the source is read once, by index.
* A manifest (row counts, checksums, highest UpdatedAt) is written last; the stage step
  refuses files that do not match it.
* Batch cap (incremental runs, ETL_MAX_ROWS_PER_RUN): when more transaction rows changed
  than the cap, only the oldest changes up to a cutoff time T are taken. Later changes stay
  in the CRM behind the watermark (the backlog queue) for the next run. A taken row pulls
  its parent rows into the same batch even if they changed after T, so a child never
  arrives without its parent. The watermark of a capped table becomes T, which is always
  past the old watermark, so every run makes progress. The key file marks held-back keys
  as Pending so reconciliation does not count them as missing.
"""
import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from psycopg import IsolationLevel, sql

from . import db, meta
from .config import SOURCE_BY_TABLE, SOURCES

log = logging.getLogger("etl")


def landing_path(cfg, run_id):
    return cfg.landing_dir / f"run_{run_id:05d}"


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _capped(src):
    return src.schema == "transaction"     # master tables are small and always taken in full


def _changed(alias, since):
    return (sql.SQL('{}."UpdatedAt" > {}').format(sql.Identifier(alias), sql.Literal(since))
            if since is not None else sql.SQL("true"))


def read_from(wm, extracted_at, overlap):
    """Lower bound of the incremental read. Late commits can only hide rows stamped at most
    `overlap` before the previous snapshot, so the overlap reaches back from that snapshot,
    never from the watermark: a run draining a backlog then re-reads nothing."""
    if wm is None:
        return None
    return wm - overlap if extracted_at is None else min(wm, extracted_at - overlap)


def _taken(src, alias, cutoff, sinces, wms, depth=0):
    """Predicate: this changed row is in the batch: it changed up to the cutoff, it is a
    re-read at or below the table's own watermark (overlap; not counted in the cap), or a
    taken child row in a capped table points to it (the parent is pulled forward).
    Uncorrelated IN lists, so each child table is scanned once (hashed), not once per row:
    the CRM has no index on the child foreign keys."""
    a = sql.Identifier(alias)
    pulls = [sql.SQL('{}."UpdatedAt" <= {}').format(a, sql.Literal(max(cutoff, wms.get(src.table) or cutoff)))]
    for child in SOURCES:
        for col, parent in child.parents:
            if parent == src.table and _capped(child):
                c = f"c{depth}_{child.table}"
                pulls.append(sql.SQL("{a}.{k} IN (SELECT {c}.{col} FROM {t} {c} WHERE {c}.{col} IS NOT NULL AND {d})").format(
                    t=sql.Identifier(child.schema, child.table), c=sql.Identifier(c), col=sql.Identifier(col),
                    a=a, k=sql.Identifier(src.key), d=_taken(child, c, cutoff, sinces, wms, depth + 1)))
    return sql.SQL("({} AND ({}))").format(_changed(alias, sinces[src.table]), sql.SQL(" OR ").join(pulls))


def _cutoff(cur, wms, cap):
    """UpdatedAt of the cap-th oldest change across the capped tables, or None if within the cap.
    Only rows newer than the watermark count: overlap re-reads are free, so the cutoff is always
    past the watermark and every run makes progress through the backlog."""
    parts = [sql.SQL('SELECT "UpdatedAt" AS u FROM {} {}').format(
                 sql.Identifier(s.schema, s.table),
                 sql.SQL('WHERE "UpdatedAt" > {}').format(sql.Literal(wms[s.table])) if wms.get(s.table) else sql.SQL(""))
             for s in SOURCES if _capped(s)]
    rows = cur.execute(sql.SQL("SELECT u FROM ({}) x ORDER BY u LIMIT 2 OFFSET {}").format(
        sql.SQL(" UNION ALL ").join(parts), sql.Literal(cap - 1))).fetchall()
    return rows[0]["u"] if len(rows) == 2 else None


def _extract_table(cfg, src, sinces, wms, snapshot, out_dir, cutoff=None):
    conn = db.connect(cfg.crm_dsn, cfg, purpose=f"CRM ({src.table})")
    conn.isolation_level = IsolationLevel.REPEATABLE_READ
    conn.read_only = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot)))
            table = sql.Identifier(src.schema, src.table)
            since, capped = sinces[src.table], cutoff is not None and _capped(src)
            changed = _changed("r", since)
            batch = _taken(src, "r", cutoff, sinces, wms) if capped else changed
            held = sql.SQL("{} AND NOT {}").format(changed, batch) if capped else sql.SQL("false")   # backlog
            where = sql.SQL("WHERE {}").format(batch)
            table = sql.SQL("{} r").format(table)
            cols = sql.SQL(", ").join(sql.SQL("r.{}").format(sql.Identifier(c)) for c in src.columns)
            stats = cur.execute(sql.SQL('SELECT count(*) AS n, max(r."UpdatedAt") AS hi FROM {} {}').format(table, where)).fetchone()
            backlog = (cur.execute(sql.SQL("SELECT count(*) AS n FROM {} WHERE {}").format(table, held)).fetchone()["n"]
                       if capped else 0)
            # capped: every row up to the cutoff was taken, so the watermark moves exactly there
            # (pulled-forward parents are newer and are simply read again next run)
            hi = cutoff if capped else stats["hi"]
            data_file, key_file = out_dir / f"{src.table}.csv", out_dir / f"{src.table}_keys.csv"
            for target, query in (
                (data_file, sql.SQL("COPY (SELECT {} FROM {} {} ORDER BY r.{}) TO STDOUT WITH (FORMAT csv, HEADER)")
                 .format(cols, table, where, sql.Identifier(src.key))),
                (key_file, sql.SQL('COPY (SELECT r.{}, ({}) AS "Pending" FROM {} ORDER BY 1) TO STDOUT WITH (FORMAT csv, HEADER)')
                 .format(sql.Identifier(src.key), held, table)),
            ):
                with open(target, "wb") as f, cur.copy(query) as cp:
                    for chunk in cp:
                        f.write(chunk)
            n_keys = cur.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(table)).fetchone()["n"]
        conn.rollback()
    finally:
        conn.close()
    return src.table, {
        "since": since.isoformat() if since else None, "rows": stats["n"], "keys": n_keys, "backlog": backlog,
        "max_updated_at": hi.isoformat() if hi else None,
        "file": data_file.name, "sha256": _sha256(data_file),
        "key_file": key_file.name, "key_sha256": _sha256(key_file),
    }


def run(ctx, dw_conn) -> dict:
    cfg = ctx.cfg
    out_dir = landing_path(cfg, ctx.run_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").unlink(missing_ok=True)       # a rerun starts clean
    marks = {} if ctx.mode == "full" else meta.watermarks(dw_conn)
    wms = {t: m[0] for t, m in marks.items()}
    overlap = timedelta(seconds=cfg.overlap_seconds)

    # Coordinator: open one consistent snapshot and share it with the workers
    coord = db.connect(cfg.crm_dsn, cfg, purpose="CRM")
    coord.isolation_level = IsolationLevel.REPEATABLE_READ
    coord.read_only = True
    try:
        snapshot = coord.execute("SELECT pg_export_snapshot() AS s").fetchone()["s"]
        extracted_at = coord.execute("SELECT now() AS t").fetchone()["t"]
        sinces = {s.table: read_from(*marks.get(s.table, (None, None)), overlap) for s in SOURCES}
        cutoff = (_cutoff(coord.cursor(), wms, cfg.max_rows_per_run)
                  if ctx.mode != "full" and cfg.max_rows_per_run > 0 else None)
        with ThreadPoolExecutor(max_workers=cfg.extract_workers) as pool:
            futures = [pool.submit(_extract_table, cfg, src, sinces, wms, snapshot, out_dir, cutoff)
                       for src in SOURCES]
            tables = dict(f.result() for f in futures)
    finally:
        coord.close()

    manifest = {"run_id": ctx.run_id, "mode": ctx.mode, "asof": str(ctx.asof),
                "extracted_at": extracted_at.isoformat(), "cutoff": cutoff.isoformat() if cutoff else None,
                "tables": tables}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    total = sum(t["rows"] for t in tables.values())
    backlog = sum(t["backlog"] for t in tables.values())
    log.info("extract: %d changed rows from %d tables -> %s", total, len(tables), out_dir)
    if backlog:
        meta.alert(dw_conn, ctx.run_id, "warning",
                   f"batch cap {cfg.max_rows_per_run:,}: {backlog:,} changed row(s) after {cutoff} are queued "
                   "in the CRM backlog; the next run continues from there")
    return {"rows_in": total, "rows_out": total, "landing_dir": str(out_dir), "cutoff": manifest["cutoff"],
            "backlog": backlog, "per_table": {k: v["rows"] for k, v in tables.items()},
            "backlog_per_table": {k: v["backlog"] for k, v in tables.items() if v["backlog"]}}
