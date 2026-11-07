"""LOAD 1: landing files -> staging (Silver).

For each table, parents first:
  1. check the file against the manifest (checksum, row count): a damaged or partial file
     stops the run before anything is written
  2. bulk COPY into land.<table> (the delta) and land.<table>_Keys (all current CRM keys,
     with Pending = held in the backlog by the batch cap)
  3. row-level data-quality rules: rejected rows go to the dead letter (quality.py)
  4. upsert the remaining rows into stg.<table>; a row is only written when it really
     changed and is not older than what staging already holds (protects against replaying
     an old landing folder)
Then, children first, delete staging rows whose key is no longer in the CRM.
"""
import hashlib
import json
import logging

from psycopg import sql

from . import quality
from .config import SOURCES
from .extract import landing_path

log = logging.getLogger("etl")


class LandingIntegrityError(Exception):
    pass


def manifest(ctx) -> dict:
    path = landing_path(ctx.cfg, ctx.run_id) / "manifest.json"
    if not path.exists():
        raise LandingIntegrityError(f"no manifest for run {ctx.run_id} at {path} (did the extract step finish?)")
    return json.loads(path.read_text())


def _check_file(path, sha):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    if h.hexdigest() != sha:
        raise LandingIntegrityError(f"checksum mismatch for {path.name}: file changed or truncated after extract")


def _copy_in(conn, table: sql.Composable, columns, path):
    cols = sql.SQL(", ").join(map(sql.Identifier, columns))
    with conn.cursor() as cur, open(path, "rb") as f:
        with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER)").format(table, cols)) as cp:
            while chunk := f.read(1 << 20):
                cp.write(chunk)


def load_keys(ctx, conn, src):
    m = manifest(ctx)["tables"][src.table]
    path = landing_path(ctx.cfg, ctx.run_id) / m["key_file"]
    _check_file(path, m["key_sha256"])
    keys = sql.Identifier("land", src.table + "_Keys")
    conn.execute(sql.SQL("TRUNCATE {}").format(keys))
    _copy_in(conn, keys, ["Key", "Pending"], path)


def run(ctx, conn) -> dict:
    man = manifest(ctx)
    folder = landing_path(ctx.cfg, ctx.run_id)
    for src in SOURCES:                                   # verify every file before writing anything
        m = man["tables"][src.table]
        _check_file(folder / m["file"], m["sha256"])
        _check_file(folder / m["key_file"], m["key_sha256"])

    per_table, total_in, total_out = {}, 0, 0
    for src in SOURCES:
        m = man["tables"][src.table]
        land, stg = sql.Identifier("land", src.table), sql.Identifier("stg", src.table)
        conn.execute(sql.SQL("TRUNCATE {}").format(land))
        _copy_in(conn, land, src.columns, folder / m["file"])
        landed = conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(land)).fetchone()["n"]
        if landed != m["rows"]:
            raise LandingIntegrityError(f"{src.table}: manifest says {m['rows']} rows, file holds {landed}")
        load_keys(ctx, conn, src)

        rejected = quality.apply_row_rules(conn, ctx.run_id, src)
        n_rejected = sum(n for rule, n in rejected.items()
                         if conn.execute('SELECT "Severity" FROM etl."DqRule" WHERE "RuleName"=%s',
                                         (rule,)).fetchone()["Severity"] == "reject")

        cols = [sql.Identifier(c) for c in src.columns]
        non_key = [sql.Identifier(c) for c in src.columns if c != src.key]
        upsert = sql.SQL("""
            WITH up AS (
              INSERT INTO {stg} AS s ({cols}, "_RunID")
              SELECT {cols}, %(run)s FROM {land}
              ON CONFLICT ({key}) DO UPDATE SET {sets}, "_RunID" = EXCLUDED."_RunID"
                WHERE ({s_cols}) IS DISTINCT FROM ({e_cols}) AND EXCLUDED."UpdatedAt" >= s."UpdatedAt"
              RETURNING (xmax = 0) AS ins)
            SELECT count(*) FILTER (WHERE ins) AS inserted, count(*) FILTER (WHERE NOT ins) AS updated FROM up""").format(
            stg=stg, land=land, key=sql.Identifier(src.key),
            cols=sql.SQL(", ").join(cols),
            sets=sql.SQL(", ").join(sql.SQL("{c} = EXCLUDED.{c}").format(c=c) for c in non_key),
            s_cols=sql.SQL(", ").join(sql.SQL("s.{}").format(c) for c in non_key),
            e_cols=sql.SQL(", ").join(sql.SQL("EXCLUDED.{}").format(c) for c in non_key))
        r = conn.execute(upsert, {"run": ctx.run_id}).fetchone()
        # dead-letter rows whose key has now been loaded in a corrected version are resolved
        conn.execute(sql.SQL('UPDATE etl."RejectedRow" d SET "ResolvedRunID" = %s WHERE d."SourceTable" = %s '
                             'AND d."ResolvedRunID" IS NULL AND d."RunID" < %s '
                             'AND EXISTS (SELECT 1 FROM {land} l WHERE l.{key} = d."SourceKey")').format(
            land=land, key=sql.Identifier(src.key)), (ctx.run_id, src.table, ctx.run_id))
        per_table[src.table] = {"extracted": landed, "rejected": n_rejected, "inserted": r["inserted"],
                                "updated": r["updated"], "unchanged": landed - n_rejected - r["inserted"] - r["updated"],
                                "deleted": 0, "flags": rejected}
        total_in += landed
        total_out += r["inserted"] + r["updated"]

    for src in reversed(SOURCES):                          # deletes: children first
        stg, keys = sql.Identifier("stg", src.table), sql.Identifier("land", src.table + "_Keys")
        n = conn.execute(sql.SQL('DELETE FROM {stg} s WHERE NOT EXISTS (SELECT 1 FROM {keys} k WHERE k."Key" = s.{key})')
                         .format(stg=stg, keys=keys, key=sql.Identifier(src.key))).rowcount
        conn.execute(sql.SQL('UPDATE etl."RejectedRow" d SET "ResolvedRunID" = %s WHERE d."SourceTable" = %s '
                             'AND d."ResolvedRunID" IS NULL AND NOT EXISTS (SELECT 1 FROM {keys} k WHERE k."Key" = d."SourceKey")')
                     .format(keys=keys), (ctx.run_id, src.table))
        per_table[src.table]["deleted"] = n
        total_out += n
    log.info("stage: %d landed rows, %d written to staging", total_in, total_out)
    return {"rows_in": total_in, "rows_out": total_out, "tables": per_table}
