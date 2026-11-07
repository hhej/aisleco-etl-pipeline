"""LOAD 2: work.<fact> (what the fact should contain now) -> fact.<fact>.

Incremental, with change detection: a row is inserted when new, updated only when one of
its values really changed (compared after casting to the target column type), deleted
when its source row is gone, and otherwise left alone. Surrogate keys therefore stay
stable from run to run, and the step reports what it changed.

`scope` limits which target rows may be updated or deleted. The weekly workload fact
uses it to freeze weeks that have ended: those rows are only inserted when missing.
"""
from psycopg import sql


def _columns(conn, schema, table):
    rows = conn.execute("""
        SELECT a.attname AS col, format_type(a.atttypid, a.atttypmod) AS typ
        FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped AND a.attidentity = ''
        ORDER BY a.attnum""", (schema, table)).fetchall()
    return [(r["col"], r["typ"]) for r in rows]


def merge(conn, table, keys, scope=None, cascade=(), frozen_cols=None) -> dict:
    """table: name in both fact and work schemas. keys: natural key columns.
    scope: SQL predicate on alias {a} for rows that may be changed (None = all).
    cascade: (child table, column) pairs to delete from first when a row is deleted.
    frozen_cols: columns whose difference in a frozen (out-of-scope) row counts as a late
    change in the source. Columns that are snapshots by design (e.g. the standard hours that
    applied that week) are left out, so a contract change is not reported as a problem."""
    t, s = sql.Identifier("fact", table), sql.Identifier("work", table)
    cols = _columns(conn, "fact", table)
    data_cols = [(c, ty) for c, ty in cols if c not in keys]
    on = sql.SQL(" AND ").join(sql.SQL("t.{c} = s.{c}").format(c=sql.Identifier(k)) for k in keys)
    changed = sql.SQL("({}) IS DISTINCT FROM ({})").format(
        sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(c)) for c, _ in data_cols),
        sql.SQL(", ").join(sql.SQL("s.{}::{}").format(sql.Identifier(c), sql.SQL(ty)) for c, ty in data_cols))
    in_scope = sql.SQL(scope.format(a="t")) if scope else sql.SQL("true")
    fc = [(c, ty) for c, ty in data_cols if c in (frozen_cols or [c for c, _ in data_cols])]
    frozen_changed = sql.SQL("({}) IS DISTINCT FROM ({})").format(
        sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(c)) for c, _ in fc),
        sql.SQL(", ").join(sql.SQL("s.{}::{}").format(sql.Identifier(c), sql.SQL(ty)) for c, ty in fc))

    counts = conn.execute(sql.SQL("""
        SELECT count(*) FILTER (WHERE t_present AND s_present AND in_scope AND changed)     AS updated,
               count(*) FILTER (WHERE t_present AND s_present AND NOT in_scope AND frozen_changed) AS frozen_changed,
               count(*) FILTER (WHERE t_present AND s_present AND NOT changed)              AS unchanged,
               count(*) FILTER (WHERE s_present AND NOT t_present)                          AS inserted,
               count(*) FILTER (WHERE t_present AND NOT s_present AND in_scope)             AS deleted
        FROM (SELECT t.{k0} IS NOT NULL AS t_present, s.{k0} IS NOT NULL AS s_present,
                     coalesce({in_scope}, false) AS in_scope, coalesce({changed}, false) AS changed,
                     coalesce({frozen_changed}, false) AS frozen_changed
              FROM {t} t FULL JOIN {s} s ON {on}) x""").format(
        k0=sql.Identifier(keys[0]), in_scope=in_scope, changed=changed, frozen_changed=frozen_changed,
        t=t, s=s, on=on)).fetchone()

    gone = sql.SQL("{in_scope} AND NOT EXISTS (SELECT 1 FROM {s} s WHERE {on})").format(in_scope=in_scope, s=s, on=on)
    for child, col in cascade:      # e.g. an event row that points to an inquiry row being removed
        conn.execute(sql.SQL("DELETE FROM {c} WHERE {col} IN (SELECT t.{k} FROM {t} t WHERE {gone})").format(
            c=sql.Identifier("fact", child), col=sql.Identifier(col), k=sql.Identifier(keys[0]), t=t, gone=gone))
    conn.execute(sql.SQL("DELETE FROM {t} t WHERE {gone}").format(t=t, gone=gone))

    all_cols = sql.SQL(", ").join(sql.Identifier(c) for c, _ in cols)
    conn.execute(sql.SQL("""
        MERGE INTO {t} t USING {s} s ON {on}
        WHEN MATCHED AND {in_scope} AND {changed} THEN UPDATE SET {sets}
        WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({vals})""").format(
        t=t, s=s, on=on, in_scope=in_scope, changed=changed, all_cols=all_cols,
        sets=sql.SQL(", ").join(sql.SQL("{c} = s.{c}").format(c=sql.Identifier(c)) for c, _ in data_cols),
        vals=sql.SQL(", ").join(sql.SQL("s.{}").format(sql.Identifier(c)) for c, _ in cols)))
    return dict(counts)
