"""Load the notebook's simulated CRM (zones_v3/oltp_snapshot/*.csv) into a fresh CRM
database, optionally copied N times with shifted IDs (--scale N) for the throughput test.

  python scripts/load_snapshot.py data/sim/zones_v3 [--scale 10]
"""
import argparse
import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from crm import create_crm  # noqa: E402

from etl import db  # noqa: E402
from etl.config import Config  # noqa: E402

MASTER = ["MarketingChannel", "Package", "EmployeeRole", "AuspiciousDate"]
# copied once per --scale copy; each copy brings its own staff (like another branch), so the
# weekly workload stays realistic instead of 10 people carrying every copy's weddings
TRANSACTION = ["Employee", "CustomerChannelAccount", "Inquiry", "ChatMessage", "Event", "EventEmployeeAssignment"]
SHIFT = {"CustomerID", "CustomerAccountID", "InquiryID", "ChatMessageID", "EventID", "EventEmployeeAssignmentID", "EmployeeID"}
OFFSET = 50_000   # base IDs < 30,000; walk-in customers start at 1,000,000,000, so up to x19999 copies never collide


def _copy(conn, schema, table, df):
    types = {r["column_name"]: r["data_type"] for r in conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
        (schema, table)).fetchall()}
    df = df[[c for c in df.columns if c in types]].copy()
    for c in df.columns:
        if types[c] in ("integer", "smallint", "bigint"):
            df[c] = pd.to_numeric(df[c]).astype("Int64")
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    cols = ", ".join(f'"{c}"' for c in df.columns)
    with conn.cursor().copy(f'COPY {schema}."{table}" ({cols}) FROM STDIN WITH (FORMAT csv, HEADER)') as cp:
        cp.write(buf.getvalue())
    return len(df)


def load(zones, scale=1, cfg=None):
    cfg = cfg or Config()
    snap = Path(zones) / "oltp_snapshot"
    create_crm(cfg)
    frames = {t: pd.read_csv(snap / f"{t}.csv", dtype=str, keep_default_na=False, na_values=[""])
              for t in MASTER + TRANSACTION}
    counts = {}
    with db.connect(cfg.crm_dsn, cfg) as conn:
        for t in MASTER:
            counts[t] = _copy(conn, "master", t, frames[t])
        cust = set()
        for t in ("CustomerChannelAccount", "Inquiry", "Event"):
            cust |= set(pd.to_numeric(frames[t]["CustomerID"]).astype(int))
        for k in range(scale):
            shifted = {}
            for t in TRANSACTION:
                df = frames[t].copy()
                for c in SHIFT & set(df.columns):
                    df[c] = (pd.to_numeric(df[c]) + k * OFFSET).astype("Int64")
                if k and t == "Employee":
                    df["EmployeeCode"] = df["EmployeeCode"] + f"-x{k}"
                if k and t == "ChatMessage":
                    df["PlatformMessageID"] = df["PlatformMessageID"] + f"-x{k}"
                if k and t == "CustomerChannelAccount":
                    df["ExternalUserID"] = df["ExternalUserID"] + f"-x{k}"
                shifted[t] = df
            _copy(conn, "master", "Customer", pd.DataFrame({"CustomerID": [c + k * OFFSET for c in sorted(cust)]}))
            for t in TRANSACTION:
                counts[t] = counts.get(t, 0) + _copy(conn, "master" if t in ("CustomerChannelAccount", "Employee") else "transaction",
                                                     t, shifted[t])
    return counts


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("zones")
    p.add_argument("--scale", type=int, default=1)
    a = p.parse_args()
    print(load(a.zones, a.scale))
