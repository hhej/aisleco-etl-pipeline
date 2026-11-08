"""Read features from the feature store (sql/ml_feature_store.sql). Every feature is computed
in SQL by feature.inquiry_features(ids, cutoffs), point in time, so training and scoring
use the same definition."""
from datetime import date, datetime, time, timedelta

import pandas as pd

CAT = ["Channel", "BudgetRange", "PackageType"]
NUM = ["AgeDays", "HasRequestedDate", "LeadDays", "PeakDate", "ResponseMinutes", "HasChat", "CoupleMsgs",
       "CoupleMsgs24h", "HoursSilent", "Consulted", "Quoted", "QuoteShare"]
# Training examples: each inquiry as it looked 1, 7, 14 and 30 days after it arrived (while still open)
HORIZONS = (1, 7, 14, 30)


def _midnight(d: date) -> datetime:
    return datetime.combine(d, time())


def compute(conn, ids, cutoffs) -> pd.DataFrame:
    rows = conn.execute("SELECT * FROM feature.inquiry_features(%s::int[], %s::timestamp[])",
                        (list(ids), list(cutoffs))).fetchall()
    df = pd.DataFrame(rows, columns=["InquiryID", "AsOf", *CAT, *NUM] if not rows else None)
    for c in NUM:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df


def labels(conn, asof: date) -> pd.DataFrame:
    """Outcomes known on the as-of date (closed before it)."""
    return pd.DataFrame(conn.execute(
        'SELECT * FROM feature."InquiryLabel" WHERE "ClosedAt" < %s', (_midnight(asof),)).fetchall(),
        columns=["InquiryID", "InquiryDate", "Booked", "ClosedAt"])


def training_set(conn, asof: date) -> pd.DataFrame:
    """Point-in-time join: features at each horizon, with the outcome that came later."""
    lab = labels(conn, asof)
    ids, cuts = [], []
    for k in HORIZONS:
        cut = pd.to_datetime(lab.InquiryDate).dt.normalize() + timedelta(days=k)
        keep = cut < pd.to_datetime(lab.ClosedAt)            # still open at that age
        ids += lab.InquiryID[keep].tolist()
        cuts += [c.to_pydatetime() for c in cut[keep]]
    f = compute(conn, ids, cuts)
    return f.merge(lab[["InquiryID", "InquiryDate", "Booked"]], on="InquiryID")


def materialise(conn, asof: date) -> int:
    """Store the features of every inquiry open at the start of the as-of date in the offline
    store (the function drops the closed ones). Re-running a day replaces that day."""
    t = _midnight(asof)
    conn.execute('DELETE FROM feature."InquiryFeatures" WHERE "AsOf" = %s', (t,))
    return conn.execute(
        'INSERT INTO feature."InquiryFeatures" '
        'SELECT f.* FROM (SELECT array_agg("InquiryID") AS ids, count(*)::int AS n FROM stg."Inquiry" '
        '                 WHERE "InquiryDate" < %(t)s) a, '
        '     feature.inquiry_features(coalesce(a.ids, \'{}\'), array_fill(%(t)s::timestamp, ARRAY[a.n])) f',
        {"t": t}).rowcount


def stored(conn, asof: date) -> pd.DataFrame:
    rows = conn.execute('SELECT * FROM feature."InquiryFeatures" WHERE "AsOf" = %s', (_midnight(asof),)).fetchall()
    df = pd.DataFrame(rows, columns=["InquiryID", "AsOf", *CAT, *NUM] if not rows else None)
    for c in NUM:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df
