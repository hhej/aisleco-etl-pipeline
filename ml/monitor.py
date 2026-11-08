"""Monitoring (Ch.7, data distribution shift): compare what arrives now with what the
production model was trained on, and check its accuracy once outcomes are known.

- Data drift: PSI of each categorical feature and of binned numeric features, for the
  inquiries of the last 120 days (as they looked on day 1) against the training data. The
  window is longer than the longest gap between wedding fairs (99 days), so the fair calendar
  itself does not look like drift.
  PSI < 0.1 ok, 0.1-0.2 warning, >= 0.2 alert. Missing rates are checked too.
- Performance: AUC of the scores given 30-180 days ago whose outcome is now known,
  against the AUC measured on the test window at training.
Alerts also go to etl.Alert, next to the ETL alerts."""
import logging
from datetime import date, timedelta

import numpy as np
from sklearn.metrics import roc_auc_score

from etl import meta

from . import features as F
from .train import DRIFT_NUM, bin_shares, production

log = logging.getLogger("ml")
WINDOW_DAYS, WARN, ALERT = 120, 0.1, 0.2


def psi(expected: dict, actual: dict) -> float:
    keys = set(expected) | set(actual)
    e = np.array([expected.get(k, 0) for k in keys]) + 1e-4
    a = np.array([actual.get(k, 0) for k in keys]) + 1e-4
    return round(float(((a - e) * np.log(a / e)).sum()), 3)


def level(v, warn, alert):
    return "alert" if v >= alert else "warning" if v >= warn else "ok"


def monitor(conn, asof: date) -> list:
    prod = production(conn)
    if not prod:
        return []
    st, checks = prod["TrainingStats"], []
    rows = conn.execute('SELECT "InquiryID", "InquiryDate"::date + 1 AS cut FROM stg."Inquiry" '
                        'WHERE "InquiryDate" >= %s AND "InquiryDate"::date + 1 <= %s',
                        (asof - timedelta(days=WINDOW_DAYS), asof)).fetchall()
    f = F.compute(conn, [r["InquiryID"] for r in rows], [r["cut"] for r in rows])
    if len(f) < 30:
        checks.append(("new inquiries", len(f), "n/a", f"only {len(f)} inquiries in the last {WINDOW_DAYS} days"))
    else:
        for c in F.CAT:
            v = psi(st[c], f[c].value_counts(normalize=True).to_dict())
            checks.append((f"PSI {c}", v, level(v, WARN, ALERT), f"{len(f)} inquiries of the last {WINDOW_DAYS} days vs training"))
        for c in DRIFT_NUM:
            v = psi(st[c]["shares"], bin_shares(f[c], st[c]["edges"]))
            checks.append((f"PSI {c}", v, level(v, WARN, ALERT), "deciles of the training data"))
        for c in F.NUM:
            gap = round(abs(float(f[c].isna().mean()) - st["missing"][c]), 3)
            if gap >= 0.05:
                checks.append((f"missing {c}", gap, level(gap, 0.05, 0.15), f"missing-rate change vs training ({st['missing'][c]:.0%})"))

    s = conn.execute('SELECT s."Score", l."Booked" FROM ml."InquiryScore" s JOIN feature."InquiryLabel" l USING ("InquiryID") '
                     'WHERE s."AsOfDate" >= %s AND s."AsOfDate" < %s AND l."ClosedAt" < %s',
                     (asof - timedelta(days=180), asof - timedelta(days=30), asof)).fetchall()
    y = np.array([r["Booked"] for r in s])
    test_auc = prod["Metrics"]["chosen"]["auc"]
    if len(s) >= 30 and len(set(y)) == 2:
        live = round(roc_auc_score(y, [float(r["Score"]) for r in s]), 3)
        checks.append(("live AUC", live, level(test_auc - live, 0.05, 0.1), f"{len(s)} scores with a known outcome; test AUC {test_auc}"))
    else:
        checks.append(("live AUC", None, "n/a", f"{len(s)} scores with a known outcome so far"))

    conn.execute('DELETE FROM ml."DriftCheck" WHERE "AsOfDate" = %s AND "Version" = %s', (asof, prod["Version"]))
    with conn.cursor() as cur:
        cur.executemany('INSERT INTO ml."DriftCheck" VALUES (%s,%s,%s,%s,%s,%s)',
                        [(asof, prod["Version"], n, v, stt, det) for n, v, stt, det in checks])
    bad = [f"{n} = {v}" for n, v, stt, _ in checks if stt == "alert"]
    if bad:
        meta.alert(conn, None, "warning", f"model v{prod['Version']} drift on {asof}: " + ", ".join(bad) + "; retrain and review")
    return checks
