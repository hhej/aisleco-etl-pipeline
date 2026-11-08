"""Batch prediction: score every open inquiry with the production model, from the features
stored for that day (run `features` first). Results go to ml.InquiryScore for the dashboard."""
import logging
import pickle
from datetime import date

import numpy as np

from . import features as F
from .train import production

log = logging.getLogger("ml")


def score(conn, asof: date) -> int:
    prod = production(conn)
    if not prod:
        log.warning("no production model yet: nothing scored (run `python -m ml train` first)")
        return 0
    f = F.stored(conn, asof)
    conn.execute('DELETE FROM ml."InquiryScore" WHERE "AsOfDate" = %s', (asof,))
    if f.empty:
        return 0
    p = pickle.loads(prod["Artifact"]).predict_proba(f[F.CAT + F.NUM])[:, 1]
    th = prod["Thresholds"]
    band = np.where(p >= th["high"], "High", np.where(p >= th["medium"], "Medium", "Low"))
    with conn.cursor() as cur:
        cur.executemany('INSERT INTO ml."InquiryScore" VALUES (%s,%s,%s,%s,%s)',
                        [(int(i), asof, prod["Version"], round(float(s), 4), b) for i, s, b in zip(f.InquiryID, p, band)])
    return len(f)
