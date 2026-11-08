"""Training, evaluation, validation and registration (the automated pipeline of MLOps level 1).

Split by time, never at random: the model is tested on inquiries that arrived after the ones it
learnt from, as it will be used. A candidate is promoted to production only if it beats a
simple baseline clearly and is not worse than the model already in production."""
import logging
import pickle
from datetime import date, timedelta

import numpy as np
import pandas as pd
from psycopg.types.json import Jsonb
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from . import features as F

log = logging.getLogger("ml")

GAP_DAYS, TEST_DAYS = 90, 180     # test = inquiries received 270 to 90 days before the as-of date
MIN_GAIN_OVER_BASELINE = 0.05     # AUC points over the baseline
MAX_LOSS_VS_PRODUCTION = 0.01     # a refit on newer data may be this much lower and still replace it
DRIFT_NUM = ["LeadDays", "ResponseMinutes", "CoupleMsgs24h"]


def candidates(seed=0):
    def prep(scale):
        num = [SimpleImputer(strategy="median", add_indicator=True)] + ([StandardScaler()] if scale else [])
        return ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), F.CAT),
                                  ("num", make_pipeline(*num), F.NUM)])
    return {"logistic regression": make_pipeline(prep(True), LogisticRegression(C=0.5, max_iter=3000)),
            "gradient boosting": make_pipeline(prep(False), HistGradientBoostingClassifier(
                max_depth=3, learning_rate=0.05, max_iter=200, random_state=seed))}


def metrics(y, p, age):
    top = np.argsort(-p)[: max(1, len(p) // 5)]
    out = {"auc": round(roc_auc_score(y, p), 3), "pr_auc": round(average_precision_score(y, p), 3),
           "top20_capture": round(y[top].sum() / max(1, y.sum()), 3), "brier": round(brier_score_loss(y, p), 4),
           "auc_by_day": {}}
    for k in F.HORIZONS:
        m = age == k
        if len(set(y[m])) == 2:
            out["auc_by_day"][str(k)] = round(roc_auc_score(y[m], p[m]), 3)
    return out


def training_stats(df):
    """Distributions at training time (arrivals, day 1), kept for the drift checks."""
    st = {"rows": len(df), "missing": {c: round(float(df[c].isna().mean()), 4) for c in F.NUM}}
    for c in F.CAT:
        st[c] = df[c].value_counts(normalize=True).round(4).to_dict()
    for c in DRIFT_NUM:
        edges = sorted(set(np.nanquantile(df[c], np.linspace(.1, .9, 9)).round(2))) if df[c].notna().any() else []
        st[c] = {"edges": edges, "shares": bin_shares(df[c], edges)}
    return st


def bin_shares(s, edges):
    keys = np.where(s.isna(), "missing", np.digitize(s.fillna(0), edges).astype(str))
    return pd.Series(keys).value_counts(normalize=True).round(4).to_dict()


def production(conn):
    return conn.execute('SELECT * FROM ml."ModelVersion" WHERE "Status" = \'production\'').fetchone()


def train(conn, asof: date, seed=0) -> dict:
    ts = F.training_set(conn, asof)
    d = pd.to_datetime(ts.InquiryDate)
    test_end = pd.Timestamp(asof - timedelta(days=GAP_DAYS))
    test_start = test_end - timedelta(days=TEST_DAYS)
    tr, te = (d < test_start).values, ((d >= test_start) & (d < test_end)).values
    X, y, age = ts[F.CAT + F.NUM], ts.Booked.values, ts.AgeDays.values
    if tr.sum() < 200 or te.sum() < 50 or len(set(y[te])) < 2:
        raise ValueError(f"not enough labelled history before {asof} (train {tr.sum()}, test {te.sum()} rows)")

    results, fitted = {}, {}
    for name, model in candidates(seed).items():
        fitted[name] = model.fit(X[tr], y[tr])
        results[name] = metrics(y[te], model.predict_proba(X[te])[:, 1], age[te])
    rate = ts[tr].groupby("Channel").Booked.mean()        # baseline: each channel's past booking rate
    baseline = metrics(y[te], ts.Channel[te].map(rate).fillna(y[tr].mean()).values, age[te])
    name = max(results, key=lambda n: results[n]["auc"])
    chosen = results[name]

    # Production was refit on everything up to its own as-of date, so compare the two only on
    # test inquiries it never saw; until there are enough of those, the baseline gate decides.
    prod, vs_prod = production(conn), None
    if prod:
        unseen = te & (d >= pd.Timestamp(prod["AsOfDate"])).values
        if unseen.sum() >= 50 and len(set(y[unseen])) == 2:
            vs_prod = {"rows": int(unseen.sum()),
                       "candidate": round(roc_auc_score(y[unseen], fitted[name].predict_proba(X[unseen])[:, 1]), 3),
                       "production": round(roc_auc_score(y[unseen], pickle.loads(prod["Artifact"]).predict_proba(X[unseen])[:, 1]), 3)}
    if chosen["auc"] < baseline["auc"] + MIN_GAIN_OVER_BASELINE:
        status, why = "rejected", f"AUC {chosen['auc']} is not {MIN_GAIN_OVER_BASELINE} above the baseline ({baseline['auc']})"
    elif vs_prod and vs_prod["candidate"] < vs_prod["production"] - MAX_LOSS_VS_PRODUCTION:
        status, why = "rejected", (f"AUC {vs_prod['candidate']} is below production v{prod['Version']} ({vs_prod['production']}) "
                                   f"on {vs_prod['rows']} test rows it never saw")
    else:
        status = "production"
        why = (f"AUC {chosen['auc']} vs baseline {baseline['auc']}"
               + (f"; vs production v{prod['Version']}: {vs_prod['candidate']} vs {vs_prod['production']} on {vs_prod['rows']} unseen rows"
                  if vs_prod else f"; production v{prod['Version']} has no unseen test rows to compare" if prod else "")
               + "; refit on train + test")

    final = candidates(seed)[name].fit(X[tr | te], y[tr | te])      # the production model uses all labelled history
    p_all = final.predict_proba(X[tr | te])[:, 1]
    thresholds = {"high": round(float(np.quantile(p_all, .8)), 4), "medium": round(float(np.quantile(p_all, .5)), 4)}
    window = lambda m: f"{d[m].min():%Y-%m-%d} to {d[m].max():%Y-%m-%d}"
    row = conn.execute(
        'INSERT INTO ml."ModelVersion"("AsOfDate","Algorithm","TrainWindow","TestWindow","TrainRows","TestRows","Metrics",'
        '"Features","TrainingStats","Thresholds","Status","Decision","Artifact") '
        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING "Version"',
        (asof, name, window(tr), window(te), int(tr.sum()), int(te.sum()),
         Jsonb({"chosen": chosen, "candidates": results, "baseline": baseline, "vs_production": vs_prod}),
         Jsonb({"categorical": F.CAT, "numeric": F.NUM}),
         Jsonb(training_stats(ts[(tr | te) & (age == 1)])), Jsonb(thresholds), status, why, pickle.dumps(final))).fetchone()
    if status == "production" and prod:
        conn.execute('UPDATE ml."ModelVersion" SET "Status" = \'retired\' WHERE "Version" = %s', (prod["Version"],))
    log.info("model v%s (%s) %s: %s", row["Version"], name, status, why)
    return {"version": row["Version"], "algorithm": name, "status": status, "decision": why,
            "test": chosen, "baseline": baseline, "candidates": results, "vs_production": vs_prod}
