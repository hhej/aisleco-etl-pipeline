"""Conversion model: point-in-time features, training gate, scoring, drift. Runs on the
simulated three years (data/sim/zones_v3), loaded once for the module."""
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from etl import db, pipeline
from etl.__main__ import init_dw
from etl.config import Config
from load_snapshot import load
from ml import features as F
from ml import monitor, score, train

ZONES = Path(__file__).resolve().parent.parent / "data" / "sim" / "zones_v3"
pytestmark = pytest.mark.skipif(not (ZONES / "oltp_snapshot").exists(), reason="run the v3 notebook first (README)")


@pytest.fixture(scope="module")
def dw(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setenv("ETL_LANDING_DIR", str(tmp_path_factory.mktemp("landing")))
    cfg = Config()
    load(ZONES, 1, cfg)
    init_dw(cfg, drop=True)
    pipeline.run(cfg, date(2027, 1, 15), "full")
    yield cfg
    mp.undo()


def conn(cfg):
    return db.connect(cfg.dw_dsn, cfg)


def test_features_do_not_see_the_future(dw):
    """Scenario: Features of 50 inquiries as of 1 Jun 2026; then new events dated after that day are added: a couple message for each, and a consultation and a quote for those that had none.
    Expected: Recomputed features as of 1 Jun 2026 are identical: nothing dated after the cutoff leaks in."""
    t = datetime(2026, 6, 1)
    with conn(dw) as c:
        ids = [r["InquiryID"] for r in c.execute('SELECT "InquiryID" FROM stg."Inquiry" WHERE "InquiryDate" < %s '
                                                 'ORDER BY "InquiryDate" DESC LIMIT 50', (t,)).fetchall()]
        before = F.compute(c, ids, [t] * len(ids))
        assert len(before) > 0
        c.execute('UPDATE stg."Inquiry" SET "ConsultationDate" = %s, "QuotedDate" = %s, "QuotedAmount" = 1 WHERE "InquiryID" = ANY(%s) '
                  'AND "ConsultationDate" IS NULL AND "QuotedDate" IS NULL', (t + timedelta(hours=1), t + timedelta(days=2), ids))
        c.execute('INSERT INTO stg."ChatMessage"("ChatMessageID","InquiryID","SentAt","Direction","SenderType") '
                  'SELECT 900000 + "InquiryID", "InquiryID", %s, \'In\', \'Customer\' FROM stg."Inquiry" WHERE "InquiryID" = ANY(%s)',
                  (t + timedelta(minutes=5), ids))
        after = F.compute(c, ids, [t] * len(ids))
        c.rollback()
    key = ["InquiryID"]
    assert before.sort_values(key).reset_index(drop=True).equals(after.sort_values(key).reset_index(drop=True))


def test_closed_inquiries_are_not_features(dw):
    """Scenario: Every training row is an inquiry as it looked 1, 7, 14 or 30 days after arrival.
    Expected: Each row is dated before the inquiry closed, the label is not a feature, and a booked inquiry asked about after its booking returns nothing."""
    with conn(dw) as c:
        ts = F.training_set(c, date(2027, 1, 15))
        lab = F.labels(c, date(2027, 1, 15)).set_index("InquiryID")
        booked = lab[lab.Booked == 1].iloc[0]
        late = F.compute(c, [int(booked.name)], [booked.ClosedAt + timedelta(days=1)])
    assert (ts.AsOf < ts.InquiryID.map(lab.ClosedAt)).all()
    assert set(ts.AgeDays) == set(F.HORIZONS)
    assert "Booked" not in F.CAT + F.NUM
    assert late.empty


def test_training_gate_and_registry(dw, monkeypatch):
    """Scenario: Train as of 1 Oct 2025 twice with the same seed, then once more with a gate the model cannot pass.
    Expected: Same metrics both times and the model beats the baseline; the second replaces the first in production; the impossible gate gives 'rejected' and production is unchanged."""
    with conn(dw) as c:
        c.execute('TRUNCATE ml."InquiryScore", ml."DriftCheck", ml."ModelVersion" RESTART IDENTITY CASCADE')
        r1 = train.train(c, date(2025, 10, 1))
        r2 = train.train(c, date(2025, 10, 1))
        monkeypatch.setattr(train, "MIN_GAIN_OVER_BASELINE", 1.0)
        r3 = train.train(c, date(2025, 10, 1))
        status = {r["Version"]: r["Status"] for r in c.execute('SELECT "Version","Status" FROM ml."ModelVersion"').fetchall()}
        c.commit()
    assert r1["test"] == r2["test"] and r1["test"]["auc"] >= r1["baseline"]["auc"] + 0.05
    assert status == {1: "retired", 2: "production", 3: "rejected"}
    assert r3["status"] == "rejected"


def test_daily_scoring_covers_every_open_inquiry(dw):
    """Scenario: The daily steps on 1 Mar 2026 (features, then scores), run twice.
    Expected: One score per open inquiry, bands High/Medium/Low, and the rerun does not duplicate anything."""
    d = date(2026, 3, 1)
    with conn(dw) as c:
        if not train.production(c):
            train.train(c, date(2025, 10, 1))
        for _ in range(2):
            n_feat = F.materialise(c, d)
            n_scored = score.score(c, d)
        rows = c.execute('SELECT "Band", count(*) AS n FROM ml."InquiryScore" WHERE "AsOfDate" = %s GROUP BY 1', (d,)).fetchall()
        c.commit()
    assert n_feat == n_scored == sum(r["n"] for r in rows) > 0
    assert {r["Band"] for r in rows} <= {"High", "Medium", "Low"}


def test_drift_check_flags_the_2026_campaign(dw):
    """Scenario: A model trained on data to 1 Oct 2025 is monitored in Oct 2025 and in May 2026, after the TikTok campaign changed the channel mix.
    Expected: Channel PSI is ok in Oct 2025 and an alert in May 2026, and the alert is written to etl.Alert."""
    with conn(dw) as c:
        if not train.production(c):
            train.train(c, date(2025, 10, 1))
        before = {n: (v, s) for n, v, s, _ in monitor.monitor(c, date(2025, 10, 1))}
        after = {n: (v, s) for n, v, s, _ in monitor.monitor(c, date(2026, 5, 1))}
        alerts = c.execute('SELECT "Message" FROM etl."Alert" WHERE "Message" LIKE \'%%drift on 2026-05-01%%\'').fetchall()
        c.commit()
    assert before["PSI Channel"][1] == "ok"
    assert after["PSI Channel"][1] == "alert"
    assert alerts
