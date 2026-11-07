"""End-to-end: Appendix B replayed through the real pipeline (4 incremental runs).
Every number asserted here is a number printed in Appendix B of the capstone."""
from datetime import date

import pytest

import load_walkthrough
from conftest import facts, q
from etl import pipeline


def week(cfg, start):
    rows = q(cfg, '''SELECT split_part(employee,' ',1) AS who, "TotalAssignedHours" AS h, "StandardWeeklyHours" AS std,
                            round("UtilizationRate"*100) AS pct, "OvertimeHours" AS ot
                     FROM mart.v_weekly_workload WHERE week_start=%s AND "TotalAssignedHours">0''', (start,))
    return {r["who"]: (float(r["h"]), float(r["std"]), int(r["pct"]), float(r["ot"])) for r in rows}


@pytest.fixture(scope="module")
def story(tmp_path_factory):
    import os
    os.environ["ETL_LANDING_DIR"] = str(tmp_path_factory.mktemp("landing"))
    from etl.config import Config
    cfg = Config()
    seen = {}
    load_walkthrough.main(cfg, on_run=lambda label, run_id: seen.update({label: week(cfg, "2027-02-08")}))
    return cfg, seen


def test_b4_load_23_nov(story):
    """Scenario: Appendix B parts 0-3 loaded; run 1 as of 23 Nov 2026; week of 8 Feb 2027.
    Expected: Beam 48/44 h = 109 % (4 h overtime), Arm 48/40 h = 120 % (8 h overtime), Som 48 %, Nina 55 % (B.4)."""
    w = story[1]["PART 3"]
    assert w["Beam"] == (48, 44, 109, 4)          # coordinator over standard hours
    assert w["Arm"] == (48, 40, 120, 8)
    assert w["Som"][2] == 48 and w["Nina"][2] == 55


def test_b4_load_14_dec_overload_moved(story):
    """Scenario: Part 4: freelancer Fah is hired and given both 14 Feb weddings; run 2.
    Expected: Beam drops to 24/44 h = 55 %; Fah is now the one overloaded, 24/20 h = 120 %."""
    w = story[1]["PART 4"]
    assert w["Beam"][:3] == (24, 44, 55)
    assert w["Fah"][:3] == (24, 20, 120)          # the freelancer is now the one overloaded


def test_b4_load_15_dec_better_split(story):
    """Scenario: Part 4b: the work is split again; run 3.
    Expected: Beam 82 %, Fah 60 %; Arm still 120 % (a day-rate crew is needed)."""
    w = story[1]["PART 4b"]
    assert w["Beam"][2] == 82 and w["Fah"][2] == 60
    assert w["Arm"][2] == 120                     # still red: day-rate crew needed


def test_b5_contract_change_and_frozen_week(story):
    """Scenario: Part 5: Nina moves from 44 h to 36 h a week on 20 Jan; run 4 as of 1 Feb.
    Expected: Week of 8 Feb: 24/36 h = 67 %. The ended week of 11 Jan stays frozen on 44 h (14 h = 32 %). No late-change alert."""
    cfg, seen = story
    assert seen["PART 5"]["Nina"][:3] == (24, 36, 67)
    jan = week(cfg, "2027-01-11")["Nina"]
    # Frozen: still divided by 44. (Appendix B says "24 h / 44"; the hours that week are
    # 14 = 12 wedding-day hours for Ploy & Ton + 2 x 1 planning hour; the 44 is the point.)
    assert jan[:3] == (14, 44, 32)
    # a contract change is not a late change: frozen weeks keep 44 h by design, no alert
    assert not q(cfg, '''SELECT 1 FROM etl."Alert" WHERE "Message" LIKE '%%late change%%' ''')


def test_b6_inquiry_answers(story):
    """Scenario: The three Appendix B inquiries after all runs.
    Expected: Response times 11 / 745 / 7 min; lost reasons 'No reply' (AutoRule) and 'Date unavailable' (Sales); quote share 0.1267; 19 days to convert; 1 turned-away inquiry for Feb 2027."""
    cfg = story[0]
    f = {r["InquiryID"]: r for r in q(cfg, 'SELECT * FROM mart.v_inquiry')}
    assert (f[1]["ResponseTimeMinutes"], f[1]["ResponseTimeBand"]) == (11, "<1h")
    assert (f[2]["ResponseTimeMinutes"], f[2]["ResponseTimeBand"]) == (745, "1-24h")   # from 21:40, not from the bot
    assert (f[3]["ResponseTimeMinutes"], f[3]["ResponseTimeBand"]) == (7, "<1h")
    assert (f[2]["LostReason"], f[2]["ClosedBy"]) == ("No reply", "AutoRule")
    assert (f[3]["LostReason"], f[3]["ClosedBy"]) == ("Date unavailable", "Sales")
    assert float(f[1]["QuoteShareOfBudget"]) == pytest.approx(0.1267) and f[1]["DaysToConvert"] == 19
    turned = q(cfg, "SELECT * FROM mart.v_turned_away_demand")
    assert [(r["requested_month"], r["inquiries_turned_away"]) for r in turned] == [("2027-02", 1)]


def test_incremental_equals_full_refresh(story, monkeypatch):
    """Scenario: The warehouse after the 4 incremental runs vs one full rebuild from the same final CRM.
    Expected: Identical, except Nina's weeks before 1 Feb, which correctly keep 44 h (a rebuild would use today's 36 h); the hours themselves are identical."""
    cfg = story[0]
    inc = facts(cfg)
    monkeypatch.setenv("ETL_DW_DSN", "dbname=aisleco_dw_test_full")
    from etl.config import Config
    from etl.__main__ import init_dw
    cfg2 = Config()
    init_dw(cfg2, drop=True)
    pipeline.run(cfg2, date(2027, 2, 1), "full")
    full = facts(cfg2)
    for name in ("FactInquiry", "FactEventBooking"):
        assert inc[name].astype(str).equals(full[name].astype(str)), name
    a, b = inc["FactEmployeeWeeklyWorkload"], full["FactEmployeeWeeklyWorkload"]
    assert a.index.equals(b.index)
    diff = (a.astype(str) != b.astype(str)).any(axis=1)
    differing = a[diff]
    assert set(differing.index.get_level_values(0)) == {2}                        # only Nina (EmployeeID 2)
    assert (differing.index.get_level_values(1) < 20270201).all()                # only weeks before the as-of week
    assert (differing["StandardWeeklyHours"] == 44).all() and (b[diff]["StandardWeeklyHours"] == 36).all()
    assert (differing["TotalAssignedHours"] == b[diff]["TotalAssignedHours"]).all()   # hours identical, only the base differs


def test_rerun_is_idempotent(story):
    """Scenario: Run again with the same CRM and the same as-of date.
    Expected: Nothing inserted, updated or deleted in staging or in any fact; the warehouse is unchanged."""
    cfg = story[0]
    before = facts(cfg)
    run_id = pipeline.run(cfg, date(2027, 2, 1))
    after = facts(cfg)
    for name in before:
        assert before[name].astype(str).equals(after[name].astype(str)), name
    steps = {r["Step"]: r["Metrics"] for r in q(cfg, 'SELECT "Step","Metrics" FROM etl."StepRun" WHERE "RunID"=%s', (run_id,))}
    for s in ("fact_inquiry", "fact_event_booking", "fact_weekly_workload"):
        assert steps[s]["inserted"] == steps[s]["updated"] == steps[s]["deleted"] == 0, s
    assert all(t["inserted"] == t["updated"] == t["deleted"] == 0 for t in steps["stage"]["tables"].values())
