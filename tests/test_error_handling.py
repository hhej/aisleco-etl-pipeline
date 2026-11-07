"""Error handling and recovery (MADT8102 Ch.6, slides 24-26 and 30)."""
from datetime import date

import pytest

import load_walkthrough
from conftest import facts, q
from crm import crm_exec
from etl import pipeline
from etl.quality import DataQualityError
from etl.stage import LandingIntegrityError

ASOF = date(2027, 2, 1)


@pytest.fixture
def base(cfg):
    """Fresh CRM + DW with the whole Appendix B story loaded (4 successful runs)."""
    load_walkthrough.main(cfg)
    return cfg


def watermarks(cfg):
    return {r["SourceTable"]: r["LastUpdatedAt"] for r in q(cfg, 'SELECT * FROM etl."Watermark"')}


def last_run(cfg):
    return q(cfg, 'SELECT * FROM etl."Run" ORDER BY "RunID" DESC LIMIT 1')[0]


def test_source_unavailable_retries_then_fails_cleanly(base, monkeypatch):
    """Scenario: E fails: the CRM is unreachable (port closed); 3 connection attempts allowed.
    Expected: Run fails at extract after 3 attempts with one error alert; watermarks and warehouse unchanged."""
    from etl.config import Config
    before, wm = facts(base), watermarks(base)
    monkeypatch.setenv("ETL_CRM_DSN", "host=127.0.0.1 port=1 dbname=aisleco_crm_test")   # nothing listens there
    monkeypatch.setenv("ETL_CONNECT_RETRIES", "3")
    cfg = Config()
    with pytest.raises(ConnectionError, match="after 3 attempts"):
        pipeline.run(cfg, ASOF)
    run = last_run(cfg)
    assert run["Status"] == "failed" and "extract" in run["Message"]
    assert [r["Step"] for r in q(cfg, 'SELECT "Step" FROM etl."StepRun" WHERE "RunID"=%s', (run["RunID"],))] == ["extract"]
    assert q(cfg, 'SELECT count(*) AS n FROM etl."Alert" WHERE "RunID"=%s AND "Severity"=\'error\'', (run["RunID"],))[0]["n"] == 1
    assert watermarks(cfg) == wm                                   # the next run re-reads the same window
    after = facts(cfg)
    assert all(before[k].astype(str).equals(after[k].astype(str)) for k in before)


def test_bad_row_goes_to_dead_letter_and_comes_back_when_fixed(base):
    """Scenario: T rejects a row: a staff chat message arrives without the staff member.
    Expected: Run succeeds; the row is in the dead letter (chat_staff_without_employee), not in staging. Once fixed in the CRM, the next run loads it and marks the dead letter resolved."""
    cfg = base
    # The inbox logged a staff reply without the staff member (the CRM does not enforce it)
    crm_exec('''INSERT INTO transaction."ChatMessage"("ChatMessageID","CustomerAccountID","InquiryID","PlatformMessageID",
                "SentAt","Direction","SenderType","EmployeeID") VALUES (900,3,3,'L-0900','2026-11-22 10:00','Out','Staff',NULL)''', cfg)
    run1 = pipeline.run(cfg, ASOF)
    assert last_run(cfg)["Status"] == "success"
    dead = q(cfg, 'SELECT d.*, r."RuleName" FROM etl."RejectedRow" d JOIN etl."DqRule" r USING ("RuleID")')
    assert [(d["SourceTable"], d["SourceKey"], d["RuleName"], d["ResolvedRunID"]) for d in dead] == \
           [("ChatMessage", 900, "chat_staff_without_employee", None)]
    assert dead[0]["Payload"]["SenderType"] == "Staff"
    assert not q(cfg, 'SELECT 1 FROM stg."ChatMessage" WHERE "ChatMessageID"=900')
    # Someone fixes it in the CRM -> UpdatedAt moves -> the next run picks it up and resolves the dead letter
    crm_exec('UPDATE transaction."ChatMessage" SET "EmployeeID"=5 WHERE "ChatMessageID"=900', cfg)
    run2 = pipeline.run(cfg, ASOF)
    assert q(cfg, 'SELECT "ResolvedRunID" FROM etl."RejectedRow"')[0]["ResolvedRunID"] == run2 > run1
    assert q(cfg, 'SELECT "EmployeeID" FROM stg."ChatMessage" WHERE "ChatMessageID"=900')[0]["EmployeeID"] == 5


def test_rejected_parent_takes_its_children_with_it(base):
    """Scenario: T rejects a parent: a new event without a wedding date, with one assignment.
    Expected: Event rejected (event_missing_dates) and its assignment too (assignment_orphan); the run still succeeds."""
    cfg = base
    crm_exec('''INSERT INTO master."Customer"("CustomerID") VALUES (50);
                INSERT INTO transaction."Event" VALUES (50,50,NULL,3,NULL,100,'Low',90000,'Confirmed','2026-12-01');
                INSERT INTO transaction."EventEmployeeAssignment"("EventID","EmployeeID","WorkloadHours") VALUES (50,2,20)''', cfg)
    pipeline.run(cfg, ASOF)                                        # event without a date: rejected, so is its assignment
    rules = sorted(r["RuleName"] for r in q(cfg, 'SELECT r."RuleName" FROM etl."RejectedRow" d JOIN etl."DqRule" r USING ("RuleID")'))
    assert rules == ["assignment_orphan", "event_missing_dates"]
    assert last_run(cfg)["Status"] == "success"


def test_blocking_quality_gate_stops_before_the_warehouse(base, monkeypatch):
    """Scenario: T finds a systematic problem: an upstream change sends 30 messages with an unknown sender type.
    Expected: The gate stops the run (reject rate 30/30) after extract, stage and dq_gate; facts and watermarks unchanged."""
    from etl.config import Config
    cfg = base
    before, wm = facts(cfg), watermarks(cfg)
    # An upstream change starts sending a new sender type on every message: a systematic problem
    crm_exec('''INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt","Direction","SenderType")
                SELECT 3, 3, 'X-'||k, '2026-11-22 10:00'::timestamp + k * interval '1 minute', 'Out', 'AutoReply'
                FROM generate_series(1,30) k''', cfg)
    monkeypatch.setenv("ETL_OVERLAP_SECONDS", "0")
    with pytest.raises(DataQualityError, match="reject_rate: ChatMessage 30/30"):
        pipeline.run(Config(), ASOF)
    run = last_run(cfg)
    steps = [r["Step"] for r in q(cfg, 'SELECT "Step" FROM etl."StepRun" WHERE "RunID"=%s ORDER BY 1', (run["RunID"],))]
    assert sorted(steps) == ["dq_gate", "extract", "stage"] and run["Status"] == "failed"
    after = facts(cfg)
    assert all(before[k].astype(str).equals(after[k].astype(str)) for k in before)
    assert watermarks(cfg) == wm


def test_crash_during_load_then_resume_from_checkpoint(base, monkeypatch):
    """Scenario: L crashes: Fah's assignment goes 16 -> 20 h and the weekly workload step crashes before it commits; then resume.
    Expected: The failed step is rolled back (week still 0.8 h); resume reruns only that step, finished steps are not rerun; result 1.6 h; ended weeks stay frozen and are reported once as a late change; a further run changes nothing."""
    from etl.config import Config
    cfg = base
    crm_exec('UPDATE transaction."EventEmployeeAssignment" SET "WorkloadHours"=20 WHERE "EventID"=104 AND "EmployeeID"=6', cfg)
    monkeypatch.setenv("ETL_FAIL_AT", "fact_weekly_workload")
    with pytest.raises(RuntimeError, match="injected failure"):
        pipeline.run(Config(), ASOF)
    run_id = last_run(cfg)["RunID"]
    # 16 h -> 20 h: planning goes from 4 h to 8 h over 4 Jan - 1 Feb (0.8 -> 1.6 h a week); only
    # the week of 1 Feb is still open, the January weeks have ended and stay frozen
    fah = lambda: float(q(cfg, '''SELECT "TotalAssignedHours" AS h FROM mart.v_weekly_workload
                                  WHERE employee LIKE 'Fah%%' AND week_start='2027-02-01' ''')[0]["h"])
    assert fah() == 0.8                                            # the failed step was rolled back completely
    monkeypatch.delenv("ETL_FAIL_AT")
    pipeline.resume(Config(), run_id)
    attempts = {r["Step"]: r["n"] for r in q(cfg, 'SELECT "Step", count(*) AS n FROM etl."StepRun" WHERE "RunID"=%s '
                                                  'GROUP BY 1', (run_id,))}
    assert attempts["extract"] == attempts["fact_inquiry"] == 1     # finished steps were not run again
    assert attempts["fact_weekly_workload"] == 2
    assert last_run(cfg)["Status"] == "success" and fah() == 1.6
    # the resumed result equals what a clean run produces: a further run changes nothing
    # the 4 extra planning hours also belong to 4 January weeks that have ended: kept frozen,
    # reported once as a late change (the alert of the rolled-back attempt was rolled back too)
    late = lambda: q(cfg, '''SELECT "RunID" FROM etl."Alert" WHERE "Message" LIKE '%%late change%%' ''')
    assert [a["RunID"] for a in late()] == [run_id]
    assert "4 row(s)" in q(cfg, '''SELECT "Message" FROM etl."Alert" WHERE "Message" LIKE '%%late change%%' ''')[0]["Message"]
    snapshot = facts(cfg)
    pipeline.run(Config(), ASOF)
    assert all(snapshot[k].astype(str).equals(facts(cfg)[k].astype(str)) for k in snapshot)
    assert len(late()) == 1                                         # same condition, not alerted again


def test_damaged_landing_file_is_refused(base):
    """Scenario: E->L: the Inquiry landing file is altered after extract.
    Expected: The stage step refuses it (checksum mismatch); nothing is staged."""
    cfg = base
    run_id = pipeline.start(cfg, ASOF)
    pipeline.run_step(cfg, run_id, "extract")
    f = cfg.landing_dir / f"run_{run_id:05d}" / "Inquiry.csv"
    f.write_text(f.read_text() + "999,1,,2026-01-01 00:00:00,,,,,,,,,,New,,2026-01-01 00:00:00\n")
    with pytest.raises(LandingIntegrityError, match="checksum mismatch for Inquiry.csv"):
        pipeline.run_step(cfg, run_id, "stage")
    assert not q(cfg, 'SELECT 1 FROM stg."Inquiry" WHERE "InquiryID"=999')


def test_resume_refused_for_an_old_run(base, monkeypatch):
    """Scenario: A run fails, a newer run succeeds, then someone resumes the old failed run.
    Expected: Refused ('not the latest run'), because it would replay older data over newer; a successful run cannot be resumed either."""
    from etl.config import Config
    monkeypatch.setenv("ETL_FAIL_AT", "stage")
    with pytest.raises(RuntimeError):
        pipeline.run(Config(), ASOF)
    failed = last_run(base)["RunID"]
    monkeypatch.delenv("ETL_FAIL_AT")
    pipeline.run(Config(), ASOF)
    with pytest.raises(ValueError, match="not the latest run"):
        pipeline.resume(Config(), failed)
    with pytest.raises(ValueError, match="already succeeded"):
        pipeline.resume(Config(), failed + 1)


def test_asof_cannot_move_backwards(base):
    """Scenario: A run dated 31 Jan after a run as of 1 Feb.
    Expected: Refused (AsOfError), because it would reopen weeks that are already frozen."""
    from etl.pipeline import AsOfError
    with pytest.raises(AsOfError, match="earlier than the last successful run"):
        pipeline.run(base, date(2027, 1, 31))
