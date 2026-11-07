"""Batch cap (ETL_MAX_ROWS_PER_RUN): a burst larger than the cap waits in the CRM backlog and
is drained by the following runs, without orphans, dead letters or lost changes."""
from dataclasses import replace
from datetime import date, datetime, timedelta

import pytest

import load_walkthrough
from conftest import facts, q
from crm import crm_exec
from etl import db, pipeline
from etl.extract import read_from

ASOF = date(2027, 2, 1)


@pytest.fixture
def base(cfg):
    load_walkthrough.main(cfg)
    return replace(cfg, overlap_seconds=0)


def extract_metrics(cfg, run_id):
    return q(cfg, '''SELECT "Metrics" FROM etl."StepRun" WHERE "RunID"=%s AND "Step"='extract' AND "Status"='success' ''',
             (run_id,))[0]["Metrics"]


def next_run_later(cfg):
    """Runs here are seconds apart; in production they are at least 5 minutes apart. Moving the
    recorded snapshot time 10 minutes on gives the next run the overlap it would have then."""
    with db.connect(cfg.dw_dsn, cfg) as c:
        c.execute('UPDATE etl."Watermark" SET "ExtractedAt" = "ExtractedAt" + interval \'10 minutes\'')


def test_read_window():
    """Scenario: Where an incremental read starts, for four watermark / snapshot situations.
    Expected: No watermark: read all. Caught up: 5 min before the previous snapshot. Draining a backlog: exactly at the watermark (no re-read)."""
    wm, s, five = datetime(2027, 1, 1, 10, 0), datetime(2027, 1, 1, 10, 3), timedelta(minutes=5)
    assert read_from(None, s, five) is None                                   # no watermark: read all
    assert read_from(wm, None, five) == wm - five                             # older watermark rows: as before
    assert read_from(wm, s, five) == s - five                                 # caught up: late commits re-read
    assert read_from(wm, s + timedelta(hours=1), five) == wm                  # draining a backlog: no re-read


def test_burst_is_queued_then_drained(base):
    """Scenario: 10 new messages (one CRM transaction each), then inquiry 1 updated; cap 4; production 5-minute overlap.
    Expected: At most 4 messages per run; run 1 pulls inquiry 1 along and queues 6; drained in 3 runs; one backlog alert; no dead letters; same warehouse as a full refresh."""
    acct = q(base, 'SELECT "CustomerAccountID" FROM transaction."ChatMessage" WHERE "InquiryID"=1 LIMIT 1',
             dsn=base.crm_dsn)[0]["CustomerAccountID"]
    next_run_later(base)
    n = 10
    for k in range(n):                      # new messages, one CRM transaction each, as in real use
        crm_exec(f'''INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt",
                     "Direction","SenderType") VALUES ({acct}, 1, 'burst-{k}', '2026-04-03 10:{k:02d}', 'In', 'Customer')''', base)
    crm_exec('UPDATE transaction."Inquiry" SET "QuotedAmount" = 99000 WHERE "InquiryID" = 1', base)   # the newest change
    capped = replace(base, max_rows_per_run=4, overlap_seconds=300)          # production overlap

    runs, backlog = [], None
    while backlog != 0 and len(runs) < 10:
        runs.append(pipeline.run(capped, ASOF))                             # reconcile passes: queued keys are Pending
        m = extract_metrics(capped, runs[-1])
        assert m["per_table"]["ChatMessage"] <= 4                          # the cap holds, no overlap re-reads pile up
        backlog = m["backlog"]
        next_run_later(base)
    first = extract_metrics(capped, runs[0])
    # run 1: the 4 oldest new messages, plus inquiry 1 pulled forward as their parent
    assert (first["per_table"]["ChatMessage"], first["per_table"]["Inquiry"], first["backlog"]) == (4, 1, n - 4)
    assert len(runs) == -(-n // 4)                                           # ceil(changes / cap) runs
    assert q(base, '''SELECT count(*) AS n FROM etl."Alert" WHERE "RunID"=%s AND "Message" LIKE '%%backlog%%' ''',
             (runs[0],))[0]["n"] == 1
    assert q(base, 'SELECT count(*) AS n FROM etl."RejectedRow"')[0]["n"] == 0
    assert q(base, 'SELECT count(*) AS n FROM stg."ChatMessage" WHERE "InquiryID"=1')[0]["n"] >= n
    assert q(base, 'SELECT "QuotedAmount" FROM fact."FactInquiry" WHERE "InquiryID"=1')[0]["QuotedAmount"] == 99000

    drained = facts(base)
    pipeline.run(base, ASOF, "full")
    full = facts(base)
    assert all(drained[k].astype(str).equals(full[k].astype(str)) for k in drained)


def test_child_pulls_its_new_parent_into_the_batch(base):
    """Scenario: New inquiry 900 with a chat message; the inquiry is then updated after the message; cap 1.
    Expected: The message is taken and pulls inquiry 900 into the same batch; nothing rejected as an orphan; nothing queued."""
    i = q(base, 'SELECT "CustomerID","ChannelID" FROM transaction."Inquiry" WHERE "InquiryID"=1', dsn=base.crm_dsn)[0]
    acct = q(base, 'SELECT "CustomerAccountID" FROM transaction."ChatMessage" LIMIT 1', dsn=base.crm_dsn)[0]
    crm_exec(f'''INSERT INTO transaction."Inquiry"("InquiryID","CustomerID","ChannelID","InquiryDate","Status")
                 VALUES (900, {i["CustomerID"]}, {i["ChannelID"]}, '2027-01-30 09:00', 'New')''', base)
    crm_exec(f'''INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt",
                 "Direction","SenderType") VALUES ({acct["CustomerAccountID"]}, 900, 'cap-test-1', '2027-01-30 09:00', 'In', 'Customer')''', base)
    crm_exec('UPDATE transaction."Inquiry" SET "Status"=\'Contacted\' WHERE "InquiryID"=900', base)   # parent now newer than child
    capped = replace(base, max_rows_per_run=1)

    run1 = pipeline.run(capped, ASOF)       # cutoff = the chat message; it pulls inquiry 900 forward
    m = extract_metrics(capped, run1)
    assert (m["per_table"]["ChatMessage"], m["per_table"]["Inquiry"], m["backlog"]) == (1, 1, 0)
    assert q(base, 'SELECT count(*) AS n FROM etl."RejectedRow"')[0]["n"] == 0
    assert q(base, 'SELECT "Outcome" FROM mart.v_inquiry WHERE "InquiryID"=900')[0]["Outcome"] == "Open"
    run2 = pipeline.run(capped, ASOF)       # the pulled parent is read once more, nothing is lost or repeated
    assert extract_metrics(capped, run2)["backlog"] == 0
