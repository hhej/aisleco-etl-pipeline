"""Transformation logic: small hand-made cases, one rule per assertion."""
from datetime import date

import pytest

from conftest import q
from crm import create_crm, crm_exec
from etl import pipeline
from etl.__main__ import init_dw

FIXTURE = """
INSERT INTO master."MarketingChannel" VALUES (1,'LINE OA','Online',NULL,'Active'),(8,'Walk-in','Offline',NULL,'Active');
INSERT INTO master."Package" VALUES (1,'Full Planning','Full',NULL,'Percent',12.5,'2025-01-01',NULL,'Active');
INSERT INTO master."EmployeeRole" VALUES (1,'Planner','Active'),(2,'Sales Executive','Active');
INSERT INTO master."Employee" VALUES
 (1,1,'E001','A','A.','FullTime','2024-01-01',40,10,'Active'),(2,1,'E002','B','B.','FullTime','2024-01-01',40,10,'Active'),
 (3,1,'E003','C','C.','FullTime','2024-01-01',40,10,'Active'),(4,1,'E004','D','D.','FullTime','2024-01-01',40,10,'Active'),
 (5,2,'E005','May','M.','FullTime','2024-01-01',44,NULL,'Active');
INSERT INTO master."Customer"("CustomerID") SELECT generate_series(1,6);
INSERT INTO master."CustomerChannelAccount"("CustomerAccountID","CustomerID","Platform","ExternalUserID")
  SELECT k, k, 'LINE', 'U'||k FROM generate_series(1,6) k;
INSERT INTO transaction."Inquiry"("InquiryID","CustomerID","ChannelID","IndicativePackageID","InquiryDate","BudgetRange",
  "QuotedAmount","QuotedDate","ConsultationDate","Status") VALUES
 (1,1,1,1,'2026-03-20 21:40','1M-1.5M',125000,'2026-03-25','2026-03-22','Converted'),
 (2,2,1,1,'2026-03-20 10:00','300k-500k',NULL,NULL,NULL,'Contacted'),
 (3,3,1,NULL,'2026-03-20 23:00',NULL,NULL,NULL,NULL,'New'),
 (4,4,8,1,'2026-03-21 11:00','500k-1M',NULL,NULL,NULL,'Contacted'),
 (5,5,1,1,'2026-03-21 12:00','around 800k',90000,'2026-03-28','2026-03-25','Quoted');
INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt","Direction","SenderType","EmployeeID") VALUES
 (1,1,'m1','2026-03-20 21:40','In','Customer',NULL),(1,1,'m2','2026-03-20 21:41','Out','Bot',NULL),
 (1,1,'m3','2026-03-21 10:05','Out','Staff',5),
 (2,2,'m4','2026-03-20 09:00','Out','Staff',5),           -- broadcast BEFORE the couple wrote
 (2,2,'m5','2026-03-20 10:00','In','Customer',NULL),(2,2,'m6','2026-03-20 10:30','Out','Staff',1),
 (3,3,'m7','2026-03-20 23:00','In','Customer',NULL),(3,3,'m8','2026-03-20 23:01','Out','Bot',NULL),
 (5,5,'m9','2026-03-21 12:00','In','Customer',NULL),(5,5,'m10','2026-03-21 12:20','Out','Staff',5);
INSERT INTO transaction."Event" VALUES
 (1,1,1,1,'2027-02-13',300,'Medium',120000,'Confirmed','2026-04-01'),
 (2,6,NULL,1,'2027-03-06',100,'Low',50000,'Cancelled','2026-04-02');
INSERT INTO transaction."EventEmployeeAssignment"("EventID","EmployeeID","AssignmentRole","AssignedDate","WorkloadHours","IsLead","Status") VALUES
 (1,1,'Planner','2027-02-09',30,'N','Active'),     -- assigned in the wedding week
 (1,2,'Planner','2027-02-20',20,'N','Active'),     -- assigned after the wedding
 (1,3,'Planner',NULL,52,'Y','Active'),             -- no assignment date -> booking date
 (1,4,'Planner','2026-06-01',10,'N','Active'),     -- <= 12 h: all wedding-day work
 (2,1,'Planner','2026-04-02',8,'N','Active');      -- cancelled event
"""


@pytest.fixture(scope="module")
def dw(tmp_path_factory):
    import os
    os.environ["ETL_LANDING_DIR"] = str(tmp_path_factory.mktemp("landing"))
    from etl.config import Config
    cfg = Config()
    create_crm(cfg)
    crm_exec(FIXTURE, cfg)
    init_dw(cfg, drop=True)
    run_id = pipeline.run(cfg, date(2026, 4, 6))
    return cfg, run_id


def inquiry(cfg, i):
    return q(cfg, 'SELECT * FROM mart.v_inquiry WHERE "InquiryID"=%s', (i,))[0]


def hours(cfg, emp, week):
    r = q(cfg, '''SELECT w.* FROM fact."FactEmployeeWeeklyWorkload" w JOIN dim."DimEmployee" e USING ("EmployeeKey")
                  WHERE e."EmployeeID"=%s AND w."WeekStartDateKey"=%s''', (emp, week))
    return (float(r[0]["PlanningHours"]), float(r[0]["EventDayHours"])) if r else None


def test_bot_greeting_is_not_a_response(dw):
    """Scenario: Inquiry 1: the couple writes at 21:40, the bot answers at 21:41, a staff member replies at 10:05 the next day.
    Expected: Response time 745 min, measured to the staff reply (the bot is ignored); band 1-24h."""
    r = inquiry(dw[0], 1)
    assert (r["ResponseTimeMinutes"], r["ResponseTimeBand"]) == (745, "1-24h")


def test_first_responder_is_the_staff_member(dw):
    """Scenario: Inquiry 1: the first staff reply was sent by employee 5 (May).
    Expected: FirstResponderEmployeeKey points to employee 5."""
    cfg = dw[0]
    emp = q(cfg, 'SELECT e."EmployeeID" FROM fact."FactInquiry" f JOIN dim."DimEmployee" e '
                 'ON e."EmployeeKey"=f."FirstResponderEmployeeKey" WHERE "InquiryID"=1')[0]["EmployeeID"]
    assert emp == 5


def test_staff_message_before_the_couple_wrote_is_ignored(dw):
    """Scenario: Inquiry 2: a staff broadcast at 09:00 BEFORE the couple writes at 10:00; a staff reply follows at 10:30.
    Expected: Response time 30 min, band <1h: the earlier broadcast is not a reply."""
    r = inquiry(dw[0], 2)
    assert (r["ResponseTimeMinutes"], r["ResponseTimeBand"]) == (30, "<1h")


def test_only_bot_replied_is_not_logged(dw):
    """Scenario: Inquiry 3: the couple writes, only the bot answers; no package of interest given.
    Expected: Response time empty, band 'Not logged'; first responder and package point to the Unknown member (-1)."""
    r = inquiry(dw[0], 3)
    assert r["ResponseTimeMinutes"] is None and r["ResponseTimeBand"] == "Not logged"
    assert r["FirstResponderEmployeeKey"] == -1 and r["IndicativePackageKey"] == -1


def test_walk_in_is_in_person(dw):
    """Scenario: Inquiry 4 comes in through the Walk-in channel, with no chat.
    Expected: Band 'In person'."""
    assert inquiry(dw[0], 4)["ResponseTimeBand"] == "In person"


def test_quote_share_of_budget(dw):
    """Scenario: Inquiry 1 is quoted 125,000 on a 1M-1.5M budget; inquiry 5's budget is 'around 800k'.
    Expected: Quote share 0.10 (125k / 1.25M midpoint); empty when the budget cannot be read."""
    assert float(inquiry(dw[0], 1)["QuoteShareOfBudget"]) == pytest.approx(0.1)     # 125k / 1.25M midpoint
    assert inquiry(dw[0], 5)["QuoteShareOfBudget"] is None                          # 'around 800k' unreadable


def test_unreadable_budget_is_flagged_not_rejected(dw):
    """Scenario: Inquiry 5's budget 'around 800k' cannot be parsed as a range.
    Expected: A warn rule records 1 failed row, and the inquiry still loads (5 of 5 in the fact)."""
    cfg, run_id = dw
    r = q(cfg, '''SELECT d."RowsFailed" FROM etl."DqResult" d JOIN etl."DqRule" r USING ("RuleID")
                  WHERE d."RunID"=%s AND r."RuleName"='inquiry_budget_unparseable' ''', (run_id,))
    assert r[0]["RowsFailed"] == 1
    assert q(cfg, 'SELECT count(*) AS n FROM fact."FactInquiry"')[0]["n"] == 5


def test_funnel_flags(dw):
    """Scenario: Inquiry 1 is booked (event created 12 days after the inquiry); inquiry 5 reached the quote only.
    Expected: Inquiry 1: furthest stage Booked, converted, event 1, 12 days to convert. Inquiry 5: furthest stage Quoted."""
    r = inquiry(dw[0], 1)
    assert (r["FurthestStage"], r["IsConverted"], r["ConvertedEventID"], r["DaysToConvert"]) == ("Booked", 1, 1, 12)
    assert inquiry(dw[0], 5)["FurthestStage"] == "Quoted"


def test_hours_assigned_in_wedding_week_all_land_there(dw):
    """Scenario: Employee 1 is assigned 30 h on 9 Feb, in the week of the 13 Feb wedding.
    Expected: All 30 h in the wedding week (18 planning + 12 wedding day); none in the week before."""
    assert hours(dw[0], 1, 20270208) == (18, 12)
    assert hours(dw[0], 1, 20270201) == (0, 0)


def test_hours_assigned_after_wedding_land_in_wedding_week(dw):
    """Scenario: Employee 2 is assigned 20 h on 20 Feb, after the 13 Feb wedding (entered late).
    Expected: All hours in the wedding week: 8 planning + 12 wedding day."""
    assert hours(dw[0], 2, 20270208) == (8, 12)


def test_missing_assignment_date_spreads_from_booking_week(dw):
    """Scenario: Employee 3 is assigned 52 h with no assignment date.
    Expected: 12 h on the wedding day; the 40 planning hours spread evenly from the booking week (30 Mar 2026), 40/45 ≈ 0.89 h a week; nothing before the booking."""
    cfg = dw[0]
    per_week = 40 / 45                                       # 45 weeks from 30 Mar 2026 to 1 Feb 2027
    assert hours(cfg, 3, 20260330)[0] == pytest.approx(per_week, abs=0.005)
    assert hours(cfg, 3, 20270201)[0] == pytest.approx(per_week, abs=0.005)
    assert hours(cfg, 3, 20270208) == (0, 12)
    assert hours(cfg, 3, 20260323) in (None, (0, 0))


def test_short_assignment_is_all_wedding_day(dw):
    """Scenario: Employee 4 is assigned 10 h (12 h or less).
    Expected: All 10 h count as wedding-day hours; no planning hours."""
    assert hours(dw[0], 4, 20270208) == (0, 10)


def test_cancelled_event_has_no_weekly_hours_but_keeps_its_booking_row(dw):
    """Scenario: Event 2 is cancelled; employee 1 had 8 h on it.
    Expected: No hours in the weekly workload; the booking row is kept with IsCancelled = 1 and 8 staff hours."""
    cfg = dw[0]
    assert hours(cfg, 1, 20270301) in (None, (0, 0))
    e = q(cfg, 'SELECT * FROM fact."FactEventBooking" WHERE "EventID"=2')[0]
    assert (e["IsCancelled"], float(e["TotalStaffHours"])) == (1, 8)


def test_hours_per_wedding(dw):
    """Scenario: Event 1 has 4 assignments (30 + 20 + 52 + 10 h).
    Expected: 4 staff, 46 wedding-day hours, 66 planning hours, 112 in total."""
    e = q(dw[0], 'SELECT * FROM fact."FactEventBooking" WHERE "EventID"=1')[0]
    assert (e["StaffCount"], float(e["EventDayHours"]), float(e["PlanningHours"]), float(e["TotalStaffHours"])) == (4, 46, 66, 112)
