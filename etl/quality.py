"""DATA QUALITY: rules kept as metadata (etl.DqRule), applied at three points.

  row   - each extracted row in land.<table>, before it reaches staging.
          reject -> the row goes to etl.RejectedRow (dead letter) and is not loaded
          warn   -> logged, row loaded
  set   - the whole staging table after the merge (dq_gate step, before any dim/fact)
          block  -> the run stops here; the warehouse is not touched
  recon - source-to-target reconciliation after the load (reconcile step)

Predicates use the alias r for the row being checked.
"""
import logging

from psycopg import sql

from . import meta

log = logging.getLogger("etl")

STATUS = "('New','Contacted','Quoted','Converted','Lost')"

RULES = [
    # ---- row rules: reject (dead letter) ----
    ("chat_staff_without_employee", "ChatMessage", "row", "reject",
     "A staff message must name the staff member (needed for first responder)",
     """r."SenderType" = 'Staff' AND r."EmployeeID" IS NULL"""),
    ("chat_invalid_values", "ChatMessage", "row", "reject",
     "Direction In/Out, SenderType Customer/Staff/Bot and SentAt are required",
     """r."SentAt" IS NULL OR r."Direction" IS DISTINCT FROM 'In' AND r."Direction" IS DISTINCT FROM 'Out'
        OR r."SenderType" IS NULL OR r."SenderType" NOT IN ('Customer','Staff','Bot')"""),
    ("chat_orphan_inquiry", "ChatMessage", "row", "reject",
     "Message points to an inquiry that is not in staging (e.g. the inquiry was rejected)",
     """r."InquiryID" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM stg."Inquiry" p WHERE p."InquiryID" = r."InquiryID")"""),
    ("inquiry_missing_date", "Inquiry", "row", "reject",
     "InquiryDate is required (every conversion KPI is grouped by it)", """r."InquiryDate" IS NULL"""),
    ("inquiry_invalid_status", "Inquiry", "row", "reject",
     "Status must be New / Contacted / Quoted / Converted / Lost",
     f"""r."Status" IS NOT NULL AND r."Status" NOT IN {STATUS}"""),
    ("inquiry_negative_quote", "Inquiry", "row", "reject",
     "QuotedAmount cannot be negative", """r."QuotedAmount" < 0"""),
    ("inquiry_orphan_channel", "Inquiry", "row", "reject",
     "Channel not in staging", """NOT EXISTS (SELECT 1 FROM stg."MarketingChannel" p WHERE p."ChannelID" = r."ChannelID")"""),
    ("event_missing_dates", "Event", "row", "reject",
     "EventDate and CreatedDate (booking date) are required to place the hours in weeks",
     """r."EventDate" IS NULL OR r."CreatedDate" IS NULL"""),
    ("event_invalid_values", "Event", "row", "reject",
     "GuestCount and AgreedPackagePrice cannot be negative", """r."GuestCount" < 0 OR r."AgreedPackagePrice" < 0"""),
    ("event_orphan_inquiry", "Event", "row", "reject",
     "Event points to an inquiry that is not in staging",
     """r."InquiryID" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM stg."Inquiry" p WHERE p."InquiryID" = r."InquiryID")"""),
    ("assignment_invalid_hours", "EventEmployeeAssignment", "row", "reject",
     "WorkloadHours must be between 0 and 1000", """r."WorkloadHours" IS NULL OR r."WorkloadHours" < 0 OR r."WorkloadHours" > 1000"""),
    ("assignment_orphan", "EventEmployeeAssignment", "row", "reject",
     "Assignment points to an event or employee that is not in staging",
     """NOT EXISTS (SELECT 1 FROM stg."Event" p WHERE p."EventID" = r."EventID")
        OR NOT EXISTS (SELECT 1 FROM stg."Employee" p WHERE p."EmployeeID" = r."EmployeeID")"""),
    ("employee_invalid_hours", "Employee", "row", "reject",
     "StandardWeeklyHours must be between 1 and 80 (it is the utilization denominator)",
     """r."StandardWeeklyHours" IS NULL OR r."StandardWeeklyHours" < 1 OR r."StandardWeeklyHours" > 80"""),
    ("auspicious_invalid_level", "AuspiciousDate", "row", "reject",
     "AuspiciousLevel must be Low / Medium / High",
     """r."CalendarDate" IS NULL OR r."AuspiciousLevel" IS NULL OR r."AuspiciousLevel" NOT IN ('Low','Medium','High')"""),
    # ---- row rules: warn ----
    ("inquiry_budget_unparseable", "Inquiry", "row", "warn",
     "BudgetRange cannot be read as a range (BudgetMidpoint and quote share will be empty)",
     """r."BudgetRange" IS NOT NULL AND etl.budget_mid(r."BudgetRange") IS NULL"""),
    ("event_booked_before_inquiry", "Event", "row", "warn",
     "Booking date is earlier than the inquiry date",
     """EXISTS (SELECT 1 FROM stg."Inquiry" i WHERE i."InquiryID" = r."InquiryID" AND r."CreatedDate" < i."InquiryDate")"""),
    # ---- set rules on staging (dq_gate) ----
    ("stg_chat_orphans", "ChatMessage", "set", "block",
     "No message in staging may point to a missing inquiry",
     """r."InquiryID" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM stg."Inquiry" p WHERE p."InquiryID" = r."InquiryID")"""),
    ("stg_assignment_orphans", "EventEmployeeAssignment", "set", "block",
     "No assignment in staging may point to a missing event or employee",
     """NOT EXISTS (SELECT 1 FROM stg."Event" p WHERE p."EventID" = r."EventID")
        OR NOT EXISTS (SELECT 1 FROM stg."Employee" p WHERE p."EmployeeID" = r."EmployeeID")"""),
    ("stg_converted_without_event", "Inquiry", "set", "warn",
     "Inquiry marked Converted but no event is linked to it",
     """r."Status" = 'Converted' AND NOT EXISTS (SELECT 1 FROM stg."Event" e WHERE e."InquiryID" = r."InquiryID")"""),
    ("reject_rate", "*", "set", "block",
     "Share of rejected rows per table above the limit (signals a systematic upstream problem)",
     "-- computed by the pipeline from the stage step's counts"),
    # ---- reconciliation (after the load) ----
    ("recon_inquiry_count", "FactInquiry", "recon", "block",
     "Every staged inquiry has exactly one FactInquiry row and nothing else",
     """SELECT (SELECT count(*) FROM stg."Inquiry" s FULL JOIN fact."FactInquiry" f USING ("InquiryID")
               WHERE s."InquiryID" IS NULL OR f."InquiryID" IS NULL) AS failed,
              (SELECT count(*) FROM stg."Inquiry") AS checked"""),
    ("recon_event_count", "FactEventBooking", "recon", "block",
     "Every staged event has exactly one FactEventBooking row and nothing else",
     """SELECT (SELECT count(*) FROM stg."Event" s FULL JOIN fact."FactEventBooking" f USING ("EventID")
               WHERE s."EventID" IS NULL OR f."EventID" IS NULL) AS failed,
              (SELECT count(*) FROM stg."Event") AS checked"""),
    ("recon_hours_per_event", "FactEventBooking", "recon", "block",
     "Staff hours per event in the fact equal the sum of the staged assignments",
     """SELECT count(*) FILTER (WHERE abs(f."TotalStaffHours" - coalesce(s.h,0)) > 0.005) AS failed, count(*) AS checked
        FROM fact."FactEventBooking" f
        LEFT JOIN (SELECT "EventID", sum("WorkloadHours") h FROM stg."EventEmployeeAssignment" GROUP BY 1) s USING ("EventID")"""),
    ("recon_hours_spread", "FactEmployeeWeeklyWorkload", "recon", "block",
     "Spreading hours over weeks neither creates nor loses hours (non-cancelled events)",
     # each weekly row is rounded to 0.01 h, so allow 0.005 h of rounding per row
     """SELECT CASE WHEN abs(coalesce((SELECT sum("TotalAssignedHours") FROM work."FactEmployeeWeeklyWorkload"),0)
                            - coalesce((SELECT sum(a."WorkloadHours") FROM stg."EventEmployeeAssignment" a
                                        JOIN stg."Event" e USING ("EventID")
                                        WHERE e."Status" IS DISTINCT FROM 'Cancelled'),0))
                        <= 0.005 * (SELECT count(*) FROM work."FactEmployeeWeeklyWorkload") + 0.01
                 THEN 0 ELSE 1 END AS failed, 1 AS checked"""),
    ("recon_unknown_keys", "FactInquiry", "recon", "block",
     "No fact row points to the Unknown member when the source has a value",
     """SELECT (SELECT count(*) FROM fact."FactInquiry" f JOIN stg."Inquiry" s USING ("InquiryID")
               WHERE f."IndicativePackageKey" = -1 AND s."IndicativePackageID" IS NOT NULL)
            + (SELECT count(*) FROM fact."FactEventBooking" f JOIN stg."Event" s USING ("EventID")
               WHERE f."PackageKey" = -1 AND s."PackageID" IS NOT NULL) AS failed,
              (SELECT count(*) FROM stg."Inquiry") + (SELECT count(*) FROM stg."Event") AS checked"""),
    ("recon_source_keys", "*", "recon", "block",
     "Staging holds exactly the CRM keys of this run's snapshot, except rows waiting in the dead letter or the backlog",
     "-- computed by the pipeline from land.<table>_Keys"),
    ("frozen_week_late_change", "FactEmployeeWeeklyWorkload", "recon", "warn",
     "Hours or events in a week that has ended no longer match the CRM (late change); frozen weeks are not back-filled",
     "-- computed by the weekly load"),
]


def seed_rules(conn):
    for name, table, kind, sev, desc, check in RULES:
        conn.execute(
            'INSERT INTO etl."DqRule"("RuleName","TableName","Kind","Severity","Description","CheckSql","IsActive") '
            "VALUES (%s,%s,%s,%s,%s,%s,'Y') ON CONFLICT (\"RuleName\") DO UPDATE SET \"TableName\"=EXCLUDED.\"TableName\", "
            '"Kind"=EXCLUDED."Kind", "Severity"=EXCLUDED."Severity", "Description"=EXCLUDED."Description", '
            '"CheckSql"=EXCLUDED."CheckSql"', (name, table, kind, sev, desc, check))


def rule_id(conn, name):
    return conn.execute('SELECT "RuleID" FROM etl."DqRule" WHERE "RuleName"=%s', (name,)).fetchone()["RuleID"]


def record(conn, run_id, name, checked, failed):
    conn.execute('INSERT INTO etl."DqResult"("RunID","RuleID","RowsChecked","RowsFailed","Passed","CheckedAt") '
                 "VALUES (%s,%s,%s,%s,%s,clock_timestamp())",
                 (run_id, rule_id(conn, name), checked, failed, "Y" if failed == 0 else "N"))


def _rules(conn, table, kind):
    return conn.execute('SELECT * FROM etl."DqRule" WHERE "TableName"=%s AND "Kind"=%s AND "IsActive"=\'Y\' '
                        'ORDER BY "RuleID"', (table, kind)).fetchall()


def apply_row_rules(conn, run_id, src) -> dict:
    """Check the landed rows of one table. Rejected rows are copied to the dead letter and
    removed from land.<table> so they are not merged. Returns {rule: failed rows}."""
    land = sql.Identifier("land", src.table)
    checked = conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(land)).fetchone()["n"]
    out = {}
    for rule in _rules(conn, src.table, "row"):
        pred = sql.SQL(rule["CheckSql"])
        if rule["Severity"] == "reject":
            failed = conn.execute(sql.SQL(
                'WITH bad AS (DELETE FROM {land} r WHERE {pred} RETURNING r.*) '
                'INSERT INTO etl."RejectedRow"("RunID","RuleID","SourceTable","SourceKey","Payload","RejectedAt") '
                'SELECT %s, %s, %s, b.{key}, to_jsonb(b), clock_timestamp() FROM bad b').format(
                    land=land, pred=pred, key=sql.Identifier(src.key)),
                (run_id, rule["RuleID"], src.table)).rowcount
        else:
            failed = conn.execute(sql.SQL("SELECT count(*) AS n FROM {land} r WHERE {pred}").format(
                land=land, pred=pred)).fetchone()["n"]
        record(conn, run_id, rule["RuleName"], checked, failed)
        if failed:
            out[rule["RuleName"]] = failed
            meta.alert(conn, run_id, "warning",
                       f"{src.table}: {failed} row(s) {'rejected' if rule['Severity'] == 'reject' else 'flagged'} "
                       f"by {rule['RuleName']}")
    return out


def gate(ctx, conn) -> dict:
    """dq_gate step: set rules on staging + the reject-rate limit. A blocking failure raises,
    so no dimension or fact is touched in this run."""
    blocking, results = [], {}
    for table in {r[1] for r in RULES if r[2] == "set" and r[1] != "*"}:
        for rule in _rules(conn, table, "set"):
            stg = sql.Identifier("stg", table)
            q = sql.SQL("SELECT count(*) AS checked, count(*) FILTER (WHERE {p}) AS failed FROM {t} r").format(
                p=sql.SQL(rule["CheckSql"]), t=stg)
            r = conn.execute(q).fetchone()
            record(conn, ctx.run_id, rule["RuleName"], r["checked"], r["failed"])
            results[rule["RuleName"]] = r["failed"]
            if r["failed"] and rule["Severity"] == "block":
                blocking.append(f"{rule['RuleName']}: {r['failed']} row(s)")
    # reject rate per table, from the stage step of this run
    stage = conn.execute('SELECT "Metrics" FROM etl."StepRun" WHERE "RunID"=%s AND "Step"=\'stage\' '
                         'AND "Status"=\'success\' ORDER BY "StepRunID" DESC LIMIT 1', (ctx.run_id,)).fetchone()
    worst = 0
    for table, m in ((stage or {}).get("Metrics") or {}).get("tables", {}).items():
        if m["extracted"] >= ctx.cfg.min_rows_for_rate and m["rejected"] / m["extracted"] > ctx.cfg.max_reject_rate:
            blocking.append(f"reject_rate: {table} {m['rejected']}/{m['extracted']} rejected "
                            f"(limit {ctx.cfg.max_reject_rate:.0%})")
            worst += 1
    record(conn, ctx.run_id, "reject_rate", len(((stage or {}).get("Metrics") or {}).get("tables", {})), worst)
    if blocking:
        raise DataQualityError("data-quality gate failed: " + "; ".join(blocking))
    return {"rules_checked": len(results), "warnings": {k: v for k, v in results.items() if v}}


def reconcile(ctx, conn) -> dict:
    """reconcile step: source-to-target checks after the load."""
    failures, results = [], {}
    for rule in conn.execute('SELECT * FROM etl."DqRule" WHERE "Kind"=\'recon\' AND "IsActive"=\'Y\' '
                             'AND "CheckSql" NOT LIKE \'--%%\' ORDER BY "RuleID"').fetchall():
        r = conn.execute(rule["CheckSql"]).fetchone()
        record(conn, ctx.run_id, rule["RuleName"], r["checked"], r["failed"])
        results[rule["RuleName"]] = r["failed"]
        if r["failed"]:
            failures.append(f"{rule['RuleName']}: {r['failed']}")
    # staging keys == CRM keys of this run's snapshot, except rows held in the dead letter
    from .config import SOURCES
    from .stage import load_keys
    key_diff = 0
    for src in SOURCES:
        load_keys(ctx, conn, src)   # from the landed file, so this also works on a resumed run
        q = sql.SQL('''SELECT count(*) AS n FROM {stg} s FULL JOIN {keys} k ON k."Key" = s.{key}
                       WHERE (k."Key" IS NULL OR s.{key} IS NULL AND NOT k."Pending")   -- backlog rows come next run
                         AND NOT EXISTS (SELECT 1 FROM etl."RejectedRow" d WHERE d."SourceTable" = %s
                                         AND d."SourceKey" = k."Key" AND d."ResolvedRunID" IS NULL)''').format(
            stg=sql.Identifier("stg", src.table), keys=sql.Identifier("land", src.table + "_Keys"),
            key=sql.Identifier(src.key))
        n = conn.execute(q, (src.table,)).fetchone()["n"]
        key_diff += n
        if n:
            failures.append(f"recon_source_keys: {src.table} {n} key(s) differ")
    record(conn, ctx.run_id, "recon_source_keys", len(SOURCES), key_diff)
    results["recon_source_keys"] = key_diff
    if failures:
        raise DataQualityError("reconciliation failed: " + "; ".join(failures))
    return {"checks": results}


class DataQualityError(Exception):
    pass
