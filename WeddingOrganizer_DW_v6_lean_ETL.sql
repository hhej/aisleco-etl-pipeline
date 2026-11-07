-- =====================================================================
-- ETL: OLTP v4 lean (master / transaction) -> DW v6 lean (dim / fact)
-- PostgreSQL, one database holding both. Run daily or weekly.
--   psql -v asof=2027-01-15 -f WeddingOrganizer_DW_v6_lean_ETL.sql   (asof defaults to today)
-- Load behaviour
--   Dimensions            : upsert by OLTP ID (keys never change)
--   FactInquiry, FactEventBooking : full refresh (accumulating, always current)
--   FactEmployeeWeeklyWorkload    : weeks starting on/after the as-of week are rebuilt;
--                                   earlier weeks are frozen (inserted once, then kept)
-- Doubles as the source-to-target mapping for the report.
-- =====================================================================
\set ON_ERROR_STOP 1
\if :{?asof}
\else
  \set asof `date +%F`
\endif
SET client_min_messages = warning;
BEGIN;

CREATE SCHEMA IF NOT EXISTS stg;
CREATE OR REPLACE FUNCTION stg.dk(d date) RETURNS int LANGUAGE sql IMMUTABLE
AS $$ SELECT coalesce(to_char(d,'YYYYMMDD')::int, -1) $$;
-- Midpoint of a budget range such as '300k-500k' or '1M-1.5M', in THB (NULL if unparseable)
CREATE OR REPLACE FUNCTION stg.thb(v text) RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
  SELECT (regexp_match(v, '^([0-9.]+)'))[1]::numeric
         * CASE upper(right(v,1)) WHEN 'K' THEN 1000 WHEN 'M' THEN 1000000 END $$;
CREATE OR REPLACE FUNCTION stg.budget_mid(r text) RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN r ~ '^[0-9.]+[kKmM]-[0-9.]+[kKmM]$'
              THEN (stg.thb(split_part(r,'-',1)) + stg.thb(split_part(r,'-',2))) / 2 END $$;
DROP TABLE IF EXISTS stg.run;
CREATE TABLE stg.run AS SELECT :'asof'::date AS asof, date_trunc('week', :'asof'::date)::date AS asof_week;

-- ===== DimDate (2025-2028): insert once, refresh calendar flags =====
INSERT INTO dim."DimDate"("DateKey","FullDate") VALUES (-1,'1900-01-01') ON CONFLICT DO NOTHING;
INSERT INTO dim."DimDate"
SELECT stg.dk(g.d), g.d, trim(to_char(g.d,'Day')), CASE WHEN extract(isodow from g.d) >= 6 THEN 'Y' ELSE 'N' END,
       date_trunc('week', g.d)::date, extract(month from g.d), trim(to_char(g.d,'Month')), to_char(g.d,'YYYY-MM'),
       extract(quarter from g.d), extract(year from g.d), extract(year from g.d) + 543,
       CASE WHEN extract(month from g.d) IN (11,12,1,2) THEN 'Y' ELSE 'N' END,
       -- approximation; a production build reads the lunar Khao Phansa / Ok Phansa dates per year
       CASE WHEN (extract(month from g.d) = 7 AND extract(day from g.d) >= 20) OR extract(month from g.d) IN (8,9)
                 OR (extract(month from g.d) = 10 AND extract(day from g.d) <= 17) THEN 'Y' ELSE 'N' END,
       CASE WHEN a.cd IS NOT NULL THEN 'Y' ELSE 'N' END, a.lvl
FROM (SELECT gs::date FROM generate_series('2025-01-01'::date, '2028-12-31'::date, interval '1 day') gs) g(d)
LEFT JOIN (SELECT "CalendarDate" cd,
                  (array['Low','Medium','High'])[max(array_position(array['Low','Medium','High'], "AuspiciousLevel"))] lvl
           FROM master."AuspiciousDate" GROUP BY 1) a ON a.cd = g.d
ON CONFLICT ("DateKey") DO UPDATE SET "IsAuspicious" = EXCLUDED."IsAuspicious", "AuspiciousLevel" = EXCLUDED."AuspiciousLevel";

-- ===== DimPackage (upsert) =====
INSERT INTO dim."DimPackage"("PackageKey","PackageID","PackageFamily") VALUES (-1,-1,'Unknown') ON CONFLICT DO NOTHING;
INSERT INTO dim."DimPackage"("PackageID","PackageFamily","PackageType","ServiceFeeType","IsCurrentVersion")
SELECT "PackageID", "PackageName", "PackageType", "ServiceFeeType", CASE WHEN "EffectiveTo" IS NULL THEN 'Y' ELSE 'N' END
FROM master."Package"
ON CONFLICT ("PackageID") DO UPDATE SET "PackageFamily" = EXCLUDED."PackageFamily", "PackageType" = EXCLUDED."PackageType",
  "ServiceFeeType" = EXCLUDED."ServiceFeeType", "IsCurrentVersion" = EXCLUDED."IsCurrentVersion";

-- ===== DimEmployee (upsert) =====
INSERT INTO dim."DimEmployee"("EmployeeKey","EmployeeID","FullName") VALUES (-1,-1,'Unknown / none') ON CONFLICT DO NOTHING;
INSERT INTO dim."DimEmployee"("EmployeeID","FullName","RoleName","EmploymentType","EmployeeStatus")
SELECT e."EmployeeID", e."FirstName" || ' ' || e."LastName", r."RoleName", e."EmploymentType", e."Status"
FROM master."Employee" e JOIN master."EmployeeRole" r USING ("RoleID")
ON CONFLICT ("EmployeeID") DO UPDATE SET "FullName" = EXCLUDED."FullName", "RoleName" = EXCLUDED."RoleName",
  "EmploymentType" = EXCLUDED."EmploymentType", "EmployeeStatus" = EXCLUDED."EmployeeStatus";

-- ===== Staging =====
DROP TABLE IF EXISTS stg.chat, stg.inq, stg.staff, stg.staff_week;

-- First customer message, first staff reply after it (Bot ignored), and who replied
CREATE TABLE stg.chat AS
SELECT c."InquiryID", c.t_in, r."SentAt" AS t_staff, r."EmployeeID" AS responder
FROM (SELECT "InquiryID", min("SentAt") AS t_in FROM transaction."ChatMessage"
      WHERE "InquiryID" IS NOT NULL AND "SenderType" = 'Customer' GROUP BY 1) c
LEFT JOIN LATERAL (SELECT m."SentAt", m."EmployeeID" FROM transaction."ChatMessage" m
                   WHERE m."InquiryID" = c."InquiryID" AND m."SenderType" = 'Staff' AND m."SentAt" >= c.t_in
                   ORDER BY m."SentAt", m."ChatMessageID" LIMIT 1) r ON true;

CREATE TABLE stg.inq AS
SELECT x.*,
       CASE WHEN x.has_chat AND x.rt_min IS NULL THEN 'Not logged'
            WHEN x.rt_min IS NULL THEN 'In person'
            WHEN x.rt_min < 60 THEN '<1h' WHEN x.rt_min < 1440 THEN '1-24h' ELSE '>24h' END AS rt_band
FROM (
  SELECT i.*, ch."ChannelName", ch."ChannelType",
         c."InquiryID" IS NOT NULL AS has_chat, c.responder,
         CASE WHEN c."InquiryID" IS NOT NULL THEN floor(extract(epoch FROM c.t_staff - c.t_in) / 60)::int
              ELSE i."ResponseTimeMinutes" END AS rt_min,
         CASE WHEN e."EventID" IS NOT NULL THEN 'Booked' WHEN i."QuotedDate" IS NOT NULL THEN 'Quoted'
              WHEN i."ConsultationDate" IS NOT NULL THEN 'Consultation'
              WHEN i."ContactedDate" IS NOT NULL THEN 'Contacted' ELSE 'New' END AS furthest,
         e."EventID" AS ev_id, e."CreatedDate" AS ev_created,
         (e."EventID" IS NOT NULL AND e."Status" IS DISTINCT FROM 'Cancelled') AS converted
  FROM transaction."Inquiry" i
  LEFT JOIN master."MarketingChannel" ch USING ("ChannelID")
  LEFT JOIN transaction."Event" e ON e."InquiryID" = i."InquiryID"
  LEFT JOIN stg.chat c ON c."InquiryID" = i."InquiryID") x;

-- Staff hours split per assignment
CREATE TABLE stg.staff AS
SELECT a."EventEmployeeAssignmentID", a."EventID", a."EmployeeID", e."Status" AS event_status, a."WorkloadHours" AS hrs,
       least(a."WorkloadHours", 12)                     AS day_hrs,
       a."WorkloadHours" - least(a."WorkloadHours", 12) AS plan_hrs,
       least(date_trunc('week', coalesce(a."AssignedDate", e."CreatedDate")), date_trunc('week', e."EventDate"))::date AS aw,
       date_trunc('week', e."EventDate")::date AS ew
FROM transaction."EventEmployeeAssignment" a JOIN transaction."Event" e USING ("EventID");

CREATE TABLE stg.staff_week AS
SELECT s."EmployeeID", s."EventID", w::date AS wk, s.plan_hrs / greatest((s.ew - s.aw) / 7, 1) AS plan_h, 0::numeric AS day_h
FROM stg.staff s, generate_series(s.aw, CASE WHEN s.ew > s.aw THEN s.ew - 7 ELSE s.ew END, interval '7 days') w
WHERE s.plan_hrs > 0 AND s.event_status IS DISTINCT FROM 'Cancelled'
UNION ALL
SELECT s."EmployeeID", s."EventID", s.ew, 0, s.day_hrs
FROM stg.staff s WHERE s.day_hrs > 0 AND s.event_status IS DISTINCT FROM 'Cancelled';

-- ===== FactInquiry + FactEventBooking (full refresh) =====
TRUNCATE fact."FactEventBooking", fact."FactInquiry" RESTART IDENTITY;

INSERT INTO fact."FactInquiry"("InquiryID","ConvertedEventID","InquiryDateKey","ConsultationDateKey","QuotedDateKey","RequestedEventDateKey",
  "IndicativePackageKey","FirstResponderEmployeeKey","ChannelName","ChannelType","InquiryStatus","FurthestStage","BudgetRange","LostReason",
  "ClosedBy","ResponseTimeBand","InquiryCount","IsConsulted","IsQuoted","IsConverted","IsLost","IsOpen","QuotedAmount","QuoteShareOfBudget",
  "ResponseTimeMinutes","DaysToConsultation","DaysConsultationToQuote","DaysQuoteToBooking","DaysToConvert")
SELECT i."InquiryID", i.ev_id,
       stg.dk(i."InquiryDate"::date), stg.dk(i."ConsultationDate"::date), stg.dk(i."QuotedDate"::date), stg.dk(i."RequestedEventDate"),
       coalesce(dp."PackageKey",-1), coalesce(de."EmployeeKey",-1),
       coalesce(i."ChannelName",'N/A'), coalesce(i."ChannelType",'N/A'), coalesce(i."Status",'N/A'), i.furthest,
       coalesce(i."BudgetRange",'N/A'), coalesce(i."LostReason",'N/A'), coalesce(i."ClosedBy",'N/A'), i.rt_band,
       1, (i."ConsultationDate" IS NOT NULL)::int, (i."QuotedDate" IS NOT NULL)::int, i.converted::int,
       coalesce(i."Status" = 'Lost', false)::int,
       (i.ev_id IS NULL AND coalesce(i."Status",'New') NOT IN ('Converted','Lost'))::int,
       i."QuotedAmount", round(i."QuotedAmount" / nullif(stg.budget_mid(i."BudgetRange"),0), 4), i.rt_min,
       i."ConsultationDate"::date - i."InquiryDate"::date,
       i."QuotedDate"::date - i."ConsultationDate"::date,
       CASE WHEN i.converted THEN i.ev_created::date - i."QuotedDate"::date END,
       CASE WHEN i.converted THEN i.ev_created::date - i."InquiryDate"::date END
FROM stg.inq i
LEFT JOIN dim."DimPackage"  dp ON dp."PackageID"  = i."IndicativePackageID"
LEFT JOIN dim."DimEmployee" de ON de."EmployeeID" = i.responder;

INSERT INTO fact."FactEventBooking"("EventID","InquiryID","BookingDateKey","EventDateKey","PackageKey","EventStatus","ComplexityLevel",
  "EventCount","IsCancelled","GuestCount","AgreedPackagePrice","BookingLeadTimeDays","StaffCount","PlanningHours","EventDayHours","TotalStaffHours")
SELECT e."EventID", e."InquiryID", stg.dk(e."CreatedDate"::date), stg.dk(e."EventDate"), coalesce(dp."PackageKey",-1),
       coalesce(e."Status",'N/A'), coalesce(e."ComplexityLevel",'N/A'), 1, coalesce(e."Status" = 'Cancelled', false)::int,
       e."GuestCount", e."AgreedPackagePrice", e."EventDate" - e."CreatedDate"::date,
       coalesce(s.n,0), coalesce(s.ph,0), coalesce(s.dh,0), coalesce(s.h,0)
FROM transaction."Event" e
LEFT JOIN dim."DimPackage" dp ON dp."PackageID" = e."PackageID"
LEFT JOIN (SELECT "EventID", count(DISTINCT "EmployeeID") n, sum(plan_hrs) ph, sum(day_hrs) dh, sum(hrs) h
           FROM stg.staff GROUP BY 1) s USING ("EventID");

-- >>> WEEKLY: rebuild weeks from the as-of week onwards; earlier weeks stay frozen
DELETE FROM fact."FactEmployeeWeeklyWorkload" w
USING dim."DimDate" d WHERE d."DateKey" = w."WeekStartDateKey" AND d."FullDate" >= (SELECT asof_week FROM stg.run);

INSERT INTO fact."FactEmployeeWeeklyWorkload"("WeekStartDateKey","EmployeeKey","PlanningHours","EventDayHours","TotalAssignedHours",
  "StandardWeeklyHours","OvertimeHours","UtilizationRate","ActiveEvents","MaxConcurrentEvents","IsOverCapacity")
WITH weeks AS (
       SELECT generate_series((SELECT min(wk) FROM stg.staff_week), (SELECT max(wk) FROM stg.staff_week), interval '7 days')::date wk),
     agg AS (
       SELECT "EmployeeID", wk, sum(plan_h) ph, sum(day_h) dh, count(DISTINCT "EventID") act FROM stg.staff_week GROUP BY 1,2),
     grid AS (            -- active employees every week from their hire week; others only in weeks they worked
       SELECT w.wk, e."EmployeeID", e."StandardWeeklyHours", e."MaxConcurrentEvents"
       FROM weeks w JOIN master."Employee" e
         ON w.wk >= date_trunc('week', coalesce(e."HireDate",'1900-01-01'))::date
        AND (e."Status" = 'Active' OR EXISTS (SELECT 1 FROM agg a WHERE a."EmployeeID" = e."EmployeeID" AND a.wk = w.wk)))
SELECT stg.dk(g.wk), de."EmployeeKey",
       round(coalesce(a.ph,0),2), round(coalesce(a.dh,0),2), round(coalesce(a.ph,0) + coalesce(a.dh,0),2),
       g."StandardWeeklyHours",
       round(greatest(coalesce(a.ph,0) + coalesce(a.dh,0) - g."StandardWeeklyHours", 0),2),
       round((coalesce(a.ph,0) + coalesce(a.dh,0)) / nullif(g."StandardWeeklyHours",0),4),
       coalesce(a.act,0), g."MaxConcurrentEvents",
       CASE WHEN g."MaxConcurrentEvents" IS NOT NULL AND coalesce(a.act,0) > g."MaxConcurrentEvents" THEN 1 ELSE 0 END
FROM grid g
JOIN dim."DimEmployee" de ON de."EmployeeID" = g."EmployeeID"
LEFT JOIN agg a ON a."EmployeeID" = g."EmployeeID" AND a.wk = g.wk
WHERE NOT EXISTS (SELECT 1 FROM fact."FactEmployeeWeeklyWorkload" f
                  WHERE f."EmployeeKey" = de."EmployeeKey" AND f."WeekStartDateKey" = stg.dk(g.wk));
-- <<< WEEKLY

COMMIT;
