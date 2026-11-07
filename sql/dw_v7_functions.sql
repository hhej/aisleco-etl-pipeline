-- =====================================================================
-- DW v7: helper functions, landing tables and work schema (run after the v7 DDL)
-- =====================================================================
SET client_min_messages = warning;

-- Date -> DateKey (YYYYMMDD), -1 when unknown. Same rule as DW v6.
CREATE OR REPLACE FUNCTION etl.dk(d date) RETURNS int LANGUAGE sql IMMUTABLE
AS $$ SELECT coalesce(to_char(d,'YYYYMMDD')::int, -1) $$;

-- '500k' -> 500000, '1.5M' -> 1500000
CREATE OR REPLACE FUNCTION etl.thb(v text) RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
  SELECT (regexp_match(v, '^([0-9.]+)'))[1]::numeric
         * CASE upper(right(v,1)) WHEN 'K' THEN 1000 WHEN 'M' THEN 1000000 END $$;

-- Midpoint of a budget range such as '300k-500k' or '1M-1.5M' in THB, NULL if it cannot be parsed
CREATE OR REPLACE FUNCTION etl.budget_mid(r text) RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN r ~ '^[0-9.]+[kKmM]-[0-9.]+[kKmM]$'
              THEN (etl.thb(split_part(r,'-',1)) + etl.thb(split_part(r,'-',2))) / 2 END $$;

-- Landing (delta) tables: what one extract delivered, before checks. UNLOGGED because
-- they are rebuilt from the landed CSV files whenever needed.
CREATE SCHEMA IF NOT EXISTS land;
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['MarketingChannel','Package','EmployeeRole','Employee','AuspiciousDate',
                           'Inquiry','ChatMessage','Event','EventEmployeeAssignment']
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS land.%I, land.%I', t, t || '_Keys');
    EXECUTE format('CREATE UNLOGGED TABLE land.%I (LIKE stg.%I)', t, t);
    EXECUTE format('ALTER TABLE land.%I DROP COLUMN "_RunID"', t);
    -- Pending: the row waits in the CRM backlog (batch cap), so it may be missing from staging
    EXECUTE format('CREATE UNLOGGED TABLE land.%I ("Key" int PRIMARY KEY, "Pending" boolean NOT NULL DEFAULT false)', t || '_Keys');
  END LOOP;
END $$;

-- Work tables of the transform steps live here and are rebuilt by each step.
CREATE SCHEMA IF NOT EXISTS work;
CREATE TABLE IF NOT EXISTS work.run_ctx (run_id int, asof date, asof_week date);

-- Indexes that the transform steps rely on (first-reply lookup, assignments per event)
CREATE INDEX IF NOT EXISTS ix_stg_chat_inq ON stg."ChatMessage" ("InquiryID", "SentAt");
CREATE INDEX IF NOT EXISTS ix_stg_eea_event ON stg."EventEmployeeAssignment" ("EventID");
CREATE INDEX IF NOT EXISTS ix_stg_event_inq ON stg."Event" ("InquiryID");

-- ---------------------------------------------------------------------
-- Business rules shared by two facts, defined once (DW v6 rules, unchanged)
-- ---------------------------------------------------------------------
-- Staff hours per assignment: up to 12 h = wedding-day work in the wedding week,
-- the rest = planning, spread from the assignment week (booking date if missing)
CREATE OR REPLACE VIEW work.v_staff_assignment AS
SELECT a."EventEmployeeAssignmentID", a."EventID", a."EmployeeID", e."Status" AS event_status, a."WorkloadHours" AS hrs,
       least(a."WorkloadHours", 12)                     AS day_hrs,
       a."WorkloadHours" - least(a."WorkloadHours", 12) AS plan_hrs,
       least(date_trunc('week', coalesce(a."AssignedDate", e."CreatedDate")), date_trunc('week', e."EventDate"))::date AS aw,
       date_trunc('week', e."EventDate")::date AS ew
FROM stg."EventEmployeeAssignment" a JOIN stg."Event" e USING ("EventID");

-- ... spread over weeks: planning evenly from the assignment week to the week before the
-- wedding (wedding week if assigned that week or later); cancelled events excluded
CREATE OR REPLACE VIEW work.v_staff_week AS
SELECT s."EmployeeID", s."EventID", w::date AS wk, s.plan_hrs / greatest((s.ew - s.aw) / 7, 1) AS plan_h, 0::numeric AS day_h
FROM work.v_staff_assignment s,
     generate_series(s.aw, CASE WHEN s.ew > s.aw THEN s.ew - 7 ELSE s.ew END, interval '7 days') w
WHERE s.plan_hrs > 0 AND s.event_status IS DISTINCT FROM 'Cancelled'
UNION ALL
SELECT s."EmployeeID", s."EventID", s.ew, 0, s.day_hrs
FROM work.v_staff_assignment s WHERE s.day_hrs > 0 AND s.event_status IS DISTINCT FROM 'Cancelled';
