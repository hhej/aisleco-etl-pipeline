-- =====================================================================
-- OLTP v5: support for incremental extraction (run after the v5 DDL)
--   * trigger: every UPDATE stamps "UpdatedAt" = now(), so the application
--     cannot forget it (inserts get it from the column default)
--   * index on "UpdatedAt" for the 9 tables the ETL reads, so the nightly
--     extract is an index range scan instead of a full scan of the CRM
-- Kept out of the dbdiagram file because dbdiagram cannot import functions.
-- =====================================================================
SET client_min_messages = warning;
CREATE OR REPLACE FUNCTION public.set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW."UpdatedAt" := now();
  RETURN NEW;
END $$;

DO $$
DECLARE t record;
BEGIN
  FOR t IN SELECT table_schema, table_name FROM information_schema.tables
           WHERE table_schema IN ('master', 'transaction') AND table_type = 'BASE TABLE'
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_updated_at ON %I.%I', t.table_schema, t.table_name);
    EXECUTE format('CREATE TRIGGER trg_updated_at BEFORE UPDATE ON %I.%I
                    FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()', t.table_schema, t.table_name);
  END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS ix_marketingchannel_upd ON master."MarketingChannel" ("UpdatedAt");
CREATE INDEX IF NOT EXISTS ix_package_upd          ON master."Package" ("UpdatedAt");
CREATE INDEX IF NOT EXISTS ix_employeerole_upd     ON master."EmployeeRole" ("UpdatedAt");
CREATE INDEX IF NOT EXISTS ix_employee_upd         ON master."Employee" ("UpdatedAt");
CREATE INDEX IF NOT EXISTS ix_auspiciousdate_upd   ON master."AuspiciousDate" ("UpdatedAt");
CREATE INDEX IF NOT EXISTS ix_inquiry_upd          ON transaction."Inquiry" ("UpdatedAt");
CREATE INDEX IF NOT EXISTS ix_chatmessage_upd      ON transaction."ChatMessage" ("UpdatedAt");
CREATE INDEX IF NOT EXISTS ix_event_upd            ON transaction."Event" ("UpdatedAt");
CREATE INDEX IF NOT EXISTS ix_eea_upd              ON transaction."EventEmployeeAssignment" ("UpdatedAt");
