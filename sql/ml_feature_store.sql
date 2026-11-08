-- =====================================================================
-- Feature store and model registry for the conversion model (Chapter 7).
-- Runs after the DW v7 DDL (it reads stg, the Silver zone, and etl.budget_mid).
--
--   feature.inquiry_features(ids, cutoffs)  the one feature definition, point in time
--   feature."InquiryFeatures"               offline store: what each open inquiry looked like each day
--   feature."InquiryLabel"                  outcome of closed inquiries (booked or lost) and when
--   ml."ModelVersion"                       model registry + metadata store
--   ml."InquiryScore"                       daily batch predictions
--   ml."DriftCheck"                         monitoring results
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS feature;
CREATE SCHEMA IF NOT EXISTS ml;

-- Features of inquiry ids[k] as it looked just before cutoffs[k], using only rows dated
-- before the cutoff. Training (past cutoffs) and daily scoring (today) call the same code,
-- so the model is always fed features computed the same way.
-- Only inquiries still open at the cutoff are returned: received before it, not yet booked
-- and not yet closed. Event timestamps (messages, consultation, quote, booking, closing)
-- are taken as of the cutoff; descriptive fields (channel, budget, package, wedding date)
-- are the latest values in Silver.
CREATE OR REPLACE FUNCTION feature.inquiry_features(p_ids int[], p_cutoffs timestamp[])
RETURNS TABLE ("InquiryID" int, "AsOf" timestamp, "AgeDays" int, "Channel" text, "BudgetRange" text,
               "PackageType" text, "HasRequestedDate" int, "LeadDays" int, "PeakDate" int,
               "ResponseMinutes" int, "HasChat" int, "CoupleMsgs" int, "CoupleMsgs24h" int,
               "HoursSilent" numeric, "Consulted" int, "Quoted" int, "QuoteShare" numeric)
LANGUAGE sql STABLE AS $$
SELECT i."InquiryID", s.cutoff,
       s.cutoff::date - i."InquiryDate"::date,
       coalesce(ch."ChannelName", 'Unknown')::text,
       coalesce(i."BudgetRange", 'Unknown')::text,
       coalesce(p."PackageType", 'None')::text,
       (i."RequestedEventDate" IS NOT NULL)::int,
       i."RequestedEventDate" - i."InquiryDate"::date,
       coalesce(extract(month FROM i."RequestedEventDate") IN (11, 12, 1, 2)
                OR EXISTS (SELECT 1 FROM stg."AuspiciousDate" a WHERE a."CalendarDate" = i."RequestedEventDate"), false)::int,
       -- first staff reply minus first couple message (as in FactInquiry), once the reply exists
       CASE WHEN m.t_in IS NOT NULL THEN floor(extract(epoch FROM r.t_staff - m.t_in) / 60)::int
            WHEN i."ContactedDate" < s.cutoff THEN i."ResponseTimeMinutes" END,
       (m.t_in IS NOT NULL)::int,
       coalesce(m.n, 0)::int,
       coalesce(m.n24, 0)::int,
       round((extract(epoch FROM s.cutoff - m.t_last) / 3600)::numeric, 1),
       coalesce(i."ConsultationDate" < s.cutoff, false)::int,
       coalesce(i."QuotedDate" < s.cutoff, false)::int,
       CASE WHEN i."QuotedDate" < s.cutoff
            THEN round(i."QuotedAmount" / nullif(etl.budget_mid(i."BudgetRange"), 0), 4) END
FROM unnest(p_ids, p_cutoffs) AS s(id, cutoff)
JOIN stg."Inquiry" i ON i."InquiryID" = s.id
LEFT JOIN stg."MarketingChannel" ch ON ch."ChannelID" = i."ChannelID"
LEFT JOIN stg."Package" p ON p."PackageID" = i."IndicativePackageID"
LEFT JOIN stg."Event" e ON e."InquiryID" = i."InquiryID"
LEFT JOIN LATERAL (
    SELECT min(c."SentAt") AS t_in, max(c."SentAt") AS t_last, count(*) AS n,
           count(*) FILTER (WHERE c."SentAt" < i."InquiryDate" + interval '1 day') AS n24
    FROM stg."ChatMessage" c
    WHERE c."InquiryID" = i."InquiryID" AND c."SenderType" = 'Customer' AND c."SentAt" < s.cutoff
    HAVING count(*) > 0) m ON true
LEFT JOIN LATERAL (
    SELECT min(c."SentAt") AS t_staff FROM stg."ChatMessage" c
    WHERE c."InquiryID" = i."InquiryID" AND c."SenderType" = 'Staff'
      AND c."SentAt" >= m.t_in AND c."SentAt" < s.cutoff) r ON true
WHERE i."InquiryDate" < s.cutoff
  AND (e."CreatedDate" IS NULL OR e."CreatedDate" >= s.cutoff)
  AND (i."ClosedDate" IS NULL OR i."ClosedDate" >= s.cutoff)
$$;

-- Offline store: one row per open inquiry per scoring day (filled by `python -m ml features`)
CREATE TABLE IF NOT EXISTS feature."InquiryFeatures" AS
SELECT * FROM feature.inquiry_features('{}'::int[], '{}'::timestamp[]) WITH NO DATA;
DO $$ BEGIN
  ALTER TABLE feature."InquiryFeatures" ADD PRIMARY KEY ("InquiryID", "AsOf");
EXCEPTION WHEN invalid_table_definition THEN NULL; END $$;

-- Label: did the inquiry turn into a booking? Booked = an event was created (a later
-- cancellation does not change the sales outcome). Only closed inquiries have a label.
CREATE OR REPLACE VIEW feature."InquiryLabel" AS
SELECT i."InquiryID", i."InquiryDate", (e."EventID" IS NOT NULL)::int AS "Booked",
       coalesce(e."CreatedDate", i."ClosedDate") AS "ClosedAt"
FROM stg."Inquiry" i
LEFT JOIN stg."Event" e ON e."InquiryID" = i."InquiryID"
WHERE e."EventID" IS NOT NULL OR i."Status" = 'Lost';

-- Registry and metadata: every training run, promoted or not, with what it was trained on
-- and how it scored. The model itself is stored here too (pickled scikit-learn pipeline).
CREATE TABLE IF NOT EXISTS ml."ModelVersion" (
  "Version" int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  "TrainedAt" timestamp NOT NULL DEFAULT now(),
  "AsOfDate" date NOT NULL,                 -- data known up to this date
  "Algorithm" text NOT NULL,
  "TrainWindow" text NOT NULL,
  "TestWindow" text NOT NULL,
  "TrainRows" int NOT NULL,
  "TestRows" int NOT NULL,
  "Metrics" jsonb NOT NULL,                 -- test metrics of every candidate and the baseline
  "Features" jsonb NOT NULL,
  "TrainingStats" jsonb NOT NULL,           -- feature distributions at training, for drift checks
  "Thresholds" jsonb NOT NULL,              -- score cut-offs for the High / Medium / Low bands
  "Status" text NOT NULL CHECK ("Status" IN ('production', 'retired', 'rejected')),
  "Decision" text NOT NULL,
  "Artifact" bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS ml."InquiryScore" (
  "InquiryID" int NOT NULL,
  "AsOfDate" date NOT NULL,
  "Version" int NOT NULL REFERENCES ml."ModelVersion" ("Version"),
  "Score" numeric(6,4) NOT NULL,
  "Band" text NOT NULL,
  PRIMARY KEY ("InquiryID", "AsOfDate")
);

CREATE TABLE IF NOT EXISTS ml."DriftCheck" (
  "AsOfDate" date NOT NULL,
  "Version" int NOT NULL REFERENCES ml."ModelVersion" ("Version"),
  "Check" text NOT NULL,                    -- e.g. 'PSI Channel', 'missing ResponseMinutes', 'live AUC'
  "Value" numeric,
  "Status" text NOT NULL CHECK ("Status" IN ('ok', 'warning', 'alert', 'n/a')),
  "Detail" text,
  PRIMARY KEY ("AsOfDate", "Version", "Check")
);
