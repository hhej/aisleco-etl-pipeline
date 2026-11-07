-- Step fact_inquiry (transform). Builds work."FactInquiry" = the target rows as they
-- should be now, from staging. The load step merges it into fact."FactInquiry".
-- Rules are DW v6 rules. The outcome labels go to the junk dimension DimInquiryOutcome,
-- which this step also loads (new label combinations only; keys never change).
DROP TABLE IF EXISTS work.chat, work.inq, work."FactInquiry";

-- First customer message, first staff reply after it (Bot ignored), and who replied
CREATE TABLE work.chat AS
SELECT c."InquiryID", c.t_in, r."SentAt" AS t_staff, r."EmployeeID" AS responder
FROM (SELECT "InquiryID", min("SentAt") AS t_in FROM stg."ChatMessage"
      WHERE "InquiryID" IS NOT NULL AND "SenderType" = 'Customer' GROUP BY 1) c
LEFT JOIN LATERAL (SELECT m."SentAt", m."EmployeeID" FROM stg."ChatMessage" m
                   WHERE m."InquiryID" = c."InquiryID" AND m."SenderType" = 'Staff' AND m."SentAt" >= c.t_in
                   ORDER BY m."SentAt", m."ChatMessageID" LIMIT 1) r ON true;

CREATE TABLE work.inq AS
SELECT x.*,
       CASE WHEN x.has_chat AND x.rt_min IS NULL THEN 'Not logged'
            WHEN x.rt_min IS NULL THEN 'In person'
            WHEN x.rt_min < 60 THEN '<1h' WHEN x.rt_min < 1440 THEN '1-24h' ELSE '>24h' END AS rt_band,
       -- order keeps the old flags: Open = no event and not Converted/Lost (IsOpen), Lost = Status 'Lost' (IsLost)
       CASE WHEN x.converted THEN 'Booked' WHEN x."Status" = 'Lost' THEN 'Lost'
            WHEN x.ev_id IS NOT NULL THEN 'Cancelled' WHEN x."Status" = 'Converted' THEN 'Converted, no event'
            ELSE 'Open' END AS outcome
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
  FROM stg."Inquiry" i
  LEFT JOIN stg."MarketingChannel" ch USING ("ChannelID")
  LEFT JOIN stg."Event" e ON e."InquiryID" = i."InquiryID"
  LEFT JOIN work.chat c ON c."InquiryID" = i."InquiryID") x;

INSERT INTO dim."DimInquiryOutcome" ("Outcome","FurthestStage","ClosedBy","LostReason")
SELECT DISTINCT outcome, furthest, coalesce("ClosedBy",'N/A'), coalesce("LostReason",'N/A') FROM work.inq
ON CONFLICT DO NOTHING;

CREATE TABLE work."FactInquiry" AS
SELECT i."InquiryID"                                                  AS "InquiryID",
       i.ev_id                                                        AS "ConvertedEventID",
       etl.dk(i."InquiryDate"::date)                                  AS "InquiryDateKey",
       etl.dk(i."ConsultationDate"::date)                             AS "ConsultationDateKey",
       etl.dk(i."QuotedDate"::date)                                   AS "QuotedDateKey",
       etl.dk(i."RequestedEventDate")                                 AS "RequestedEventDateKey",
       coalesce(dp."PackageKey",-1)                                   AS "IndicativePackageKey",
       coalesce(de."EmployeeKey",-1)                                  AS "FirstResponderEmployeeKey",
       o."OutcomeKey"                                                 AS "OutcomeKey",
       coalesce(i."ChannelName",'N/A')                                AS "ChannelName",
       coalesce(i."ChannelType",'N/A')                                AS "ChannelType",
       coalesce(i."BudgetRange",'N/A')                                AS "BudgetRange",
       i.rt_band                                                      AS "ResponseTimeBand",
       1                                                              AS "InquiryCount",
       (i."ConsultationDate" IS NOT NULL)::int                        AS "IsConsulted",
       (i."QuotedDate" IS NOT NULL)::int                              AS "IsQuoted",
       i.converted::int                                               AS "IsConverted",
       i."QuotedAmount"                                               AS "QuotedAmount",
       etl.budget_mid(i."BudgetRange")                                AS "BudgetMidpoint",
       i.rt_min                                                       AS "ResponseTimeMinutes",
       i."ConsultationDate"::date - i."InquiryDate"::date             AS "DaysToConsultation",
       i."QuotedDate"::date - i."ConsultationDate"::date              AS "DaysConsultationToQuote",
       CASE WHEN i.converted THEN i.ev_created::date - i."QuotedDate"::date END  AS "DaysQuoteToBooking",
       CASE WHEN i.converted THEN i.ev_created::date - i."InquiryDate"::date END AS "DaysToConvert"
FROM work.inq i
LEFT JOIN dim."DimPackage"  dp ON dp."PackageID"  = i."IndicativePackageID"
LEFT JOIN dim."DimEmployee" de ON de."EmployeeID" = i.responder
JOIN dim."DimInquiryOutcome" o ON (o."Outcome", o."FurthestStage", o."ClosedBy", o."LostReason")
                                = (i.outcome, i.furthest, coalesce(i."ClosedBy",'N/A'), coalesce(i."LostReason",'N/A'));

SELECT count(*) AS rows_built FROM work."FactInquiry";
