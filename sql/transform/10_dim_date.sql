-- Step dim_date. DimDate 2025-2028: calendar inserted once; auspicious flags refreshed
-- from stg.AuspiciousDate (Thai + Chinese entries merged, highest level kept).
INSERT INTO dim."DimDate"("DateKey","FullDate") VALUES (-1,'1900-01-01') ON CONFLICT DO NOTHING;

WITH up AS (
INSERT INTO dim."DimDate" AS t
SELECT etl.dk(g.d), g.d, trim(to_char(g.d,'Day')), CASE WHEN extract(isodow from g.d) >= 6 THEN 'Y' ELSE 'N' END,
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
           FROM stg."AuspiciousDate" GROUP BY 1) a ON a.cd = g.d
ON CONFLICT ("DateKey") DO UPDATE SET "IsAuspicious" = EXCLUDED."IsAuspicious", "AuspiciousLevel" = EXCLUDED."AuspiciousLevel"
  WHERE (t."IsAuspicious", t."AuspiciousLevel") IS DISTINCT FROM (EXCLUDED."IsAuspicious", EXCLUDED."AuspiciousLevel")
RETURNING (xmax = 0) AS ins)
SELECT count(*) FILTER (WHERE ins) AS inserted, count(*) FILTER (WHERE NOT ins) AS updated FROM up;
