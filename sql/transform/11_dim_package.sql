-- Step dim_package. Type 1 upsert by PackageID (each OLTP PackageID = one price version).
INSERT INTO dim."DimPackage"("PackageKey","PackageID","PackageFamily") VALUES (-1,-1,'Unknown') ON CONFLICT DO NOTHING;

WITH up AS (
INSERT INTO dim."DimPackage" AS t ("PackageID","PackageFamily","PackageType","ServiceFeeType","IsCurrentVersion")
SELECT "PackageID", "PackageName", "PackageType", "ServiceFeeType", CASE WHEN "EffectiveTo" IS NULL THEN 'Y' ELSE 'N' END
FROM stg."Package"
ON CONFLICT ("PackageID") DO UPDATE SET "PackageFamily" = EXCLUDED."PackageFamily", "PackageType" = EXCLUDED."PackageType",
  "ServiceFeeType" = EXCLUDED."ServiceFeeType", "IsCurrentVersion" = EXCLUDED."IsCurrentVersion"
  WHERE (t."PackageFamily", t."PackageType", t."ServiceFeeType", t."IsCurrentVersion")
        IS DISTINCT FROM (EXCLUDED."PackageFamily", EXCLUDED."PackageType", EXCLUDED."ServiceFeeType", EXCLUDED."IsCurrentVersion")
RETURNING (xmax = 0) AS ins)
SELECT count(*) FILTER (WHERE ins) AS inserted, count(*) FILTER (WHERE NOT ins) AS updated FROM up;
