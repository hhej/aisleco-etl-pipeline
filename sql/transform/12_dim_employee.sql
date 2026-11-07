-- Step dim_employee. Type 1 upsert by EmployeeID. Capacity numbers are NOT here:
-- they are copied into each weekly workload row (history without SCD2).
INSERT INTO dim."DimEmployee"("EmployeeKey","EmployeeID","FullName") VALUES (-1,-1,'Unknown / none') ON CONFLICT DO NOTHING;

WITH up AS (
INSERT INTO dim."DimEmployee" AS t ("EmployeeID","FullName","RoleName","EmploymentType","EmployeeStatus")
SELECT e."EmployeeID", e."FirstName" || ' ' || e."LastName", r."RoleName", e."EmploymentType", e."Status"
FROM stg."Employee" e LEFT JOIN stg."EmployeeRole" r USING ("RoleID")
ON CONFLICT ("EmployeeID") DO UPDATE SET "FullName" = EXCLUDED."FullName", "RoleName" = EXCLUDED."RoleName",
  "EmploymentType" = EXCLUDED."EmploymentType", "EmployeeStatus" = EXCLUDED."EmployeeStatus"
  WHERE (t."FullName", t."RoleName", t."EmploymentType", t."EmployeeStatus")
        IS DISTINCT FROM (EXCLUDED."FullName", EXCLUDED."RoleName", EXCLUDED."EmploymentType", EXCLUDED."EmployeeStatus")
RETURNING (xmax = 0) AS ins)
SELECT count(*) FILTER (WHERE ins) AS inserted, count(*) FILTER (WHERE NOT ins) AS updated FROM up;
