-- Step fact_weekly_workload (transform). Employee x week grid with hours, overtime and
-- active events, using TODAY's standard hours (utilization is computed in
-- mart.v_weekly_workload, not stored). The load step applies the
-- freeze: only weeks from the as-of week onwards are written over; earlier weeks are
-- inserted only if missing, so they keep the standard hours that applied at the time.
DROP TABLE IF EXISTS work."FactEmployeeWeeklyWorkload";

CREATE TABLE work."FactEmployeeWeeklyWorkload" AS
WITH sw AS (SELECT * FROM work.v_staff_week),
     weeks AS (
       SELECT generate_series((SELECT min(wk) FROM sw), (SELECT max(wk) FROM sw), interval '7 days')::date wk),
     agg AS (
       SELECT "EmployeeID", wk, sum(plan_h) ph, sum(day_h) dh, count(DISTINCT "EventID") act FROM sw GROUP BY 1,2),
     grid AS (            -- active employees every week from their hire week; others only in weeks they worked
       SELECT w.wk, e."EmployeeID", e."StandardWeeklyHours", e."MaxConcurrentEvents"
       FROM weeks w JOIN stg."Employee" e
         ON w.wk >= date_trunc('week', coalesce(e."HireDate",'1900-01-01'))::date
        AND (e."Status" = 'Active' OR EXISTS (SELECT 1 FROM agg a WHERE a."EmployeeID" = e."EmployeeID" AND a.wk = w.wk)))
SELECT etl.dk(g.wk)                                                                  AS "WeekStartDateKey",
       de."EmployeeKey"                                                              AS "EmployeeKey",
       round(coalesce(a.ph,0),2)                                                     AS "PlanningHours",
       round(coalesce(a.dh,0),2)                                                     AS "EventDayHours",
       round(coalesce(a.ph,0) + coalesce(a.dh,0),2)                                  AS "TotalAssignedHours",
       g."StandardWeeklyHours"                                                       AS "StandardWeeklyHours",
       round(greatest(coalesce(a.ph,0) + coalesce(a.dh,0) - g."StandardWeeklyHours", 0),2) AS "OvertimeHours",
       coalesce(a.act,0)                                                             AS "ActiveEvents",
       g."MaxConcurrentEvents"                                                       AS "MaxConcurrentEvents",
       CASE WHEN g."MaxConcurrentEvents" IS NOT NULL AND coalesce(a.act,0) > g."MaxConcurrentEvents" THEN 1 ELSE 0 END AS "IsOverCapacity"
FROM grid g
JOIN dim."DimEmployee" de ON de."EmployeeID" = g."EmployeeID"
LEFT JOIN agg a ON a."EmployeeID" = g."EmployeeID" AND a.wk = g.wk;

SELECT count(*) AS rows_built FROM work."FactEmployeeWeeklyWorkload";
