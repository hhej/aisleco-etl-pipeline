-- Step fact_event_booking (transform). One row per booked wedding with staff hours.
DROP TABLE IF EXISTS work."FactEventBooking";

CREATE TABLE work."FactEventBooking" AS
SELECT e."EventID"                                   AS "EventID",
       e."InquiryID"                                 AS "InquiryID",
       etl.dk(e."CreatedDate"::date)                 AS "BookingDateKey",
       etl.dk(e."EventDate")                         AS "EventDateKey",
       coalesce(dp."PackageKey",-1)                  AS "PackageKey",
       coalesce(e."Status",'N/A')                    AS "EventStatus",
       coalesce(e."ComplexityLevel",'N/A')           AS "ComplexityLevel",
       1                                             AS "EventCount",
       coalesce(e."Status" = 'Cancelled', false)::int AS "IsCancelled",
       e."GuestCount"                                AS "GuestCount",
       e."AgreedPackagePrice"                        AS "AgreedPackagePrice",
       e."EventDate" - e."CreatedDate"::date         AS "BookingLeadTimeDays",
       coalesce(s.n,0)                               AS "StaffCount",
       coalesce(s.ph,0)                              AS "PlanningHours",
       coalesce(s.dh,0)                              AS "EventDayHours",
       coalesce(s.h,0)                               AS "TotalStaffHours"
FROM stg."Event" e
LEFT JOIN dim."DimPackage" dp ON dp."PackageID" = e."PackageID"
LEFT JOIN (SELECT "EventID", count(DISTINCT "EmployeeID") n, sum(plan_hrs) ph, sum(day_hrs) dh, sum(hrs) h
           FROM work.v_staff_assignment GROUP BY 1) s USING ("EventID");

SELECT count(*) AS rows_built FROM work."FactEventBooking";
