"manufacturer/make" "Cleaned Engine Number"model

select count(*) from public.samechassisno_differregno

select count(*) from public.cleanchassisengine_basiccleaned_appended_base_and_pr

select count(*) from public.cleanchassisengine_samechassisno_differregno

SELECT *
FROM public.dupclean_basiccleaned_appended_base_and_pr
WHERE "Cleaned Reg no" IN (
    SELECT "Cleaned Reg no"
    FROM public.dupclean_basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Reg no", model
    HAVING COUNT(DISTINCT "Cleaned Chassis Number") > 1
)
--And "Cleaned Reg no" <> '' And "Cleaned Reg no" <> 'new'
ORDER BY "Cleaned Reg no"

SELECT *
FROM public.cleanchassisengine_basiccleaned_appended_base_and_pr
WHERE "Cleaned Reg no" IN (
    SELECT "Cleaned Reg no"
    FROM public.cleanchassisengine_basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Reg no", model
    HAVING COUNT(DISTINCT "Cleaned Chassis Number") > 1
)
And "Cleaned Reg no" = '' --And "Cleaned Reg no" = 'new'
ORDER BY "Cleaned Reg no"

ap05de2266

e158908

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Reg no" = ''

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Reg no" = 'new'

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = 'e158908'

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Reg no" = ''

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Chassis Number" = '' or "Cleaned Chassis Number" = 'blank'

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = '' or "Cleaned Engine Number" = 'blank'


select * from public.dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = '' or "Cleaned Engine Number" = 'blank'

SELECT *
FROM public.dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr
WHERE "Cleaned Reg no" IN (
    SELECT "Cleaned Reg no"
    FROM public.dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Reg no", model
    HAVING COUNT(DISTINCT "Cleaned Chassis Number") > 1
)
And "Cleaned Reg no" = '' or "Cleaned Reg no" = 'new'
ORDER BY "Cleaned Reg no"

SELECT 
    "Cleaned Chassis Number", 
    "Cleaned Engine Number", 
	model,
    "policy start date", 
    "policy end date", 
    COUNT(*) AS record_count
FROM public.dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr
GROUP BY 
    "Cleaned Chassis Number", 
    "Cleaned Engine Number",
	model,
    "policy start date", 
    "policy end date"
HAVING COUNT(*) > 1;

SELECT 
    "Cleaned Chassis Number", 
    "Cleaned Engine Number", 
	model,
    "policy start date", 
    "policy end date", 
    COUNT(*) AS record_count
FROM public.dupclean_cleanchassisengine_samechassisno_differregno
GROUP BY 
    "Cleaned Chassis Number", 
    "Cleaned Engine Number",
	model,
    "policy start date", 
    "policy end date"
HAVING COUNT(*) > 1;

CREATE TABLE overallcleaned_chessis_engine AS
SELECT * FROM public.dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr
UNION ALL
SELECT * FROM public.dupclean_cleanchassisengine_samechassisno_differregno;

SELECT 
    "Cleaned Chassis Number", 
    "Cleaned Engine Number", 
	model,
    "policy start date", 
    "policy end date", 
    COUNT(*) AS record_count
FROM overallcleaned_chessis_engine
GROUP BY 
    "Cleaned Chassis Number", 
    "Cleaned Engine Number",
	model,
    "policy start date", 
    "policy end date"
HAVING COUNT(*) > 1;

select Count(*) from public.dupclean_cleanchassisengine_samechassisno_differregno

select Count(*) from public.dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr

select Count(*) from overallcleaned_chessis_engine

Select "data",Count(*) from overallcleaned_chessis_engine
where "insured name" Is Null
Group by 1

Select "data",Count(*) from overallcleaned_chessis_engine
where "Cleaned insured name" = '' or "Cleaned insured name" = 'none'
Group by 1

Select * from overallcleaned_chessis_engine
where "insured name" Is Null
Group by 1

select * from overallcleaned_chessis_engine
where "Cleaned Engine Number" = 'none'

select * from overallcleaned_chessis_engine
where "Cleaned Chassis Number" = 'none'

select * from overallcleaned_chessis_engine
where "Cleaned Reg no" = 'none'

select * from overallcleaned_chessis_engine
where "Cleaned Reg no" = 'mh49b3233'

SELECT t1."old policy no", t1."policy no", t1."insured name", 
       (SELECT t2."insured name" 
        FROM public.appended_base_and_pr t2
        WHERE t1."old policy no" = t2."policy no"
        AND t2."insured name" IS NOT NULL
        LIMIT 1) AS NewInsuredName
FROM public.appended_base_and_pr t1
WHERE t1."old policy no" IS NOT NULL
AND t1."insured name" IS NULL

SELECT t."old policy no" AS matched_policy, COUNT(*) AS duplicate_count
FROM overallcleaned_chessis_engine t
JOIN overallcleaned_chessis_engine p ON t."old policy no" = p."policy no"
WHERE t."old policy no" IS NOT NULL
GROUP BY t."old policy no"
HAVING COUNT(*) > 1;

SELECT 
    t."old policy no" AS matched_policy,
    COUNT(*) AS duplicate_count,
    COUNT(DISTINCT t."Cleaned insured name") AS distinct_insured_count
FROM overallcleaned_chessis_engine t
JOIN overallcleaned_chessis_engine p 
    ON t."old policy no" = p."policy no"
WHERE t."old policy no" IS NOT NULL
GROUP BY t."old policy no"
HAVING COUNT(*) > 1 
   AND COUNT(DISTINCT t."Cleaned insured name") > 1;

SELECT 
    t."old policy no" AS matched_policy,
    COUNT(*) AS duplicate_count,
    COUNT(DISTINCT t."Cleaned insured name") AS distinct_insured_count
FROM overallcleaned_chessis_engine t
JOIN overallcleaned_chessis_engine p 
    ON t."old policy no" = p."policy no"
WHERE t."old policy no" IS NOT NULL
GROUP BY t."old policy no"
HAVING COUNT(*) > 1 
   AND COUNT(t."Cleaned insured name") > 1;


SELECT 
    a.*,
    CASE 
        WHEN (a."Cleaned insured name" IS NULL 
              OR a."Cleaned insured name" = '' 
              OR lower(a."Cleaned insured name") = 'none')
        THEN b.lookup_name
        ELSE a."Cleaned insured name"
    END AS "Cleaned insured name_filled"
FROM overallcleaned_chessis_engine a
LEFT JOIN (
    SELECT 
        "policy no", 
        MAX("Cleaned insured name") AS lookup_name
    FROM overallcleaned_chessis_engine
    WHERE "Cleaned insured name" IS NOT NULL 
      AND "Cleaned insured name" <> '' 
      AND lower("Cleaned insured name") <> 'none'
    GROUP BY "policy no"
) b ON a."old policy no" = b."policy no";

SELECT * From (
Select
    a.*,
    CASE 
        WHEN (a."Cleaned insured name" IS NULL 
              OR a."Cleaned insured name" = '' 
              OR lower(a."Cleaned insured name") = 'none')
        THEN b.lookup_name
        ELSE a."Cleaned insured name"
    END AS "Cleaned insured name_filled"
FROM overallcleaned_chessis_engine a
LEFT JOIN (
    SELECT 
        "policy no", 
        MAX("Cleaned insured name") AS lookup_name
    FROM overallcleaned_chessis_engine
    WHERE "Cleaned insured name" IS NOT NULL 
      AND "Cleaned insured name" <> '' 
      AND lower("Cleaned insured name") <> 'none'
    GROUP BY "policy no"
) b ON a."old policy no" = b."policy no")
Where "Cleaned insured name_filled" = '' or "Cleaned insured name_filled" = 'none' or "Cleaned insured name_filled" Is null

Select Count(*) from public.cleancus_overallcleaned_chessis_engine

Select "data", Count(*) from public.cleancus_overallcleaned_chessis_engine
where "Cleaned insured name_filled" = '' or "Cleaned insured name_filled" = 'none' or "Cleaned insured name_filled" Is null
Group by 1

WITH ordered AS (
  SELECT *,
    LEAD("policy start date") OVER (
      PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "Cleaned insured name_filled"
      ORDER BY "policy start date"
    ) AS next_policy_start_date
  FROM public.cleancus_overallcleaned_chessis_engine
)
SELECT *
FROM (
  SELECT *,
    CASE 
      WHEN booked IS NULL
           AND next_policy_start_date IS NOT NULL
           AND next_policy_start_date >= "policy end date" + INTERVAL '1 day'
        THEN '1.0'
      WHEN booked IS NULL
           AND next_policy_start_date IS NULL
           AND "policy end date" >= '2025-01-01'
        THEN '2.0'
      WHEN booked IS NULL
           AND next_policy_start_date IS NULL
           AND "policy end date" < '2025-01-01'
        THEN '0.0'
      WHEN booked IS NULL
           AND next_policy_start_date IS NOT NULL
           AND "policy end date" < '2025-01-01'
        THEN '0.0'
      WHEN booked IS NULL
           AND next_policy_start_date IS NOT NULL
           AND "policy end date" >= '2025-01-01'
        THEN '2.0'
      ELSE booked::text
    END AS upd_booked
  FROM ordered
  ORDER BY "Cleaned Chassis Number", "Cleaned Engine Number", "Cleaned insured name_filled", "policy start date", "policy end date"
) a;

Select * from public.handled_bookedcase_base_pr

Select upd_booked, Count(*) from public.handled_bookedcase_base_pr
group by 1

Select * from public.handled_bookedcase_base_pr
Where upd_booked = '0' And next_policy_start_date Is Not null

Select * from public.handled_bookedcase_base_pr
Where upd_booked = '0.0' And next_policy_start_date Is Not null

select * from public.handled_bookedcase_base_pr 
Where "Cleaned Reg no" = 'gj01rh6206'


SELECT *
FROM public.handled_bookedcase_base_pr
WHERE upd_booked IN ('0', '0.0')
  AND (
       "policy end date" > '2025-01-01'
       OR next_policy_start_date >= "policy end date" + INTERVAL '1 day'
  )
ORDER BY "Cleaned Chassis Number",
         "Cleaned Engine Number",
         "Cleaned insured name_filled",
         "policy start date";


select * from public.handled_bookedcase_base_pr 
Where "Cleaned Chassis Number" = '00324948'

select * from public.handled_bookedcase_base_pr 
Where "Cleaned Chassis Number" = '00b10926'

select * from public.handled_bookedcase_base_pr 
Where "Cleaned Chassis Number" = '0mbjaa3em6007574610816'

--Drop Table public.backup_handled_bookedcase_base_pr


Select * Into public.backup_handled_bookedcase_base_pr from public.handled_bookedcase_base_pr

Select upd_booked, Count(*) from public.backup_handled_bookedcase_base_pr
group by 1
public.corrected_cleancus_overallcleaned_chessis_engine
corrected_name

-- UPDATE public.handled_bookedcase_base_pr
-- SET upd_booked =
--   CASE
--     WHEN upd_booked IN ('0', '0.0')
--          AND "policy end date" > '2025-01-01'
--       THEN '2.0'
--     WHEN upd_booked IN ('0', '0.0')
--          AND next_policy_start_date >= "policy end date" + INTERVAL '1 day'
--       THEN '1.0'
--     ELSE upd_booked
--   END
-- WHERE upd_booked IN ('0', '0.0')
--   AND (
--        "policy end date" > '2025-01-01'
--        OR next_policy_start_date >= "policy end date" + INTERVAL '1 day'
--   );

Select upd_booked, Count(*) from public.handled_bookedcase_base_pr
group by 1

Select Concat("Cleaned Chassis Number", '_', "Cleaned Engine Number"),
count(Concat("Cleaned Chassis Number", '_', "Cleaned Engine Number")) From public.handled_bookedcase_base_pr
Group by Concat("Cleaned Chassis Number", '_', "Cleaned Engine Number")
Order by count(Concat("Cleaned Chassis Number", '_', "Cleaned Engine Number")) Desc

Select Count(*) from public.corrected_cleancus_overallcleaned_chessis_engine
malc381ulhm267595_d4fbhm348318
malfc81alnm303238_g3lcnm493280
majaxxmrkajy74802_jy74802
--Drop Table public.corrected_cleancus_overallcleaned_chessis_engine

Select * from public.handled_bookedcase_base_pr
Where Concat("Cleaned Chassis Number", '_', "Cleaned Engine Number") = 'malc381ulhm267595_d4fbhm348318'

Select * from public.samechassisno_differregno
Where Concat("Cleaned Chassis Number", '_', "Cleaned Engine Number") = 'malc381ulhm267595_d4fbhm348318'

Select * from public.corrected_cleancus_overallcleaned_chessis_engine
Where chassis_engine_key = '204120_754668' 

--204120_754668 check

Select * from public.corrected_cleancus_overallcleaned_chessis_engine
Where chassis_engine_key = '204559_1789224'

Select * from public.corrected_cleancus_overallcleaned_chessis_engine
Where chassis_engine_key = '326760_3233161'

Select * from public.corrected_cleancus_overallcleaned_chessis_engine
Where chassis_engine_key = 'ma1na2xjxm6f27253_xjm6f48016'

Select * from public.corrected_cleancus_overallcleaned_chessis_engine
Where chassis_engine_key = 'ma1na2xjxm6h37289_xjm6h62083'
Order by "policy start date"

Select upd_booked, Count(*) from public.handled_bookedcase_base_pr
Where date_part("month", "policy end date") = 11 And date_part("year", "policy end date") = 2024
group by 1

SELECT *,
       CASE 
         WHEN "old policy no" IS NULL
              -- Check if the previous row was a renewed policy
              AND LAG(upd_booked) OVER (
                    PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                    ORDER BY "policy start date"
                  ) = '1.0'
              -- Ensure that the current start date is at least one day after the previous end date
              AND "policy start date" >= LAG("policy end date") OVER (
                    PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                    ORDER BY "policy start date"
                  ) + INTERVAL '1 day'
         THEN LAG("policy no") OVER (
                PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                ORDER BY "policy start date"
              )
         ELSE "old policy no"
       END AS updated_old_policy_no
FROM public.handled_bookedcase_base_pr
ORDER BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", "policy start date";

