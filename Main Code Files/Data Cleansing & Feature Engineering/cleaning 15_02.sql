UPDATE public.appended_base_and_pr t1
SET "insured name" = t2."Insured name "
FROM public."2022_pr_cleaneddata" t2
WHERE t1."policy no" = t2."Policy No"
AND t1."insured name" Is null;

Select "data",Count(*) from public.appended_base_and_pr
where "insured name" Is Null
Group by 1

SELECT t1."old policy no", t1."policy no", t1."insured name", 
       (SELECT t2."insured name" 
        FROM public.appended_base_and_pr t2
        WHERE t1."old policy no" = t2."policy no"
        AND t2."insured name" IS NOT NULL
        LIMIT 1) AS NewInsuredName
FROM public.appended_base_and_pr t1
WHERE t1."old policy no" IS NOT NULL
AND t1."insured name" IS NULL
Limit 100

Select "data",Count(*) from public.basiccleaned_appended_base_and_pr
where "insured name" Is Null
Group by 1

Select Count(*) from public.basiccleaned_appended_base_and_pr

SELECT * 
FROM public.basiccleaned_appended_base_and_pr
WHERE "Cleaned Reg no" IN (
    SELECT "Cleaned Reg no"
    FROM public.basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Reg no"
    HAVING COUNT(DISTINCT "Cleaned Chassis Number") > 1
)
And "Cleaned Reg no" <> ''
ORDER BY "Cleaned Reg no"

SELECT 
    t1."Cleaned Reg no",
    t1."Cleaned Chassis Number" AS Original_Chessis,
    t2."Cleaned Chassis Number" AS Corrected_Chessis
FROM public.basiccleaned_appended_base_and_pr AS t1
JOIN public.basiccleaned_appended_base_and_pr AS t2
ON t1."Cleaned Reg no" = t2."Cleaned Reg no"
AND LENGTH(t1."Cleaned Chassis Number") < LENGTH(t2."Cleaned Chassis Number")
AND t2."Cleaned Chassis Number" LIKE '%' || t1."Cleaned Chassis Number"
limit 100

SELECT * 
FROM public.basiccleaned_appended_base_and_pr
WHERE "Cleaned Chassis Number" IN (
    SELECT "Cleaned Chassis Number"
    FROM public.basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Chassis Number"
    HAVING COUNT(DISTINCT "Cleaned Reg no") > 1
)
--And "Cleaned Chassis Number" <> ''
ORDER BY "Cleaned Chassis Number"

CREATE TABLE IF NOT EXISTS public.samechassisno_differregno AS 
SELECT * 
FROM public.basiccleaned_appended_base_and_pr
WHERE "Cleaned Chassis Number" IN (
    SELECT "Cleaned Chassis Number"
    FROM public.basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Chassis Number"
    HAVING COUNT(DISTINCT "Cleaned Reg no") > 1
);

select * from public.samechassisno_differregno

Select * Into public.backup_basiccleaned_appended_base_and_pr from public.basiccleaned_appended_base_and_pr

Drop Table public.basiccleaned_appended_base_and_pr

Select * Into public.basiccleaned_appended_base_and_pr from public.backup_basiccleaned_appended_base_and_pr

Delete FROM public.basiccleaned_appended_base_and_pr
WHERE "Cleaned Chassis Number" IN (
    SELECT "Cleaned Chassis Number"
    FROM public.basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Chassis Number"
    HAVING COUNT(DISTINCT "Cleaned Reg no") > 1
);

Select count(*) from public.basiccleaned_appended_base_and_pr


UPDATE public.basiccleaned_appended_base_and_pr AS t1
SET "Cleaned Chassis Number" = t2."Cleaned Chassis Number"
FROM public.basiccleaned_appended_base_and_pr AS t2
WHERE t1."Cleaned Reg no" = t2."Cleaned Reg no"
AND LENGTH(t1."Cleaned Chassis Number") < LENGTH(t2."Cleaned Chassis Number")
AND t2."Cleaned Chassis Number" LIKE '%' || t1."Cleaned Chassis Number";


SELECT * 
FROM public.basiccleaned_appended_base_and_pr
WHERE "Cleaned Chassis Number" IN (
    SELECT "Cleaned Chassis Number"
    FROM public.basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Chassis Number"
    HAVING COUNT(DISTINCT "Cleaned Reg no") = 1
)
--And "Cleaned Chassis Number" <> ''
ORDER BY "Cleaned Chassis Number""Cleaned Engine Number"

SELECT t1.*,  -- Select all columns from t1 (original records)
       t2."Cleaned Chassis Number" AS new_chassis -- New chassis number from t2
FROM public.basiccleaned_appended_base_and_pr AS t1
JOIN public.basiccleaned_appended_base_and_pr AS t2
ON t1."Cleaned Reg no" = t2."Cleaned Reg no"
WHERE LENGTH(t1."Cleaned Chassis Number") < LENGTH(t2."Cleaned Chassis Number")
AND t2."Cleaned Chassis Number" LIKE '%' || t1."Cleaned Chassis Number"
limit 10

SELECT
  pid,
  usename,
  datname,
  client_addr,
  backend_start,
  query_start,
  state,
  query
FROM pg_stat_activity;

SELECT pg_cancel_backend(19888);

select count(*) from public.dupclean_basiccleaned_appended_base_and_pr

SELECT * 
FROM public.dupclean_basiccleaned_appended_base_and_pr
WHERE "Cleaned Chassis Number" IN (
    SELECT "Cleaned Chassis Number"
    FROM public.dupclean_basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Chassis Number"
    HAVING COUNT(DISTINCT "Cleaned Reg no") > 1
)
--And "Cleaned Chassis Number" <> ''
ORDER BY "Cleaned Chassis Number"

Select * from public.dupclean_basiccleaned_appended_base_and_pr
Where "Cleaned Reg no" = 'up32cs0879'
Order By "policy start date"


SELECT "Cleaned insured name", count("Cleaned insured name")
FROM public.dupclean_basiccleaned_appended_base_and_pr
WHERE "Cleaned Reg no" IN (
    SELECT "Cleaned Reg no"
    FROM public.dupclean_basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Reg no"
    HAVING COUNT(DISTINCT "Cleaned Chassis Number") > 1
)
And "Cleaned Reg no" <> ''
--ORDER BY "Cleaned Reg no"
Group by "Cleaned insured name" 
Order by count("Cleaned insured name") < 5 Desc

Select * from public.dupclean_basiccleaned_appended_base_and_pr
where "Cleaned insured name" = '4gidentitysolutionspvtltd'
Order by "Cleaned Reg no", "policy start date"

pg_dump -U postgres -d postgres -t public.basiccleaned_appended_base_and_pr -F t -f "D:\Backup DB data\basiccleaned_appended_base_and_pr_backup.tar"

SELECT 
    "Cleaned Chassis Number", 
    "Cleaned Engine Number", 
    "policy start date", 
    "policy end date", 
    COUNT(*) AS record_count
FROM public.dupclean_basiccleaned_appended_base_and_pr
GROUP BY 
    "Cleaned Chassis Number", 
    "Cleaned Engine Number", 
    "policy start date", 
    "policy end date"
HAVING COUNT(*) > 1;

SELECT *
FROM public.dupclean_basiccleaned_appended_base_and_pr
WHERE "Cleaned Reg no" IN (
    SELECT "Cleaned Reg no"
    FROM public.dupclean_basiccleaned_appended_base_and_pr
    GROUP BY "Cleaned Reg no"
    HAVING COUNT(DISTINCT "Cleaned Chassis Number") > 1
)
--And "Cleaned Reg no" <> ''
ORDER BY "Cleaned Reg no"

Select "data",Count(*) from public.appended_base_and_pr
where "insured name" Is Null
Group by 1

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = 'e158908'

select * from public.basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = 'e158908'

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = 'xjn6c27785'

select * from public.basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = 'xjn6c27785'

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = 'revtrn11fxxk71513'

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Reg no" = 'ap05de2266'

select * from public.basiccleaned_appended_base_and_pr
where "Cleaned Reg no" = 'new'

select * from public.basiccleaned_appended_base_and_pr
where "Cleaned Reg no" = ''

select * from public.basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = 'k12np1135861'

select * from public.dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Reg no" = ''

-- Drop Table public.dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Reg no" = ''

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Chassis Number" = '' or "Cleaned Chassis Number" = 'blank'

select * from public.cleanchassisengine_basiccleaned_appended_base_and_pr
where "Cleaned Engine Number" = '' or "Cleaned Engine Number" = 'blank'