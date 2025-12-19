
SELECT *,
       CASE 
         WHEN "old policy no" IS NULL
              AND LAG(upd_booked) OVER (
                    PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                    ORDER BY "policy start date"
                  ) IN ('1.0', '1')
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


Select Count(*) from public.mapoldpolicy_handled_bookedcase_base_pr

Select count(*) from public.mapoldpolicy_handled_bookedcase_base_pr
where updated_old_policy_no Is Not null

Select upd_booked, Count(*) from public.mapoldpolicy_handled_bookedcase_base_pr
group by 1 

Select count(*) from public.mapoldpolicy_handled_bookedcase_base_pr
where "old policy no" Is Not null

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "old policy no" Is Not null And updated_old_policy_no Is null

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "old policy no" Is Not null And updated_old_policy_no Is not null

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where updated_old_policy_no Is Not null

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
Where chassis_engine_key = '326760_3233161' 

Select "Policy Status", count(*) from public.mapoldpolicy_handled_bookedcase_base_pr
Group by 1

SELECT
    "policy no",
    COUNT(DISTINCT corrected_name) AS distinct_customers,
    COUNT(DISTINCT "Cleaned Branch Name 2")   AS distinct_branches
FROM public.mapoldpolicy_handled_bookedcase_base_pr
GROUP BY "policy no"
HAVING COUNT(DISTINCT corrected_name) > 1 
    OR COUNT(DISTINCT "Cleaned Branch Name 2") > 1;

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "policy no" = '''201120010121850012800000'

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "policy no" = '''201120010221800080400000'

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "policy no" = '''201120010222850011300000'

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "policy no" = '''201120010222850020200000'

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "policy no" = '''201130140121100053705000'

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "policy no" = '''202550030121700044201000'

Select * from public.mapoldpolicy_handled_bookedcase_base_pr
where "policy no" = '''201120010222850098100000'


SELECT
    "policy no",
    COUNT(DISTINCT corrected_name) AS distinct_customers,
    COUNT(DISTINCT "Cleaned Branch Name 2")   AS distinct_branches
FROM public.mapoldpolicy_handled_bookedcase_base_pr
GROUP BY "policy no"
HAVING COUNT(DISTINCT corrected_name) > 1 



SELECT *
FROM (
    SELECT 
        *,
        LEAD(biztype) OVER (
            PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", corrected_name
            ORDER BY "policy start date"
        ) AS next_biztype
    FROM mapoldpolicy_handled_bookedcase_base_pr
) AS sub
WHERE biztype = 'Renewal Business'
  AND "Policy Status" = 'Renewed'
  AND next_biztype IS NOT NULL
  AND next_biztype <> biztype
ORDER BY "policy start date";


SELECT *
    -- "Cleaned Chassis Number", 
    -- "Cleaned Engine Number", 
    -- corrected_name,
    -- MIN("policy start date") AS first_policy_start_date
    -- -- You can add other aggregate functions here if needed.
FROM (
    SELECT 
        *,
        LEAD(biztype) OVER (
            PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", corrected_name
            ORDER BY "policy start date"
        ) AS next_biztype
    FROM mapoldpolicy_handled_bookedcase_base_pr
) AS sub
WHERE biztype = 'Renewal Business'
  AND "Policy Status" = 'Renewed'
  AND next_biztype IS NOT NULL
  AND next_biztype <> biztype
GROUP BY "Cleaned Chassis Number", "Cleaned Engine Number", corrected_name
ORDER BY MIN("policy start date");

Select * from mapoldpolicy_handled_bookedcase_base_pr
where corrected_name = 'jayadityagupta'
order by "policy start date"


Select * from mapoldpolicy_handled_bookedcase_base_pr
where corrected_name = 'millenniumstockbrokingpvtltd'
order by "policy start date"


Select * from mapoldpolicy_handled_bookedcase_base_pr
where corrected_name = 'mahendersinghtyagi'
order by "policy start date"

Select * from mapoldpolicy_handled_bookedcase_base_pr
where corrected_name = 'jaginderkaur'
order by "policy start date"


WITH sub AS (
    SELECT 
        *,
        LEAD(biztype) OVER (
            PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", corrected_name
            ORDER BY "policy start date"
        ) AS next_biztype
    FROM mapoldpolicy_handled_bookedcase_base_pr
)
SELECT *,
       next_policy_start_date - "policy end date" AS date_diff
FROM sub
WHERE biztype = 'Renewal Business'
  AND "Policy Status" = 'Renewed'
  AND next_biztype IS NOT NULL
  AND next_biztype <> biztype
ORDER BY date_diff Asc

Select chassis_engine_key from mapoldpolicy_handled_bookedcase_base_pr
Group by chassis_engine_key 
Having count(distinct model) > 1

select *  from mapoldpolicy_handled_bookedcase_base_pr
where chassis_engine_key = '00000n6h94216_00000zdn4h66374'


select *  from mapoldpolicy_handled_bookedcase_base_pr
where chassis_engine_key = '041659elz_258059'


select *  from mapoldpolicy_handled_bookedcase_base_pr
where chassis_engine_key = '386790_2234621'


select *  from mapoldpolicy_handled_bookedcase_base_pr
where chassis_engine_key = '481481_2315403'

--Drop table public.overall_cleaned_base_and_pr_ef

Select Count(*) from public.overallcorrected_base_pr_claim

select * from public.overallcorrected_base_pr_claim limit 1000

Select "Policy Status", count(*) from public.overall_cleaned_base_and_pr_ef
Group by 1

Select Count(*) From  public.overall_cleaned_base_and_pr_ef
Where updated_old_policy_no Is Not null

WITH base AS (
  SELECT *,
         -- Determine if the row is the start of a new renewal chain:
         CASE 
           WHEN LAG("policy start date") OVER (
                  PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name" 
                  ORDER BY "policy start date"
                ) IS NULL 
                THEN 1
           -- If the row does NOT meet the renewal condition, mark it as a new chain start.
           WHEN NOT (
                  "old policy no" IS NULL
                  AND LAG(upd_booked) OVER (
                        PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name" 
                        ORDER BY "policy start date"
                      ) IN ('1.0', '1')
                  AND "policy start date" >= LAG("policy end date") OVER (
                        PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name" 
                        ORDER BY "policy start date"
                      ) + INTERVAL '1 day'
                )
                THEN 1
           ELSE 0
         END AS chain_start_flag
  FROM public.overall_cleaned_base_and_pr_ef
),
grouped AS (
  SELECT *,
         -- Create a cumulative group number for each renewal chain.
         SUM(chain_start_flag) OVER (
           PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name" 
           ORDER BY "policy start date" 
           ROWS UNBOUNDED PRECEDING
         ) AS chain_group
  FROM base
)
SELECT 
  *,
  -- For each renewal chain, pick the first policy no.
  FIRST_VALUE("policy no") OVER (
    PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", chain_group
    ORDER BY "policy start date"
  ) AS first_initial_policy_no
FROM grouped
ORDER BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", "policy start date";


WITH base AS (
  SELECT *,
         -- Mark a row as the start of a new renewal chain if:
         -- 1. There is no previous row (i.e. first row in the group), OR
         -- 2. The renewal conditions are not met:
         --      - Previous upd_booked is NOT in ('1.0', '1') OR
         --      - Current policy start date is NOT at least 1 day after the previous policy end date.
         CASE 
           WHEN LAG("policy start date") OVER (
                  PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                  ORDER BY "policy start date"
                ) IS NULL 
           THEN 1
           WHEN NOT (
                  LAG(upd_booked) OVER (
                    PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                    ORDER BY "policy start date"
                  ) IN ('1.0', '1')
                  AND "policy start date" >= LAG("policy end date") OVER (
                    PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                    ORDER BY "policy start date"
                  ) + INTERVAL '1 day'
                )
           THEN 1
           ELSE 0
         END AS chain_start_flag
  FROM public.overall_cleaned_base_and_pr_ef
),
grouped AS (
  SELECT *,
         -- Compute a cumulative sum of the chain_start_flag to assign a group number
         SUM(chain_start_flag) OVER (
           PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
           ORDER BY "policy start date"
           ROWS UNBOUNDED PRECEDING
         ) AS chain_group
  FROM base
),
first_policy AS (
  SELECT *,
         -- For each renewal chain, select the first policy number as the initial policy.
         FIRST_VALUE("policy no") OVER (
           PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", chain_group
           ORDER BY "policy start date"
         ) AS first_initial_policy_no
  FROM grouped
),
tenure AS (
  SELECT *,
         -- Calculate the chain’s start and end dates
         MIN("policy start date") OVER (
           PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", chain_group
         ) AS chain_start_date,
         MAX("policy end date") OVER (
           PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", chain_group
         ) AS chain_end_date
  FROM first_policy
)
SELECT *,
       -- Compute overall policy tenure as the difference between chain_end_date and chain_start_date.
       chain_end_date - chain_start_date AS overall_policy_tenure
FROM tenure
ORDER BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", "policy start date";


WITH base AS (
  SELECT *,
         -- Mark a row as starting a new renewal chain if:
         -- (1) It's the first row in the group, OR
         -- (2) The renewal conditions are not met:
         --      - Previous upd_booked is NOT in ('1.0', '1'), OR
         --      - Current policy start date is NOT at least 1 day after the previous policy end date.
         CASE 
           WHEN LAG("policy start date") OVER (
                  PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                  ORDER BY "policy start date"
                ) IS NULL 
           THEN 1
           WHEN NOT (
                  LAG(upd_booked) OVER (
                    PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                    ORDER BY "policy start date"
                  ) IN ('1.0', '1')
                  AND "policy start date" >= LAG("policy end date") OVER (
                    PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
                    ORDER BY "policy start date"
                  ) + INTERVAL '1 day'
                )
           THEN 1
           ELSE 0
         END AS new_chain_flag
  FROM public.overall_cleaned_base_and_pr_ef
),
grouped AS (
  SELECT *,
         -- Compute a cumulative sum of new_chain_flag to assign a chain_group for each renewal chain.
         SUM(new_chain_flag) OVER (
           PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name"
           ORDER BY "policy start date"
           ROWS UNBOUNDED PRECEDING
         ) AS chain_group
  FROM base
),
first_policy AS (
  SELECT *,
         -- For each renewal chain, select the first policy number as the initial policy.
         FIRST_VALUE("policy no") OVER (
           PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", chain_group
           ORDER BY "policy start date"
         ) AS first_initial_policy_no
  FROM grouped
)
SELECT *
FROM (
  SELECT 
    *,
    -- For each chain group, assign a sequential number (starting at 1) based on policy start date.
    ROW_NUMBER() OVER (
      PARTITION BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", chain_group
      ORDER BY "policy start date"
    ) AS policy_wise_purchase
  FROM first_policy
) AS final_result
ORDER BY "Cleaned Chassis Number", "Cleaned Engine Number", "corrected_name", "policy start date";

Select Count(*) from public.overall_cleaned_base_and_pr_ef_policyef
Where 

Select * from public.overall_cleaned_base_and_pr_ef_policyef
Order By customerid, "policy start date"

Select Count(*) from public.overall_cleaned_base_and_pr_ef_policyef
where "Cleaned Zone 2" = 'none' 

SELECT 
    CONCAT("policy no", '-', "policy start date", '-', "policy end date") AS policy_identifier,
    COUNT(*) AS record_count
FROM public.cleaned_appended_base_and_pr
GROUP BY policy_identifier
HAVING COUNT(*) > 1;

-- Add a new column for the cleaned insured name
ALTER TABLE public.overall_cleaned_base_and_pr_ef_policyef ADD COLUMN "cleaned new vertical" TEXT;

-- Update the new column with cleaned names
UPDATE public.overall_cleaned_base_and_pr_ef_policyef
SET  "cleaned new vertical" = LOWER(REGEXP_REPLACE("new vertical", '[^a-zA-Z0-9]', '', 'g'));

SELECT 
    CONCAT("policy no", '-', "policy start date", '-', "policy end date") AS policy_identifier,
    COUNT(*) AS record_count
FROM public.overall_cleaned_base_and_pr_ef_policyef
GROUP BY policy_identifier
HAVING COUNT(*) > 1;

SELECT 
    CONCAT("Cleaned Chassis Number", '-', "Cleaned Engine Number", '-', "policy start date", '-', "policy end date") AS policy_identifier,
    COUNT(*) AS record_count
FROM public.overall_cleaned_base_and_pr_ef_policyef
GROUP BY policy_identifier
HAVING COUNT(*) > 1;

Select Count(*) from public.overall_cleaned_base_and_pr_ef_policyef
where "cleaned new vertical" Is null or "cleaned new vertical" = 'none' or "cleaned new vertical" = ''

Select * from public.overall_cleaned_base_and_pr_ef_policyef
where "tie up" Is null or "cleaned new vertical" = 'none' or "cleaned new vertical" = ''

Select Count(*) from public.overall_cleaned_base_and_pr_ef_policyef
where "tie up" Is null And updated_old_policy_no is not null

select * from public.overall_cleaned_base_and_pr_ef_policyef limit 10000

select * from public.overallcorrected_base_pr_claim limit 100

select Count(*) from public.overall_cleaned_base_and_pr_ef_policyef limit 100

select Count(*) from public.overallcorrected_base_pr_claim limit 100

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


ALTER TABLE public.overall_cleaned_base_and_pr_ef_policyef
ADD COLUMN "Overall Churned" TEXT;

WITH LatestChurn AS (
    SELECT
        customerid,
        "Churn Label",
        ROW_NUMBER() OVER (
            PARTITION BY customerid
            ORDER BY "End Year" DESC
        ) AS rn
    FROM
        public.overall_cleaned_base_and_pr_ef_policyef
)
UPDATE public.overall_cleaned_base_and_pr_ef_policyef
SET "Overall Churned" = LatestChurn."Churn Label"
FROM LatestChurn
WHERE
    public.overall_cleaned_base_and_pr_ef_policyef.customerid = LatestChurn.customerid
    AND LatestChurn.rn = 1;

Select * from public.overall_cleaned_base_and_pr_ef_policyef
Order By customerid, "policy start date"
updated_old_policy_no