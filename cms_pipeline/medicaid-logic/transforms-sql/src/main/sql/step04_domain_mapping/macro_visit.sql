CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit` AS
    
    --  start in the middle of existing visit- if the start date starts in the middle of another visit merge into one with min start date and max end date
    --  inside another visit if the start date and end dates are inside another visit, merge and pick the min start date and max end date
    --  start one day after the existing visit - if the start date is one day after the another visit merge these two into one and pick the min of the first visit start date and the max of the end visit date
    --if multiple rows with same begin and end date then rank by claim source ( ip, ot, lt, rx in this order ) and take the minimun
    -- regardless of the claim source we want to looks at the claim dates by a patient - OMOP is person centric
    -- --   CASE 
    --             -- Condition 1: Merge with previous visit if start date is in the middle or if previous previous end date 
    --             WHEN visit_start_date <= LAG(visit_end_date) OVER (PARTITION BY person_id ORDER BY visit_start_date, visit_end_date) THEN 0
    --             -- Condition 2: Merge with previous visit if start date is one day after
    --             WHEN visit_start_date = LAG(DATE_ADD(visit_start_date, 1) ) OVER (PARTITION BY person_id ORDER BY visit_start_date, visit_end_date) THEN 0
    --             --LEAD() next row,  Condition 3: Merge with next visit if end date is inside 
    --             WHEN visit_end_date >= LEAD(visit_end_date) OVER (PARTITION BY person_id ORDER BY visit_start_date, visit_end_date) THEN 0
    --             -- if difference is greater than one day
    --             WHEN DATEDIFF( visit_end_date, LEAD(visit_start_date) OVER (PARTITION BY person_id ORDER BY visit_start_date, visit_end_date)) > 1 THEN 1
    --             ELSE 1
    --         END AS is_new_visit
    with cte_merge_visit as (
     SELECT distinct 
           -- we are to look at overlapping date for a person across all claims. 
            person_id,
           --start date of visit present in the claim
            visit_start_date,
            -- end date of visit present in the claim
            visit_end_date,
             -- max end date we've seen so far for this person, ordered by start date
            MAX(visit_end_date) OVER (PARTITION BY person_id ORDER by visit_start_date) AS max_end_date, 
            RANK() OVER (PARTITION BY person_id ORDER by visit_start_date) AS rank_value
        FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` v
        -- we cannot allow null end dates
        WHERE visit_end_date is not null and visit_start_date is not null 
        --some end dates are like in year 9999, clearly not possible end dates 9999-12-31
        --so remove the year that is in the future
        and visit_end_date <= CURRENT_DATE()
    ), 
-- Put a gap anytime the current visit starts AFTER the max end date of the last group of visits
--- indicate the break as gap ( sn or lt monthly should be handled in the visit_detail and not in the macro visit
--  as those claims are filed monthly/weekly
--- indicate breaks in the timeline as gap
    b AS
    (
        SELECT  v.*,
                CASE 
                    WHEN visit_start_date <= LAG(max_end_date) OVER (PARTITION BY person_id ORDER by visit_start_date) THEN 0
                    ELSE 1
                END AS gap
         FROM cte_merge_visit v
    ),

    -- the SUM(gaps) defines a unique number for each  macrovisit 
    -- if the gap is 0 then those rows will not get added to the sum
    -- assign group number to only to rows that contain time breaks. 
    c AS
    (
        SELECT *,
            SUM(gap) OVER (PARTITION BY person_id ORDER by visit_start_date) AS group_number
            FROM b
    ),

    macro_visit as (
    SELECT  
        person_id,
        group_number,
        MIN(visit_start_date) AS macrovisit_start_date,
        MAX(visit_end_date)   AS macrovisit_end_date,
        CONCAT(person_id, '_', group_number, '_', ABS(HASH(MIN(visit_start_date)))) AS macrovisit_id
    FROM c
    GROUP BY person_id, group_number
    ORDER BY person_id, group_number
    )

    SELECT
    macrovisit_id
    ,person_id
    ,macrovisit_start_date
    ,macrovisit_end_date
    from macro_visit

