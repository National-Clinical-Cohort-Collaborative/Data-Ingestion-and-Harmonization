CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/macrovisit_check` AS
with ip as (
    SELECT * FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence`
    WHERE visit_concept_id = 9201
), a AS
(
    SELECT  care_site_id,
            visit_start_date,
            -- end date of current visit
            visit_end_date,
            -- max end date we've seen so far for this person, ordered by start date. (so far?? in the algorithm?, over all the data we have)
            MAX(visit_end_date) OVER (PARTITION BY person_id ORDER by visit_start_date) AS max_end_date, 
            RANK() OVER (PARTITION BY care_site_id ORDER by visit_start_date) AS rank_value -- unused here?


    FROM ip


),


-- Put a gap anytime the current visit starts AFTER the max end date of the last group of visits.
b AS
(
    SELECT  *,
            CASE 
                WHEN visit_start_date <= LAG(max_end_date) OVER (PARTITION BY care_site_id ORDER by visit_start_date) THEN 0
                ELSE 1
            END AS gap
    FROM a
),

-- the SUM(gaps) defines a unique number for each  macrovisit 
c AS
(
    SELECT *,
        SUM(gap) OVER (PARTITION BY care_site_id ORDER by visit_start_date) AS group_number
        FROM b
)

SELECT  care_site_id,
        group_number,
        MIN(visit_start_date) AS macrovisit_start_date,
        MAX(visit_end_date)   AS macrovisit_end_date,
        CONCAT(care_site_id, '_', group_number, '_', ABS(HASH(MIN(visit_start_date)))) AS macrovisit_id
FROM c
GROUP BY care_site_id, group_number
ORDER BY care_site_id, group_number