CREATE TABLE  `ri.foundry.main.dataset.94111cbc-40cb-422d-96f2-5c751a5e638c` AS
-- `ri.foundry.main.dataset.94111cbc-40cb-422d-96f2-5c751a5e638c` AS
WITH
MAX_DATES as ( -- this is a rolling aggregate. It's not just an order-by, but an order-by on the same column...
    SELECT person_id,  provider_id, 
           visit_detail_start_date, visit_detail_end_date, 
           visit_detail_concept_id, visit_detail_type_concept_id,
           max(visit_detail_end_date) 
               OVER (partition by person_id, provider_id, visit_detail_concept_id --visit_detail_start_date,
               order by visit_detail_start_date,visit_detail_end_date ) as max_thru_date
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_detail_folder/lt_visit_detail` lt
    where visit_detail_start_date is not null and visit_detail_end_date is not null
),
VISIT_GAPS as ( -- 1 is a new group, 0 is a continuation. the date_add is for dealing with adjacent dates, not just overlaps.
    SELECT person_id, provider_id,
        visit_detail_start_date, visit_detail_end_date, max_thru_date,
        visit_detail_concept_id, visit_detail_type_concept_id, 
        CASE 
            WHEN visit_detail_start_date <= 
                date_add(
                    LAG( max_thru_date) 
                        OVER (partition by person_id, provider_id,visit_detail_concept_id 
                              order by visit_detail_start_date),
                    1)
            THEN 0
            ELSE 1
        END as gap
    FROM MAX_DATES
),
GROUP_NUMBER as (
    SELECT person_id, provider_id, 
        visit_detail_start_date, visit_detail_end_date,  max_thru_date, 
        visit_detail_concept_id,  visit_detail_type_concept_id, 
        gap, SUM(gap) 
            OVER(partition by person_id, provider_id,visit_detail_concept_id order by visit_detail_start_date) as group_number
    FROM VISIT_GAPS
),
VISIT_SPANS as (
    SELECT person_id, provider_id,  
       min(visit_detail_start_date) as span_start_date, 
       max(visit_detail_end_date) as span_end_date, 
       visit_detail_concept_id as span_concept_id,
       --visit_detail_type_concept_id as span_type_concept_id, 
       group_number,
       CONCAT(person_id, '_', 
              group_number,  '_',
              provider_id, '_', 
              visit_detail_concept_id, '_',
              ABS(HASH(MIN(visit_detail_start_date)))) AS span_id
    FROM GROUP_NUMBER
    GROUP BY person_id, provider_id, group_number, 
            visit_detail_concept_id-- , visit_detail_type_concept_id
)
SELECT   
     vd. visit_detail_id,
     vd.person_id, vd.provider_id, 
     vd.care_site_id,
     vd.visit_detail_start_date, vd.visit_detail_end_date,
     vd.visit_detail_start_datetime, vd.visit_detail_end_datetime, 
     vd.visit_detail_concept_id, visit_detail_type_concept_id, 
     vd.admitting_source_value, 
     vs.span_id, vs.span_start_date, vs.span_end_date
FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_detail_folder/lt_visit_detail` vd
inner JOIN VISIT_SPANS vs  --- <-------------------------------------------- inner join filter out the orphan codes. Left join keeps them
  ON vd.person_id = vs.person_id 
   and coalesce(cast(vd.provider_id as long), -1) = coalesce(cast(vs.provider_id as long), -1)
   and datediff(vd.visit_detail_start_date, vs.span_start_date) >=0
   and datediff(vs.span_end_date, vd.visit_detail_start_date) >=0
   and datediff(vs.span_end_date, vd.visit_detail_end_date) >= 0
   and datediff(vd.visit_detail_end_date, vs.span_start_date) >= 0

   and datediff(vs.span_end_date, vs.span_start_date) >= 0
   and datediff(vd.visit_detail_end_date, vd.visit_detail_start_date) >= 0
    and vd.visit_detail_concept_id = vs.span_concept_id




