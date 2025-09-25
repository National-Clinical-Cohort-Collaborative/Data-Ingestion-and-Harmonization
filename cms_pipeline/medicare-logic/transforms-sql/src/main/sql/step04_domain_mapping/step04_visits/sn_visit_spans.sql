CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_spans` AS
 WITH 
-- these first 4 create the VISIT_SPANS from the visit_detail rows
MAX_DATES as ( -- this is a rolling aggregate. It's not just an order-by, but an order-by on the same column...
    SELECT person_id, provider_id, 
    visit_detail_start_date, visit_detail_end_date, visit_detail_concept_id,
           max(visit_detail_end_date) OVER (partition by person_id, provider_id, visit_detail_start_date order by visit_detail_end_date  ) as max_thru_date
    FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_detail_prelim` 
),
VISIT_GAPS as ( -- 1 is a new group, 0 is a continuation. the date_add is for dealing with adjacent dates, not just overlaps.
    select person_id, provider_id, visit_detail_concept_id, visit_detail_start_date, visit_detail_end_date, max_thru_date,
    case 
      WHEN visit_detail_start_date <= 
             date_add(
                LAG( max_thru_date) OVER (partition by person_id, provider_id order by visit_detail_start_date),
                1)
        THEN 0
      ELSE 1
    end as gap
    FROM MAX_DATES
),
GROUP_NUMBER as (
    select person_id, provider_id, visit_detail_concept_id, visit_detail_start_date, visit_detail_end_date,  max_thru_date, gap,
       SUM(gap) OVER(partition by person_id, provider_id order by visit_detail_start_date) as group_number
    from VISIT_GAPS
)

select person_id, provider_id, 
       visit_detail_concept_id as span_concept_id,
       min(visit_detail_start_date) as span_start_date, 
       max(visit_detail_end_date) as span_end_date, 
       group_number,
       CONCAT(person_id, '_', group_number, '_', visit_detail_concept_id, '-', ABS(HASH(MIN(visit_detail_start_date)))) AS span_id
from GROUP_NUMBER
group by person_id, provider_id, group_number, visit_detail_concept_id