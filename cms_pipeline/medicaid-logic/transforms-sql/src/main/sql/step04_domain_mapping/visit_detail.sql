
CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_detail` AS

-- CREATE TABLE  `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_detail`  AS


WITH lt_detail as (
SELECT
distinct
vs.visit_detail_id, 
vs.person_id, 
vs.visit_detail_concept_id,
vs.visit_detail_start_date, 
vs.visit_detail_start_datetime, 
vs.visit_detail_end_date, 
vs.visit_detail_end_datetime, 
vs.visit_detail_type_concept_id, 
vs.provider_id, 
vs.care_site_id, 
CAST(null as string) as visit_detail_source_value, 
CAST(null as int) as visit_detail_source_concept_id,
CAST(null as string) as admitting_source_value,   
CAST(null as int) as admitting_source_concept_id,
CAST(null as int) as discharge_to_source_value, 
CAST(null as int) as discharge_to_concept_id,
CAST(null as int) as preceeding_visit_detail_id, 
CAST(null as int) as visit_detail_parent_id, 
vo.visit_occurrence_id, -- (reqd)
'LT' as source_domain  
-- vs.hashed_id, 
-- vs.sub_hash_value, 
-- vs.base_10_hash_value

  /* Below, the vs table has provider_id as string with empty values, vo has provider as long with nulls
     Try to cast both to long, then coalesce the resulting nulls to the same value
     When casting vo.provider_id to long (from long), it's a no brainer and null casts to null which is then coalesced to -1.
     When casting vs.provider_id to long from a string, the empty strings turn into nulls that are then coalescd to -1.
     I cast on both sides because its mostly harmless and spares one the burden of remembering which was which type.
     Additionally, provider_id sometimes includes string values "000000000" that when compared to a zero cast to a string
     fails to equal "0". It works better to cast both to long and compare the numbers rather than the strings. */
 
from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/lt_visit_detail_with_spans`  vs 
join `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` vo
  on vo.person_id = vs.person_id 
  and coalesce(cast(vo.provider_id as long), -1) = coalesce(cast(vs.provider_id  as long), -1)
  and coalesce(cast(vo.care_site_id as long), -1) = coalesce(cast(vs.care_site_id as long), -1)
  and vo.visit_concept_id = vs.visit_detail_concept_id
  and datediff(vo.visit_end_date, vs.span_end_date) = 0
  and datediff(vs.span_start_date, vo.visit_start_date) = 0
)

select * from lt_detail

  

