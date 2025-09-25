CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_detail` AS

-- Basically fetching everyting from sn_visit_detail_prelim to create visit_detail, 
-- adding the visit_occurrence_id along the way.
-- in more detail:
--   We start from sn_visit_detail_prelim and join to the sn_visits_spans_with_detail_id to 
--   identify the visit_occurrence by person, provider and dates (we don't have the id here),
--          the visit_detail by the id that we have.
--   then we can write the visit_detail row with the the visit_occurrence id. 

SELECT
vdp.cms_visit_detail_occurrence_id,
vdp.person_id,
vdp.visit_detail_concept_id,
vdp.visit_detail_start_date,
vdp.visit_detail_start_datetime,
vdp.visit_detail_end_date,
vdp.visit_detail_end_datetime,
vdp.visit_detail_type_concept_id,
cast(null as int) as provider_id,
vdp.provider_id as care_site_id,
vdp.visit_detail_source_value,
vdp.visit_detail_source_concept_id,
vdp.admitting_source_concept_id,
vdp.admitting_source_value,
vdp.discharge_to_concept_id,
vdp.discharge_to_source_value,
vdp.preceeding_visit_detail_id,
vdp.visit_detail_parent_id,
vdp.source_domain,
vo.visit_occurrence_id, 
vdp.hashed_id,
vdp.sub_hash_value,
vdp.base_10_hash_value,
vdp.visit_detail_id

 
from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_detail_prelim`  vdp 
join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_spans_with_detail_id` vs 
  on  vdp.person_id = vs.person_id 
  and vdp.provider_id = vs.provider_id 
  and vdp.visit_detail_concept_id = vs.visit_detail_concept_id
  and vdp.visit_detail_start_date = vs.visit_detail_start_date 
  and vdp.visit_detail_end_date = vs.visit_detail_end_date
  and vdp.visit_detail_id = vs.visit_detail_id

join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` vo
  on vo.person_id = vs.person_id and vs.provider_id = vo.care_site_id 
  and vo.visit_concept_id = vs.visit_detail_concept_id
  and datediff(vo.visit_end_date, vs.span_end_date) = 0
  and datediff(vs.span_start_date, vo.visit_start_date) = 0

  and datediff(vo.visit_end_date, vdp.visit_detail_start_date) >= 0
  and datediff(vo.visit_end_date, vdp.visit_detail_end_date) >= 0
  and datediff(vdp.visit_detail_start_date, vo.visit_start_date) >= 0
  and datediff(vdp.visit_detail_end_date,   vo.visit_start_date) >= 0

  

