CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_spans_with_detail_id` AS

-- sn_visit_spans person_id, provider_id, visit_detail_start_date,  max(visit_detail_end_date) as visit_detail_end_date, group_number

select vd.visit_detail_id, 
     vd.person_id, vd.provider_id, 
     vd.visit_detail_concept_id,
     vd.visit_detail_start_date, vd.visit_detail_end_date,
     vs.span_id,
     vs.span_start_date, vs.span_end_date
from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_detail_prelim` vd
left join  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_spans` vs
  on vd.person_id = vs.person_id and vd.provider_id = vs.provider_id
   and datediff(vd.visit_detail_start_date, vs.span_start_date) >=0
   and datediff(vs.span_end_date, vd.visit_detail_start_date) >=0
   and datediff(vs.span_end_date, vd.visit_detail_end_date) >= 0
   and datediff(vd.visit_detail_end_date, vs.span_start_date) >= 0

   and datediff(vs.span_end_date, vs.span_start_date) >= 0
   and datediff(vd.visit_detail_end_date, vd.visit_detail_start_date) >= 0
   and vd.visit_detail_concept_id = vs.span_concept_id








