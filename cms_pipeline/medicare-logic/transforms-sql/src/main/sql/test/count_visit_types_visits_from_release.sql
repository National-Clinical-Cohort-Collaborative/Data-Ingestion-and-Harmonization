CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/logic/test/count_visit_types_visits_from_release` AS
-- trying to add rank/over to counts in here, l157


select "OMOP" as title, count(distinct visit_occurrence_id) as ct, visit_concept_id, source_domain 
-- from  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/visit_occurrence` vo 
from  `ri.foundry.main.dataset.2bc1565d-2f3c-4d6c-bd41-f2e50cda64ea` vo
group by visit_concept_id, source_domain

UNION
select "OMOP domain sum" as title, count(distinct visit_occurrence_id) as ct, 0 as visit_concept_id, source_domain 
-- from  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/visit_occurrence` vo 
from  `ri.foundry.main.dataset.2bc1565d-2f3c-4d6c-bd41-f2e50cda64ea` vo
group by  source_domain

UNION
select "OMOP concept sum" as title, count(distinct visit_occurrence_id) as ct, visit_concept_id, "all domains"
-- from  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/visit_occurrence` vo 
from  `ri.foundry.main.dataset.2bc1565d-2f3c-4d6c-bd41-f2e50cda64ea` vo
group by visit_concept_id



/*
union
select "OMOP release: person, provider, start, end" as title, count(person_id, provider_id, visit_start_date , visit_end_date) as ct, visit_concept_id,  source_domain 
-- from  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/visit_occurrence` vo 
from  `ri.foundry.main.dataset.2bc1565d-2f3c-4d6c-bd41-f2e50cda64ea` vo
where source_domain = 'SN'
group by visit_concept_id, source_domain
*/
