CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06_5 - add concept name/condition_occurrence`
  
    SELECT co.*, c.concept_name as condition_concept_name, cs.concept_name as condition_source_concept_name
    
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06 - schema check/condition_occurrence` co
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON co.condition_concept_id = c.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON co.condition_source_concept_id = cs.concept_id
    