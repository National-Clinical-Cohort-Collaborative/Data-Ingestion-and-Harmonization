CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06_5 - add concept name/condition_era` AS
 
   SELECT co.*, c.concept_name as condition_concept_name
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06 - schema check/condition_era` co
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON co.condition_concept_id = c.concept_id 