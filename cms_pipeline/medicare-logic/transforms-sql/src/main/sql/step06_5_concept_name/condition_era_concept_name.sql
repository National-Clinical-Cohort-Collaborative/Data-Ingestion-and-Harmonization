CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/condition_era` AS
   SELECT co.*, c.concept_name as condition_concept_name
    FROM `ri.foundry.main.dataset.a840415c-2725-493e-b652-22dec250d202` co
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON co.condition_concept_id = c.concept_id 