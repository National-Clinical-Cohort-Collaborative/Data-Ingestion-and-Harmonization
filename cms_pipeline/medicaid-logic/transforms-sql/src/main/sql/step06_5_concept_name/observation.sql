CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06_5 - add concept name/observation` AS

     SELECT o.*, c.concept_name as observation_concept_name, cs.concept_name as observation_source_concept_name
    FROM `ri.foundry.main.dataset.4c3121e4-158a-4d9d-98d0-5bc0d49b24e3` o
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON o.observation_concept_id = c.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON o.observation_source_concept_id = cs.concept_id