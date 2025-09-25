CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/observation` AS

    SELECT o.*, c.concept_name as observation_concept_name, cs.concept_name as observation_source_concept_name
    FROM `ri.foundry.main.dataset.b885e6a3-2251-4f7a-a369-b24541208fd3` o
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON o.observation_concept_id = c.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON o.observation_source_concept_id = cs.concept_id