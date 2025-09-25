CREATE TABLE `ri.foundry.main.dataset.9c94b3d2-6f0a-4de6-a9d3-1fde29caa236` AS

    SELECT co.*, c.concept_name as condition_concept_name, cs.concept_name as condition_source_concept_name
    FROM `ri.foundry.main.dataset.345fe88b-e146-4770-9b19-6e9bd10a9eaf` co
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON co.condition_concept_id = c.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON co.condition_source_concept_id = cs.concept_id
    