CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/measurement` AS
    SELECT m.*, 
    c.concept_name as measurement_concept_name, cs.concept_name as measurement_source_concept_name
    FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06 - schema check/measurement` m
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON m.measurement_concept_id = c.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON m.measurement_source_concept_id = cs.concept_id