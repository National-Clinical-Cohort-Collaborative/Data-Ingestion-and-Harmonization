CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/visit_occurrence_with_macrovisit` AS

    SELECT    v.*
            , c.concept_name as visit_concept_name
    FROM `ri.foundry.main.dataset.5275d2c9-6ac0-4363-a702-7c6f3eec3de5` v
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON v.visit_concept_id = c.concept_id
