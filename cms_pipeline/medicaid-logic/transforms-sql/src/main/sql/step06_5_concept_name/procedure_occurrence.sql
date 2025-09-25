CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06_5 - add concept name/procedure_occurrence` AS
 

    SELECT p.*, c.concept_name as procedure_concept_name, cs.concept_name as procedure_source_concept_name
    FROM `ri.foundry.main.dataset.46599e6c-aed0-4a1a-b27f-14ef567bf176` p
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON p.procedure_concept_id = c.concept_id 
     LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON p.procedure_source_concept_id = cs.concept_id
    