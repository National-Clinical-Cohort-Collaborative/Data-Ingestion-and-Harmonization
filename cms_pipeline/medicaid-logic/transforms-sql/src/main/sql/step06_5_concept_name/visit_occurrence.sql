CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06_5 - add concept name/visit_occurrence` AS

    SELECT v.*, c.concept_name as visit_concept_name
    FROM `ri.foundry.main.dataset.b265545b-cef5-42bb-b2e3-4e63adfee7b1` v
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON v.visit_concept_id = c.concept_id 