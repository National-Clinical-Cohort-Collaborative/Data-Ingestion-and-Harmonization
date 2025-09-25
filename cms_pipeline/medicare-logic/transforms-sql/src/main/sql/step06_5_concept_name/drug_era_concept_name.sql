CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/drug_era` AS

    SELECT d.*, c.concept_name as drug_concept_name
    FROM `ri.foundry.main.dataset.881a36fa-959a-4317-92b9-811ac7004b9e` d
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON d.drug_concept_id = c.concept_id 