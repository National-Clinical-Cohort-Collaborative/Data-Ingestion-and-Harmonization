
   CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/drug_exposure` AS

    SELECT d.*, c.concept_name as drug_concept_name, cs.concept_name as drug_source_concept_name
    FROM `ri.foundry.main.dataset.3ebbb824-6c85-49f3-ac85-2f61f8dc5fd8` d
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON d.drug_concept_id = c.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON d.drug_source_concept_id = cs.concept_id
    