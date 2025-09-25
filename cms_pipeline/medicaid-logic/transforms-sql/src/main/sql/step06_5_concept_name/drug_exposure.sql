CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06_5 - add concept name/drug_exposure` AS

    SELECT d.*, c.concept_name as drug_concept_name, cs.concept_name as drug_source_concept_name
    FROM `ri.foundry.main.dataset.7cb51575-f09f-4448-89d3-8e03729afd68` d
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON d.drug_concept_id = c.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON d.drug_source_concept_id = cs.concept_id
    