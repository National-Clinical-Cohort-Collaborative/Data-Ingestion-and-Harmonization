CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06_5 - add concept name/device_exposure` AS

    SELECT d.*, c.concept_name as device_concept_name, cs.concept_name as device_source_concept_name
    FROM `ri.foundry.main.dataset.e7606f88-597f-4aed-9250-a92d5fa81c2f` d
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON d.device_concept_id = c.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` cs
    ON d.device_source_concept_id = cs.concept_id
    