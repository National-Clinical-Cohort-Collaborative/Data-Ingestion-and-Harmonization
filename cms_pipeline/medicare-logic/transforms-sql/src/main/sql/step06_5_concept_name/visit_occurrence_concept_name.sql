CREATE TABLE `ri.foundry.main.dataset.8a94aaa8-6619-42fc-8d3d-8cd8793ae00a` AS

    SELECT    v.*
            , c.concept_name as visit_concept_name
    FROM `ri.foundry.main.dataset.0b72bbc5-8752-44a7-8b6a-5df973bfed85` v
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON v.visit_concept_id = c.concept_id
