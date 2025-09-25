
CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/person` AS

    SELECT p.*, 
    gender.concept_name as gender_concept_name,
    eth.concept_name as ethnicity_concept_name,
    race.concept_name as race_concept_name
    FROM `ri.foundry.main.dataset.4c3109e1-103c-41ab-8600-3d409c960879` p
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` gender
    ON p.gender_concept_id = gender.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` eth
    ON p.ethnicity_concept_id = eth.concept_id 
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` race
    ON p.race_concept_id = race.concept_id
    
   
