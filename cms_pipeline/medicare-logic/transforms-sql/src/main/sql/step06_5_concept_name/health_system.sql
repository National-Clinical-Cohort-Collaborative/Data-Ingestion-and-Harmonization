CREATE TABLE `ri.foundry.main.dataset.01b7b859-b153-4f22-82b9-7dfe2d32cf19` AS
    SELECT DISTINCT 
        hsys.health_system_id,
        hsys.compendium_year,
        hsys.care_site_name,
        hsys.place_of_service_concept_id,
        hsys.location_id,
        hsys.care_site_source_value,
        hsys.place_of_service_source_value,
        hsys.teaching_intensity,
        hsys.teaching_intensity_category,
        hsys.multistate,
        hsys.multistate_category,
        hsys.care_site_size,
        hsys.care_site_size_category,
        hsys.health_system_hash_id,
        cs.care_site_id,
        lnk.compendium_year AS chsp_compendium_year, -- this will be used in the joins
        lnk.chsp_health_sys_id AS chsp_health_sys_id,
        lnk.chsp_ccn AS chsp_ccn
    FROM `ri.foundry.main.dataset.8302aba2-d751-4157-806a-6af46f8fe783` hsys
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/care_site_link2_health_system` lnk
    ON hsys.ccn = lnk.chsp_ccn AND hsys.health_sys_id = lnk.chsp_health_sys_id AND hsys.compendium_year = lnk.compendium_year
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/care_site` cs
    ON hsys.ccn = cs.orig_care_site_id
    where lnk.compendium_year is not null and lnk.chsp_health_sys_id is not null and lnk.chsp_ccn is not null 
