CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/care_site_link2_health_system` AS
    SELECT DISTINCT  
    c.care_site_id,
    c.orig_care_site_id, 
    chsp.ccn as chsp_ccn, 
    chsp.health_sys_id as chsp_health_sys_id, 
    chsp.compendium_year,
    p.health_sys_id as cs_year_health_sys_id, 
    chsp.compendium_year as chsp_compendium_year
    FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/care_site` c
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/characterization files/chsp-hospital-linkage_20-21-22_2024-02-21` chsp
    on c.ccn = chsp.ccn 
    LEFT JOIN `ri.foundry.main.dataset.711bdb10-2e9a-4786-954f-7e793a95df93` p
    on chsp.health_sys_id = p.health_sys_id

