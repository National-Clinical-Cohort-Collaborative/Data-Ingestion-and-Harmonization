CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/care_site_characterization` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, EXECUTOR_CORES_EXTRA_LARGE, DRIVER_MEMORY_EXTRA_EXTRA_LARGE') AS
--/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/care_site
--ccn number in the chsp hospital linkage file seem to match with the PROVIDER number from the source files
--- and ccn number from the file chsp-hospital-linkage_2018.csv file match on PROVIDER and CCN
--- The following info is pulled from the linkage file: hospital name, health system name, health_sys_city, total number of md counts, and primary_care_mds_cnt
--- The place_of_service_concept_id is inferred from the claims source files. 

WITH all_providers AS ( 
 
    SELECT DISTINCT 
      PROVIDER as care_site_id 
      , CAST( 8717 AS int ) as place_of_service_concept_id
      , 'Inpatient Facility' as place_of_service_source_value
    FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6`
    UNION 

    SELECT DISTINCT 
      PROVIDER as care_site_id 
       , CAST( 8756 AS int ) as place_of_service_concept_id
      , 'Outpatient Facility' as place_of_service_source_value
    FROM `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a`
    UNION

    SELECT DISTINCT 
      PROVIDER as care_site_id 
      , CAST( 8546 AS int ) as place_of_service_concept_id
      , 'Hospice Facility' as place_of_service_source_value
      FROM `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823`
    UNION

    SELECT DISTINCT 
      PROVIDER as care_site_id
      , CAST( 8863 AS int ) as place_of_service_concept_id
      , 'Skilled Nursing Facility' as place_of_service_source_value
    FROM `ri.foundry.main.dataset.a983c4ae-0a51-470d-9286-e19c1193be74`
    UNION
    SELECT DISTINCT 
      PROVIDER as care_site_id 
     , CAST( 581476 AS int ) as place_of_service_concept_id
      , 'Home Visit' as place_of_service_source_value
    FROM `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4`
    -- tax num may be associated with a provider /person
    -- UNION
    -- SELECT TAX_NUM as care_site_id FROM `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0`
    -- UNION
    -- SELECT TAX_NUM as care_site_id FROM `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f`
    -- UNION 
    -- SELECT PRESCRIBER_ID as care_site_id FROM  `ri.foundry.main.dataset.654c7522-38f1-4ca6-8a09-62abbbbb1991`
    -- UNION
    -- SELECT SRVC_PROVIDER_ID as care_site_id FROM  `ri.foundry.main.dataset.654c7522-38f1-4ca6-8a09-62abbbbb1991`
    WHERE PROVIDER is not null  
), 

  care_site_ext as (
    SELECT DISTINCT 
    care_site_id  /* cms provider is the care_site_id in OMOP **/
    , chsp.ccn
    , chsp.hospital_name as care_site_name
    , chsp.health_sys_id as chsp_health_sys_id
    , chsp.health_sys_name as chsp_health_sys_name
    -- compendium columns 
    , chsp_compendium_id
    , compendium_hashed_id
    , compendium.compendium_year
    , compendium.health_sys_id
    , compendium.health_sys_name
    , compendium.health_sys_city
    , compendium.health_sys_state
    , in_onekey
    , in_aha
    , onekey_id
    , aha_sysid
    , multistate
    , multistate_category
    , care_site_size
    , care_site_size_category
    , teaching_intensity
    , teaching_intensity_category
    , total_mds
    , prim_care_mds
    , total_nps
    , total_pas
    , grp_cnt
    , hosp_cnt
    , acutehosp_cnt
    , nh_cnt
    , sys_multistate
    , sys_beds
    , sys_dsch
    , sys_res
    , deg_children
    , sys_incl_majteachhosp
    , sys_incl_vmajteachhosp
    , sys_teachint
    , sys_incl_highdpphosp
    , sys_highucburden
    , sys_incl_highuchosp
    , sys_anyins_product
    , sys_mcare_adv
    , sys_mcaid_mngcare
    , sys_healthins_mktplc
    , sys_ma_plan_contracts
    , sys_ma_plan_enroll
    , sys_ownership
    , compendium.hos_net_revenue
    , compendium.hos_total_revenue
    , ranking
    FROM all_providers cs
    ---use newest /UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/characterization files/chsp-hospital-linkage_20-21-22_2024-02-21
    LEFT JOIN `ri.foundry.main.dataset.d24703a2-1d42-424f-8e33-6aed5981f7c9` chsp on cs.care_site_id = chsp.ccn
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/characterization files/pipeline/transform/clean/clean_chsp_compendium` compendium 
    on chsp.health_sys_id = compendium.health_sys_id
    where compendium.health_sys_id is not null and ccn is not null 
    --and ranking = 1
    ), 

    care_site_char as (
    select distinct 
    md5(concat_ws(
              ';'
        , COALESCE(care_site_id, '')      
        , COALESCE(health_sys_id, '')
        , COALESCE(compendium_year, '')
        , COALESCE(ranking, '') 
        , COALESCE( chsp_compendium_id, '')
        , COALESCE( hos_net_revenue, '')
        , COALESCE( hos_total_revenue, '')
        , COALESCE( care_site_name, '')
        , COALESCE( chsp_health_sys_name, '')
        )) as 
    care_site_char_hash_id, 
    *
    from care_site_ext
    )

    SELECT
    cast(conv(substr(care_site_char_hash_id, 1, 15), 16, 10) as bigint) as care_site_characeristic_id,
    *
    FROM care_site_char