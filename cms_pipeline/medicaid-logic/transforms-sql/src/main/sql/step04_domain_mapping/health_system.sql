CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/health_system` AS
    with provider_linkage as 
    ( 
        -- join in linkage file provided by the Acumen
    SELECT cs.* 
    , pc.NPI
    , pc.PECOS_ASCT_CNTL_ID
    , pcccn.PECOS_ASCT_CNTL_ID as CCN_PECOS_ASCT_CNTL_ID
    , pcccn.CCN
    FROM `ri.foundry.main.dataset.792e64d0-ca52-4c6b-9f7f-bd88dca342ff` cs
    -- npi and pecos id
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/pecos_npi.csv` pc
    on cs.care_site_npi = pc.NPI
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/pecos_ccn.csv` pcccn
    on pc.PECOS_ASCT_CNTL_ID = pcccn.PECOS_ASCT_CNTL_ID
    ),
    
--- link provider with the chsp-hospital linkage file to retrive the health system ids  
    hospital_linkage as( 
    SELECT DISTINCT 
    -- create a health system id using the unique qualifiers
    pl.care_site_id 
    , care_site_name
    , care_site_npi
    , pl.CCN as pl_ccn
    -- from the linkage dataset
    , link.ccn as chsp_ccn
    , link.health_sys_id as chsp_health_sys_id
    , link.compendium_year as chsp_compendium_year
    , compendium_hospital_id
    FROM provider_linkage pl
    INNER JOIN `ri.foundry.main.dataset.d24703a2-1d42-424f-8e33-6aed5981f7c9` link -- provides us with the health_sys_id that we can reference
    ON link.ccn = pl.CCN 
    where pl.CCN is not null and link.health_sys_id is not null 
    ),
    health_system_characteristics as (
    --- build the information about the health system characteristics
    select DISTINCT
    hosp.*
    -- from the compendium dataset
    , chsp_compendium_id
    , compendium_year as comp_compendium_year
    , health_sys_id as comp_health_sys_id
    , multistate
    , multistate_category
    , care_site_size
    , care_site_size_category
    , teaching_intensity
    , teaching_intensity_category
    FROM hospital_linkage hosp
   --- contains the information about the health system characteristics
    INNER JOIN   `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/characterization files/pipeline/transform/clean/clean_chsp_compendium` comp
    on hosp.chsp_compendium_year = comp.compendium_year and hosp.chsp_health_sys_id = comp.health_sys_id
    where hosp.pl_ccn is not null and health_sys_id is not NULL
    ), 
    final_health_system as (---final health system characteristics dataset
    SELECT DISTINCT 
    md5(concat_ws(
              ';'
        , COALESCE(care_site_id, '')     
        , COALESCE(chsp_ccn, '')
        , COALESCE( chsp_compendium_id, '')
        , COALESCE(comp_compendium_year, '')
        , COALESCE(comp_health_sys_id, '')
        )) as health_system_hashed_id    
    , teaching_intensity 
    , teaching_intensity_category
    , multistate
    , multistate_category
    , care_site_size
    , care_site_size_category
    , care_site_id -- already hashed
    ---- identifying 
    , chsp_ccn
    , chsp_compendium_id
    , chsp_compendium_year as care_site_year
    , chsp_health_sys_id
    FROM health_system_characteristics
    )

    SELECT DISTINCT 
    cast(conv(substr(health_system_hashed_id, 1, 15), 16, 10) as bigint) as health_system_id
    , teaching_intensity 
    , teaching_intensity_category
    , multistate
    , multistate_category
    , care_site_size
    , care_site_size_category
    , CAST( null AS string) AS care_site_name
    , CAST( null as int ) AS place_of_service_concept_id
    , CAST( null as long ) as location_id
    , CAST( null as string ) as care_site_source_value
    , CAST( null as string ) as place_of_service_source_value
    , care_site_year
    , care_site_id 
    , health_system_hashed_id
    --, "CMS_Medicaid" as data_source
    FROM final_health_system