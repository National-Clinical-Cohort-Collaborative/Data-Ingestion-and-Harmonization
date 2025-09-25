CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/unit_test/medicaid_care_site_characterization` AS

    --collect all providers mentioned in the medicaid claim source files.
    WITH all_providers AS ( 
    --- ip ---------------------------------
    SELECT DISTINCT 
        ADMTG_PRVDR_ID as care_site_or_provider_id
      , ADMTG_PRVDR_NPI as care_site_or_provider_npi 
      , CAST( 8717 AS int ) as place_of_service_concept_id
       , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Inpatient Facility' as place_of_service_source_value
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip
    UNION 
    select distinct 
      BLG_PRVDR_ID as care_site_or_provider_id
      , BLG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 8717 AS int ) as place_of_service_concept_id
       , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Inpatient Facility' as place_of_service_source_value
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip
    UNION
      select distinct 
      cast( null as string ) as care_site_or_provider_id
      , OPRTG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 8717 AS int ) as place_of_service_concept_id
       , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Inpatient Facility' as place_of_service_source_value
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip
    UNION
    select distinct 
      RFRG_PRVDR_ID as care_site_or_provider_id
      , RFRG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 8717 AS int ) as place_of_service_concept_id
       , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Inpatient Facility' as place_of_service_source_value
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip
    UNION
    select distinct 
      SRVC_PRVDR_ID as care_site_or_provider_id
      , SRVC_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 8717 AS int ) as place_of_service_concept_id
       , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Inpatient Facility' as place_of_service_source_value
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip
    UNION 
    --- ot ---------------------------------
    SELECT DISTINCT 
      BLG_PRVDR_ID as care_site_or_provider_id
      , BLG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 8756 AS int ) as place_of_service_concept_id
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Outpatient Facility' as place_of_service_source_value
      , 'OT' as source_domain
    FROM `ri.foundry.main.dataset.7996f1b7-6f17-421a-b6a1-39aded1c3e4a` ot
    UNION
    select distinct 
      cast(null as string) as care_site_or_provider_id
      , HLTH_HOME_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 8756 AS int ) as place_of_service_concept_id
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Outpatient Facility' as place_of_service_source_value
      , 'OT' as source_domain
    FROM `ri.foundry.main.dataset.7996f1b7-6f17-421a-b6a1-39aded1c3e4a` ot
    UNION
    select distinct 
      RFRG_PRVDR_ID as care_site_or_provider_id
      , RFRG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 8756 AS int ) as place_of_service_concept_id
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Outpatient Facility' as place_of_service_source_value
      , 'OT' as source_domain
    FROM `ri.foundry.main.dataset.7996f1b7-6f17-421a-b6a1-39aded1c3e4a` ot
    UNION
    select distinct 
      SRVC_PRVDR_ID as care_site_or_provider_id
      , SRVC_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 8756 AS int ) as place_of_service_concept_id
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Outpatient Facility' as place_of_service_source_value
      , 'OT' as source_domain
    FROM `ri.foundry.main.dataset.7996f1b7-6f17-421a-b6a1-39aded1c3e4a` ot
    UNION
    ----lt --------------------------
    SELECT DISTINCT 
      ADMTG_PRVDR_ID as care_site_or_provider_id
      , ADMTG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 42898160 AS int ) as place_of_service_concept_id
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Long Term Care Facility' as place_of_service_source_value
      , 'LT' as source_domain
     FROM `ri.foundry.main.dataset.b11642ff-0086-4c35-aaa1-a908841b55f0` lt
     UNION
       SELECT DISTINCT 
       BLG_PRVDR_ID as care_site_or_provider_id
       , BLG_PRVDR_NPI as care_site_or_provider_npi
       , CAST( 42898160 AS int ) as place_of_service_concept_id
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Long Term Care Facility' as place_of_service_source_value
      , 'LT' as source_domain
       FROM `ri.foundry.main.dataset.b11642ff-0086-4c35-aaa1-a908841b55f0` lt
     UNION
     SELECT DISTINCT 
      RFRG_PRVDR_ID as care_site_or_provider_id
      , RFRG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 42898160 AS int ) as place_of_service_concept_id
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Long Term Care Facility' as place_of_service_source_value
      , 'LT' as source_domain
     FROM `ri.foundry.main.dataset.b11642ff-0086-4c35-aaa1-a908841b55f0` lt
     UNION
     SELECT DISTINCT 
      SRVC_PRVDR_ID as care_site_or_provider_id
      , SRVC_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 42898160 AS int ) as place_of_service_concept_id
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value 
      , 'Long Term Care Facility' as place_of_service_source_value
      , 'LT' as source_domain
     FROM `ri.foundry.main.dataset.b11642ff-0086-4c35-aaa1-a908841b55f0` lt
    UNION
    --- rx--------------------------
    SELECT DISTINCT 
       BLG_PRVDR_ID as care_site_or_provider_id
      , BLG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 38004338 AS int ) as place_of_service_concept_id ---pharmacy
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Pharmacy' as place_of_service_source_value
      , 'RX' as source_domain
    FROM `ri.foundry.main.dataset.292106b5-cd0c-4a32-bc2d-6f9369ffac0b` rx
    UNION
    SELECT DISTINCT 
       DSPNSNG_PRVDR_ID as care_site_or_provider_id
      , DSPNSNG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 38004338 AS int ) as place_of_service_concept_id ---pharmacy
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Pharmacy' as place_of_service_source_value
      , 'RX' as source_domain
    FROM `ri.foundry.main.dataset.292106b5-cd0c-4a32-bc2d-6f9369ffac0b` rx
      UNION
    SELECT DISTINCT 
       PRSCRBNG_PRVDR_ID as care_site_or_provider_id
      , PRSCRBNG_PRVDR_NPI as care_site_or_provider_npi
      , CAST( 38004338 AS int ) as place_of_service_concept_id ---pharmacy
      , BLG_PRVDR_SPCLTY_CD as provider_specialty_cd_source_value
      , 'Pharmacy' as place_of_service_source_value
      , 'RX' as source_domain
    FROM `ri.foundry.main.dataset.292106b5-cd0c-4a32-bc2d-6f9369ffac0b` rx
    )

    SELECT DISTINCT 
    -- one npi may be associated with multiple place of service
    -- id + npi
  md5(concat_ws(
              ';'
          , COALESCE(cs.care_site_or_provider_id, '')
          , COALESCE(cs.care_site_or_provider_npi, '') 
          , COALESCE(cs.place_of_service_concept_id, '') 
          , COALESCE(xp.target_concept_id, '')
          , COALESCE(xp.target_concept_name, '')
          , COALESCE(xp.source_concept_id, '')
          , COALESCE(xp.source_concept_name, '')
          , COALESCE(cs.provider_specialty_cd_source_value, '')
          , COALESCE(cs.place_of_service_source_value, '')
          , COALESCE( chsp.HOSPITAL_NAME, '')
          , COALESCE( nppes.PROV_ORG_NAME_LEGAL_BUS_NAME, '') 
          , COALESCE( compendium2.health_sys_name, '')      
          )) as pkey
      , care_site_or_provider_id as care_site_or_provider_id
    , care_site_or_provider_npi as care_site_or_provider_npi
    , place_of_service_concept_id as claim_source_place_of_service_concept_id
    , xp.target_concept_id as place_of_service_concept_id
    , xp.target_concept_name as place_of_service_concept_name
    , xp.source_concept_id as place_of_service_source_concept_id
    , xp.source_concept_name as place_of_service_source_concept_name
    , xp.source_concept_code as place_of_serivce_source_concept_code
    , provider_specialty_cd_source_value
    , place_of_service_source_value as claim_source_place_of_service_source_value
    , l.location_id
    --chsp
    , chsp.HOSPITAL_NAME as provider_name
    , chsp.HEALTH_SYS_ID as provider_health_sys_id
    , chsp.HEALTH_SYS_NAME as provider_health_sys_name
    , chsp.HEALTH_SYS_CITY as provider_health_sys_city
    , chsp.HEALTH_SYS_STATE as provider_health_sys_state
    --nppes
    , nppes.PROV_ORG_NAME_LEGAL_BUS_NAME as provider_org_name
    , nppes.PROV_OTHER_ORG_NAME as provider_other_org_name
    , nppes.PROV_BUS_ADDR_CITY_NAME as provider_city
    , nppes.PROV_BUS_MAIL_ST_NAME as provider_state
    , nppes.PROV_BUS_ADDR_POSTAL_CODE as provider_zipcode
    , nppes.AUTH_OFFICIAL_CREDENTIAL_TEXT as provider_credential_text
    --compendium2
    , compendium2.health_sys_name
    , compendium2.health_sys_id
    , compendium2.health_sys_city
    , compendium2.health_sys_state
    , compendium2.in_onekey
    , compendium2.in_aha
    , compendium2.onekey_id
    , compendium2.aha_sysid
    , compendium2.total_mds
    , compendium2.prim_care_mds
    , compendium2.grp_cnt
    , compendium2.hosp_cnt
    , compendium2.acutehosp_cnt
    , compendium2.nh_cnt
    , compendium2.sys_multistate
    , compendium2.sys_beds
    , compendium2.sys_dsch
    , compendium2.sys_res
    , compendium2.maj_inv_owned
    , compendium2.deg_children
    , compendium2.sys_incl_majteachhosp
    , compendium2.sys_incl_vmajteachhosp
    , compendium2.sys_teachint
    , compendium2.sys_incl_highdpphosp
    , compendium2.sys_highucburden
    , compendium2.sys_incl_highuchosp
    , compendium2.sys_anyins_product
    , compendium2.sys_mcare_adv
    , compendium2.sys_mcaid_mngcare
    , compendium2.sys_healthins_mktplc
    , source_domain
    FROM all_providers cs
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/cms_medicaid_place_of_visit_xwalk` xp on xp.source_concept_code = cs.provider_specialty_cd_source_value
    LEFT JOIN `ri.foundry.main.dataset.85e2a030-8709-4e01-a95c-9b007fecea77` chsp on cs.care_site_or_provider_id = chsp.CCN
    LEFT JOIN `ri.foundry.main.dataset.e3aa1d2e-2577-48f1-9975-68373ee9736c` nppes on nppes.NPI = cs.care_site_or_provider_npi
    ----LEFT JOIN `ri.foundry.main.dataset.f1ebc1c6-cec8-4fee-ae1a-860acda97cef` vx on vx.source_concept_code = ip.BLG_PRVDR_SPCLTY_CD
    LEFT JOIN `ri.foundry.main.dataset.668d570c-c0b4-4b10-9b4b-00c1cf23598f` compendium2
    --LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/compendium/chsp-compendium-2021-12Sept2023` compendium2 - file got moved
    on chsp.HEALTH_SYS_ID = compendium2.health_sys_id
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location` l on l.location_id = CAST(trim(cs.care_site_or_provider_npi) as long)
    WHERE CHAR_LENGTH(trim(cs.care_site_or_provider_id)) > 0 or CHAR_LENGTH(trim(cs.care_site_or_provider_npi)) > 0 