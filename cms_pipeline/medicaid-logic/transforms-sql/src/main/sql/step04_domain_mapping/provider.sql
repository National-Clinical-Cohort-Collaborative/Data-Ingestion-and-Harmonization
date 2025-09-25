CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/provider` AS
    with ip_providers as (
    --- providers in ip
    --- BLG_PRVDR_NPI is the billing entity - do not use in the provider/ they are added to the care_site. 
    -- SELECT DISTINCT 
    --     BLG_PRVDR_NPI as provider_id
    --   FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
    --   WHERE CHAR_LENGTH(trim(ip.BLG_PRVDR_NPI)) > 0

    --SRVC_PRVDR_NPI -- The National Provider Identifier (NPI) of the health care professional who delivers or completes a particular medical service or non-surgical procedure.
    SELECT DISTINCT 
        SRVC_PRVDR_NPI as provider_id
        , SRVC_PRVDR_SPCLTY_CD as specialty_code
      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
      WHERE CHAR_LENGTH(trim(SRVC_PRVDR_NPI)) > 0
    UNION
     -- The National Provider ID (NPI) of the provider who performed the surgical procedure(s).	Operating Provider NPI
    SELECT DISTINCT 
        OPRTG_PRVDR_NPI as provider_id
        , BLG_PRVDR_SPCLTY_CD as specialty_code
      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
      WHERE CHAR_LENGTH(trim(OPRTG_PRVDR_NPI)) > 0
    UNION
    --ADMTG_PRVDR_NPI - empty 23.9k -- The National Provider ID (NPI) of the doctor responsible for admitting a patient to a hospital or other inpatient health facility.
    SELECT DISTINCT 
        ADMTG_PRVDR_NPI as provider_id
        , ADMTG_PRVDR_SPCLTY_CD as specialty_code
      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
      WHERE CHAR_LENGTH(trim(ADMTG_PRVDR_NPI)) > 0
    UNION
    ---- RFRG_PRVDR_NPI - empty 34.4 -- Referring provider
    SELECT DISTINCT 
      RFRG_PRVDR_NPI as provider_id
       -- , RFRG_PRVDR_SPCLTY_CD as specialty_code -- dropped from the source claim dataset
       ,cast (null as string ) as specialty_code
      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
      WHERE CHAR_LENGTH(trim(RFRG_PRVDR_NPI)) > 0
    )

    SELECT DISTINCT 
    --2.1k unique, so far we do not need to create domain id using the provider npi 10 digit num
    -- one provider may have more than one specialty concept id the the specialty source value -- which should we pick?
    provider_id
    --, COALESCE(nppes.PROV_ORG_NAME_LEGAL_BUS_NAME, (PROV_NAME_PREFIX_TEXT ||' ' ||PROV_1ST_NAME||' ' ||PROV_MIDDLE_NAME||' ' ||PROV_LAST_NAME_LEGAL_NAME || ' ' ||PROV_CREDENTIAL_TEXT) )as provider_name
    , CAST((LTRIM(PROV_NAME_PREFIX_TEXT ) ||' ' ||PROV_1ST_NAME||' ' ||PROV_MIDDLE_NAME||' ' ||PROV_LAST_NAME_LEGAL_NAME || ' ' ||PROV_CREDENTIAL_TEXT) as string ) as provider_name
    , provider_id as npi
    , CAST( NULL as string ) as dea
    --, COALESCE( PROV_ORG_NAME_LEGAL_BUS_NAME, nppes.OTHER_PROV_ID_ISSUER_1) as specialty_concept_text
    --, CAST(vx.target_concept_id as integer) as specialty_concept_id
    --TODO: one provider may have more than one specialty concept id the the specialty source value -- which should we pick?
    , CAST(0 as integer) as specialty_concept_id
    , CAST (NULL AS long) as care_site_id --location where provider practice?
    , CAST (NULL AS integer) as year_of_birth 
    , case when trim(PROV_GENDER_CODE) ='M' then 8507
        when trim(PROV_GENDER_CODE) ='F' then 8532
        else 0
        end as gender_concept_id
    , provider_id as provider_source_value 
    , CAST(0 as integer) as specialty_source_concept_id
    -- TODO: one provider may have more than one specialty concept id the the specialty source value -- which should we pick?
    --, CAST((ip.specialty_code ||'|'|| vx.source_concept_name) as string)  as specialty_source_value
    , CAST( NULL AS string )  as specialty_source_value
    , PROV_GENDER_CODE as gender_source_value
    , CAST(0 as integer) as gender_source_concept_id
    FROM ip_providers ip
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/cms_medicaid_place_of_visit_xwalk`  vx on vx.source_concept_code = ip.specialty_code
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/nppes_puf_20220925.csv` nppes on nppes.NPI = ip.provider_id
    where CHAR_LENGTH(trim(PROV_GENDER_CODE))  > 0  -- if gender is specified then it is likely a physician provider
    