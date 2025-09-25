CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location` AS


    with location as (
    SELECT DISTINCT
       location_id, 
        PSEUDO_ID as medicaid_person_id,
        CAST(null as string) as address_1, 
        CAST(null as string) as address_2, 
        CAST(null as string) as city, 
        CAST(BENE_STATE_CD as string) as state, 
        CAST(BENE_ZIP_CD as string) as zip,
        CAST(null as string) as county, 
     --   CAST(BENE_CNTY_CD ||'-'||BENE_STATE_CD || '-' || BENE_ZIP_CD as string) as location_source_value
       CAST(BENE_STATE_CD || '-' || BENE_ZIP_CD as string) as location_source_value
    FROM`/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location_history` b
    ),
    -- Build the list of care_site using the distinct NPI num, use care site NPI as the location id
    cms_care_site_location as (
        -- care_site from ip
        -- npi is different if located in different location but may share the same name
        -- same npi can have multiple place of service association 
        SELECT DISTINCT
        CAST(trim(ip.BLG_PRVDR_NPI) as long) as location_id
        , trim(ip.BLG_PRVDR_NPI) as care_site_npi
        -- SELECT DISTINCT NPIS - BLG_PRVDR_SPCLTY_CD AND source_concept_name CAUSES DUPS
        --, trim(ip.BLG_PRVDR_SPCLTY_CD) as place_of_service_code
        --, vx.source_concept_name as place_of_service_name
        -- npi at same location can have multiple place of care association, above is commented out to prevent dups
        , PROV_1ST_LINE_BUS_ADDR as address_1
        , PROV_2ND_LINE_BUS_MAIL as address_2
        , PROV_BUS_ADDR_CITY_NAME as city
        , PROV_BUS_ADDR_ST_NAME as state
        , PROV_BUS_ADDR_POSTAL_CODE as zip
        , CAST(NULL as string) as county
        , COALESCE( PROV_ORG_NAME_LEGAL_BUS_NAME, PROV_OTHER_ORG_NAME) as location_source_value
        FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/nppes_puf_20220925.csv` nppes on nppes.NPI = ip.BLG_PRVDR_NPI
        ---LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/cms_medicaid_visit_xwalk` vx on vx.source_concept_code = ip.BLG_PRVDR_SPCLTY_CD
        WHERE CHAR_LENGTH(trim(ip.BLG_PRVDR_NPI)) > 0 
        UNION
        SELECT DISTINCT
        CAST( care_site_npi as long) as location_id
        , care_site_npi
        ----, specialty_cd
        , PROV_1ST_LINE_BUS_ADDR as address_1
        , PROV_2ND_LINE_BUS_MAIL as address_2
        , PROV_BUS_ADDR_CITY_NAME as city
        , PROV_BUS_ADDR_ST_NAME as state
        , PROV_BUS_ADDR_POSTAL_CODE as zip
        , CAST(NULL as string) as county
        , COALESCE( PROV_ORG_NAME_LEGAL_BUS_NAME, PROV_OTHER_ORG_NAME) as location_source_value
        FROM  `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/care_site_2_include` care_site
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/nppes_puf_20220925.csv` nppes on nppes.NPI = care_site.care_site_npi
        ---LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/cms_medicaid_visit_xwalk` vx on vx.source_concept_code = ip.BLG_PRVDR_SPCLTY_CD
        WHERE CHAR_LENGTH(trim(care_site.care_site_npi)) > 0 

    ), 

    cms_location as (
    -- location of person     
    SELECT DISTINCT
        location_id
        , address_1
        , address_2
        , city
        , state
        , zip
        , county
        , location_source_value
    FROM location  
    UNION
    -- location of care_site entity
    SELECT DISTINCT
        location_id
        , address_1
        , address_2
        , city
        , state
        , zip
        , county
        , location_source_value
    FROM  cms_care_site_location
    )
    select DISTINCT * FROM cms_location