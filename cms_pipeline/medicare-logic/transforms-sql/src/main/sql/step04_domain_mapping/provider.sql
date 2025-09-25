CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/provider` AS
    -- use ri.foundry.main.dataset.e3aa1d2e-2577-48f1-9975-68373ee9736c -- nppes_puf_20220925.csv
    -- ip

 with cms_npi as (
    SELECT distinct 
        ORGNPINM as npi
    FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6` ip
    UNION
    SELECT distinct 
        AT_NPI as npi
    FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6` ip
    UNION
    SELECT distinct 
        OP_NPI as npi
    FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6` ip
    UNION
    SELECT distinct 
        OT_NPI as npi
    FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6` ip
    UNION
    SELECT distinct 
    R_NPI as npi
    FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6` ip
    UNION
    SELECT distinct 
    ORD_NPI as npi
    from `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0` dm
    UNION
    SELECT distinct 
    SUP_NPI as npi  --some source files have PLCSRVC = place_of_service
    from `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0` dm
    UNION
    SELECT distinct 
    ORGNPINM as npi
    from `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4` hh
    UNION
    SELECT distinct 
    AT_NPI as npi
    from `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4` hh
    UNION
    SELECT distinct 
    OP_NPI as npi
    from `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4` hh
    UNION
    SELECT distinct 
    OT_NPI as npi
    from `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4` hh
    UNION
    SELECT distinct 
    R_NPI as npi
    from `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4` hh
    UNION
    SELECT distinct 
    RF_NPI as npi
    from `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4` hh
    UNION
    SELECT distinct 
    SRVCNPI as npi
    from `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4` hh
    UNION
    SELECT distinct 
    ORGNPINM as npi
    from `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823` hs
    UNION
    SELECT distinct 
    AT_NPI as npi
    from `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823` hs
    UNION
    SELECT distinct 
    OP_NPI as npi
    from `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823` hs
    UNION
    SELECT distinct 
    OT_NPI as npi
    from `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823` hs
    UNION
    SELECT distinct 
    R_NPI as npi
    from `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823` hs
    UNION
    SELECT distinct 
    RF_NPI as npi
    from `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823` hs
    UNION
    SELECT distinct 
    SRVCNPI as npi
    from `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823` hs
    UNION
    SELECT distinct 
    ORGNPINM as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    
    UNION
    SELECT distinct 
    AT_NPI as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    UNION
    SELECT distinct 
    OP_NPI as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    UNION
    SELECT distinct 
    OT_NPI as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    UNION
    SELECT distinct 
    R_NPI as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    UNION
    SELECT distinct 
    RF_NPI as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    UNION
    SELECT distinct 
    SRVCNPI as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    UNION
    SELECT distinct 
    REVNPI as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    UNION 
    SELECT distinct 
    ORDRGNPI as npi
    from `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
    UNION
    SELECT distinct 
    RFR_NPI as npi
    from `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    UNION
    SELECT distinct 
    CPO_NPI as npi
    from `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    UNION
    SELECT distinct 
    BLGNPI as npi
    from `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    UNION
    SELECT distinct 
    PRFNPI as npi
    from `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    UNION
    SELECT distinct 
    PRGRPNPI as npi
    from `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    UNION
    SELECT distinct 
    MDPPNPI as npi
    from `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    UNION
    SELECT distinct 
    ORGNPINM as npi
    from `ri.foundry.main.dataset.a983c4ae-0a51-470d-9286-e19c1193be74` sn
    UNION
    SELECT distinct 
    AT_NPI as npi
    from `ri.foundry.main.dataset.a983c4ae-0a51-470d-9286-e19c1193be74` sn
    UNION
    SELECT distinct 
    OP_NPI as npi
    from `ri.foundry.main.dataset.a983c4ae-0a51-470d-9286-e19c1193be74` sn
    UNION
    SELECT distinct 
    OT_NPI as npi
    from `ri.foundry.main.dataset.a983c4ae-0a51-470d-9286-e19c1193be74` sn
    UNION
    SELECT distinct 
    sn.R_NPI as npi
    from `ri.foundry.main.dataset.a983c4ae-0a51-470d-9286-e19c1193be74` sn
    UNION
    SELECT distinct 
    pde.PROD_SERVICE_ID as npi
    from `ri.foundry.main.dataset.654c7522-38f1-4ca6-8a09-62abbbbb1991` pde
    UNION
    SELECT distinct 
    pde.SRVC_PROVIDER_ID as npi
    from `ri.foundry.main.dataset.654c7522-38f1-4ca6-8a09-62abbbbb1991` pde
    UNION 
    SELECT distinct 
    TAX_NUM_ID as npi
    from `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0` dm
)
select * from 
(
 SELECT DISTINCT 
        cast( cms_npi.npi as long) as provider_id
        ---npi can be 999999999A or PAPERCLAIM or 01-0238552 will contain alpha numeric or dash - like characters or empty string
        --- , CAST(cms_npi.npi AS string) as npi -- some npi number will be alpha numeric 
        , CAST(null  AS string) as npi
        /* join with the nppes_puf_20220925.csv file */
        -- provider_name --1. PROV_ORG_NAME_LEGAL_BUS_NAME if not - PROV_LAST_NAME_LEGAL_NAME --- either one of the two) 
        ---, COALESCE(nppes.PROV_ORG_NAME_LEGAL_BUS_NAME, concat_ws(',', COALESCE(nppes.PROV_LAST_NAME_LEGAL_NAME, ''), COALESCE(nppes.PROV_1ST_NAME,'')) ) as provider_name
        , CAST( null as string ) as provider_name
        , CAST( NULL AS string) as dea
        , CAST( null  as int) as specialty_concept_id
        , CAST( NULL AS long) as care_site_id
        , CAST( null AS int) as year_of_birth
        , CAST( NULL AS int ) as gender_concept_id /* M F null - PROV_GENDER_CODE */
        , CAST( null AS string) as provider_source_value
       --- CMS Data & Repository-analysis-provider-characterization-step01_parsed-pec_enrlmt_npi.csv 
       --- more than one value is associated with the npi number
        , CAST( null AS string) as specialty_source_value 
        , CAST(null as int) as specialty_source_concept_id
        , CAST(NULL as string ) as gender_source_value
        , CAST( NULL as int) as gender_source_concept_id
        , CAST(null AS string) as specialty_concept_name  /* also can use -  provider_concept.concept_name*/ 
        , CAST( NULL AS string ) as gender_concept_name
    FROM cms_npi
    where cms_npi.npi is not null
) a
where a.provider_id is not null 

/*** following information will be built separately for Lok for analysis as provider_ext dataset 10/31/22--------*/ 
    -- all_npi as (
    -- SELECT DISTINCT 
    --  md5(concat_ws(
    --             ';'
    --         , COALESCE('cms', '')
    --         , COALESCE(cms_npi.npi, '')
    --         , COALESCE(provider_concept.concept_id, '')
    --         , COALESCE(specialty.SPECIALTY, '')
    --         , COALESCE(nppes.PROV_GENDER_CODE, '')
    --         , COALESCE(specialty.SPECIALTY_CD, '')
    --         )) AS cms_hashed_provider_id
    --     , CAST(cms_npi.npi AS string) as npi
    --     /* join with the nppes_puf_20220925.csv file */
    --     -- provider_name --1. PROV_ORG_NAME_LEGAL_BUS_NAME if not - PROV_LAST_NAME_LEGAL_NAME --- either one of the two) 
    --     , COALESCE(nppes.PROV_ORG_NAME_LEGAL_BUS_NAME, concat_ws(',', COALESCE(nppes.PROV_LAST_NAME_LEGAL_NAME, ''), COALESCE(nppes.PROV_1ST_NAME,'')) ) as provider_name
    --     , CAST( NULL AS string) as dea
    --     , CAST( provider_concept.concept_id as int) as specialty_concept_id
    --     , CAST( NULL AS long) as care_site_id
    --     , CAST( null AS int) as year_of_birth
    --     , case WHEN nppes.PROV_GENDER_CODE = 'M' THEN 8507
    --         WHEN nppes.PROV_GENDER_CODE = 'F' THEN 8532 
    --         ELSE 0 
    --         END as gender_concept_id /* M F null - PROV_GENDER_CODE */
    --     , CAST(cms_npi.npi AS string) as provider_source_value
    --     , CAST(COALESCE(specialty.SPECIALTY, '') AS string) as specialty_source_value
    --     , CAST(null as int) as specialty_source_concept_id
    --     , CAST(nppes.PROV_GENDER_CODE as string ) as gender_source_value
    --     , CAST( NULL as int) as gender_source_concept_id
    --     , CAST(specialty.SPECIALTY AS string) as specialty_concept_name  /* also can use -  provider_concept.concept_name*/ 
    --     , case WHEN nppes.PROV_GENDER_CODE = 'M' THEN 'Male'
    --         WHEN nppes.PROV_GENDER_CODE = 'F' THEN 'Female' 
    --         ELSE cast(null as string)
    --         END as gender_concept_name
    -- FROM cms_npi
    -- LEFT JOIN `ri.foundry.main.dataset.e3aa1d2e-2577-48f1-9975-68373ee9736c` nppes
    -- on nppes.NPI = cms_npi.npi

    -- LEFT JOIN `ri.foundry.main.dataset.077b8e6c-0629-4b3a-8dfc-f4b6dbd4699f` specialty /* grab only the npi and speciatly first and then do the join here */
    -- on cms_npi.npi = specialty.NPI /*- pec_enrlmt_npi.csv CONTAINS npi and SPECIALTY, enrollment id, dt_year, specialty_cd */ 

    -- LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` provider_concept
    -- ON specialty.SPECIALTY_CD = provider_concept.concept_code and provider_concept.domain_id = 'Provider' and provider_concept.vocabulary_id = 'Medicare Specialty' 
    -- where cms_npi.npi is not null AND nppes.NPI is not null  
    -- )
