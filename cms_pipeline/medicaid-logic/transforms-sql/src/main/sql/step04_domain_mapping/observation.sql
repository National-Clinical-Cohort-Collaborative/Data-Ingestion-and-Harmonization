CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/observation` AS
    
    --all codes from all source (ip, lt, ot, rx) where target domain id is observation - icd10cms/ hcpcs / ICD10 PCS / ndc? not in D, deprecated or deleted
    with ipdx as (
    --  key columns patient / start and end dates / dx and pcs / care site and service provider
    --     ICD10CM - ADMTG_DGNS_CD, DGNS_CD_1-12 and ICD10PCS - PRCDR_CD_1 - 6
        SELECT DISTINCT
        --pkey as medicaid_person_id 
        PSEUDO_ID as medicaid_person_id
      , BLG_PRVDR_NPI as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(ADMSN_DT, 'ddMMMyyyy') as observation_date -- use start date
      , ipdx.visit_concept_id
      , xw.target_concept_id as observation_concept_id
      , xw.source_concept_id as observation_source_concept_id
      , xw.source_concept_code as observation_source_value
      , 'IP' as source_domain
     --ip_dx_long_prepared
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ip_dx_long_prepared` ipdx ------------------use melted long when ready
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ipdx.src_code = xw.cms_src_code  and xw.target_domain_id = 'Observation'
    ----where  ipdx.src_code is not null and DSCHRGDT is not null and xw.src_code_type = 'dx'
    where  ipdx.src_code is not null and xw.mapped_code_system = 'ICD10CM' ---if discharge date is null do we add?

    ), 
    ippx as (--ICD10PCS - PRCDR_CD_1 - 6
      
        SELECT DISTINCT
        --pkey as medicaid_person_id 
        PSEUDO_ID as medicaid_person_id
      , BLG_PRVDR_NPI as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(ADMSN_DT, 'ddMMMyyyy') as observation_date -- use start date
      , visit_concept_id
      , xw.target_concept_id as observation_concept_id
      , xw.source_concept_id as observation_source_concept_id
      , xw.source_concept_code as observation_source_value
      , 'IP' as source_domain
    --/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ip_px_long_prepared
    FROM `ri.foundry.main.dataset.4fa0af1c-b45e-4e5e-bd29-6365da31a5a8` ippx ------------------use melted long when ready
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ippx.src_code = xw.cms_src_code  and xw.target_domain_id = 'Observation' ----------------use ippcs.src_code 
    ----where  ipdx.src_code is not null and DSCHRGDT is not null and xw.src_code_type = 'dx'
    where  ippx.src_code is not null and xw.mapped_code_system = 'ICD10PCS' ---if discharge date is null do we add?----------------ippcs.src_code

    ),
    --ltdx--ICD10CM - ADMTG_DGNS_CD and ICD10CM - DGNS_CD_1 - 5
    ltdx as (
        SELECT DISTINCT
        --pkey as medicaid_person_id 
        PSEUDO_ID as medicaid_person_id
      , BLG_PRVDR_NPI as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi  
      , to_date(start_date_value, 'ddMMMyyyy') as observation_date -- use start date
      , to_date(end_date_value, 'ddMMMyyyy') as observation_end_date -- for join to visit_detail later
      ----lt is long term care visits or home health
     , visit_concept_id
      , xw.target_concept_id as observation_concept_id
      , xw.source_concept_id as observation_source_concept_id
      , xw.source_concept_code as observation_source_value
      , 'LT' as source_domain
    --lt_dx_long_prepared` ltdx ------------------use melted long when ready
    FROM `ri.foundry.main.dataset.66b02062-9730-49e0-b5e1-9cc04ed76a2c` ltdx
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ltdx.src_code = xw.cms_src_code  and xw.target_domain_id = 'Observation'
    where  ltdx.src_code is not null and xw.mapped_code_system = 'ICD10CM' ---if discharge date is null do we add? -------use ltdx.src_code
    ),
    --otdx -- ICD10CM - DGNS_CD_1 - 2
    otdx as (
      SELECT DISTINCT
        --pkey as medicaid_person_id 
        PSEUDO_ID as medicaid_person_id
      , BLG_PRVDR_NPI as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as observation_date -- use start date
      , visit_concept_id   
      , xw.target_concept_id as observation_concept_id
      , xw.source_concept_id as observation_source_concept_id
      , xw.source_concept_code as observation_source_value
      , 'OT' as source_domain
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ot_dx_long_prepared` otdx ------------------use melted long when ready
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON otdx.src_code = xw.cms_src_code  and xw.target_domain_id = 'Observation'
    ----where  ipdx.src_code is not null and DSCHRGDT is not null and xw.src_code_type = 'dx'
    where  otdx.src_code is not null and xw.mapped_code_system = 'ICD10CM'----------use src_code when ready in the melted dataset
    ),
    --othcpcs --* CPT or HCPCS - LINE_PRCDR_CD ---------check if LINE_PRCDR_CD_DT is same as SRVC_BGN_DT
    othcpcs as (
    SELECT DISTINCT
        --pkey as medicaid_person_id 
        PSEUDO_ID as medicaid_person_id
      , BLG_PRVDR_NPI as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as observation_date -- use start date
      ----would be helpful to add the visit concept id in the melted long datasets-----------
      , visit_concept_id
      , xw.target_concept_id as observation_concept_id
      , xw.source_concept_id as observation_source_concept_id
      , xw.source_concept_code as observation_source_value
      , 'OT' as source_domain
    FROM `ri.foundry.main.dataset.9987237b-6b2b-410d-9547-fba7aa0827e1` othcpcs ------------------use melted long when ready
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON othcpcs.LINE_PRCDR_CD = xw.cms_src_code  and xw.target_domain_id = 'Observation'
    ----where  ipdx.src_code is not null and DSCHRGDT is not null and xw.src_code_type = 'dx'
    where  othcpcs.LINE_PRCDR_CD is not null and xw.mapped_code_system = 'HCPCS'----------use src_code when ready in the melted dataset
    ),
    --------------------------------------visits --------------------------------
    ipdx_visit as (
    SELECT DISTINCT
      ipdx.medicaid_person_id
    , ipdx.care_site_npi as care_site_id
    , ipdx.provider_npi as provider_id
    , ipdx.observation_date
    , ipdx.observation_concept_id
    , ipdx.observation_source_concept_id
    , ipdx.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    ---, 0 as visit_concept_id -----add to the logic 
    , ipdx.source_domain
    , CAST(null as long) AS visit_detail_id
    from ipdx
    JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
      on ipdx.medicaid_person_id = v.person_id
      and ipdx.observation_date = v.visit_start_date
      ---and ipdx.provider_npi = v.provider_id 
      and ipdx.care_site_npi = v.care_site_id ---location_id -------------
      and v.source_domain = 'IP'
      ---and v.visit_concept_id = ipdx.visit_concept_id -----------------
    ), 

    ippx_visit as (
    SELECT DISTINCT
      ippx.medicaid_person_id
    , ippx.care_site_npi as care_site_id
    , ippx.provider_npi as provider_id
    , ippx.observation_date
    , ippx.observation_concept_id
    , ippx.observation_source_concept_id
    , ippx.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    ---, 0 as visit_concept_id -----add to the logic 
    , ippx.source_domain
    , CAST(null as long) AS visit_detail_id
    from ippx
    JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
      on ippx.medicaid_person_id = v.person_id
      and ippx.observation_date = v.visit_start_date
      ---and ippx.provider_npi = v.provider_id 
      and ippx.care_site_npi = v.care_site_id ---location_id -------------
      and v.source_domain = 'IP'
      ---and v.visit_concept_id = ippx.visit_concept_id -----------------
    ), 
    ltdx_visit as (
      SELECT DISTINCT
      ltdx.medicaid_person_id
    , ltdx.care_site_npi as care_site_id
    , ltdx.provider_npi as provider_id
    , vo.visit_end_date --, ltdx.observation_date
    , ltdx.observation_concept_id
    , ltdx.observation_source_concept_id
    , ltdx.observation_source_value
    , vd.visit_occurrence_id 
    , vd.visit_detail_concept_id
    , ltdx.source_domain
    , CAST(null as long) AS visit_detail_id -- , vd.visit_detail_id
    from ltdx
    JOIN `ri.foundry.main.dataset.1da3baeb-61cc-428d-9728-7e001f8dde6f` vd
      on ltdx.medicaid_person_id = vd.person_id
      and ltdx.observation_date = vd.visit_detail_start_date -- 38k
      -- and ltdx.provider_npi = vd.provider_id 
      and coalesce(ltdx.care_site_npi, 0) = coalesce(vd.care_site_id, 0)  ---location_id -------------
      and vd.source_domain = 'LT' -- not domain-agnostic?
      ---and vd.visit_concept_id = ltdx.visit_concept_id
    JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` vo
      ON vo.visit_occurrence_id = vd.visit_occurrence_id
    ),
    otdx_visit as (
      SELECT DISTINCT
      otdx.medicaid_person_id
    , otdx.care_site_npi as care_site_id
    , otdx.provider_npi as provider_id
    , otdx.observation_date
    , otdx.observation_concept_id
    , otdx.observation_source_concept_id
    , otdx.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , otdx.source_domain
    , CAST(null as long) AS visit_detail_id
    from otdx
    JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
      on otdx.medicaid_person_id = v.person_id
      and otdx.observation_date = v.visit_start_date
      ---and otdx.provider_npi = v.provider_id 
      and otdx.care_site_npi = v.care_site_id ---location_id -------------
      and v.source_domain = 'OT'
      ---and v.visit_concept_id = otdx.visit_concept_id
    ),
    othcpcs_visit as (
      SELECT DISTINCT
      othcpcs.medicaid_person_id
    , othcpcs.care_site_npi as care_site_id
    , othcpcs.provider_npi as provider_id
    , othcpcs.observation_date
    , othcpcs.observation_concept_id
    , othcpcs.observation_source_concept_id
    , othcpcs.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , othcpcs.source_domain
    , CAST(null as long) AS visit_detail_id
    from othcpcs
    JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
      on othcpcs.medicaid_person_id = v.person_id
      and othcpcs.observation_date = v.visit_start_date
     --- and othcpcs.provider_npi = v.provider_id 
      and othcpcs.care_site_npi = v.care_site_id ---location_id -------------
      and v.source_domain = 'OT'
      ---and v.visit_concept_id = othcpcs.visit_concept_id
    ),
    final as (
    select distinct * from ipdx_visit
    union 
    select distinct * from ippx_visit
    union 
    select distinct * from ltdx_visit
    union 
    select distinct * from otdx_visit
    union
    select distinct * from othcpcs_visit
    ),
    medicaid_observation as (
    select distinct
     md5(concat_ws(
              ';'
        , COALESCE(f.medicaid_person_id, '')
        , COALESCE(f.provider_id, '')
        , COALESCE(f.care_site_id, '')
        , COALESCE(f.observation_date, '')
        , COALESCE(f.observation_concept_id, '')
        , COALESCE(f.observation_source_concept_id, '')
        , COALESCE(f.observation_source_value, '')
        , COALESCE(f.source_domain, '')
        , COALESCE(visit_occurrence_id, '')
        , COALESCE('medicaid', '')
        )) as medicaid_hashed_observation_id
        ---, COALESCE(co.source_domain, ''), co.visit_concept_id ) as observation_id -- should also add the visit_concept_id----------TODO
        , f.medicaid_person_id AS person_id
            , observation_concept_id
            , observation_date
            , cast( observation_date as timestamp ) as observation_datetime
            , 32810 as observation_type_concept_id
            , cast( null as float ) as value_as_number
            , cast( null as string ) as value_as_string
            , cast( null as int) as value_as_concept_id
            , cast( null as int) as qualifier_concept_id
            , cast( null as int) as unit_concept_id
            , f.provider_id as provider_id
            , visit_occurrence_id 
            , visit_detail_id 
            , observation_source_value
            , observation_source_concept_id
            , cast( null as string) as unit_source_value
            , cast( null as string) as qualifier_source_value
            , f.source_domain
  from final f
  where observation_concept_id is not null
  --left join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/person` p
  --on f.BID = p.person_id
  -- match visit on bid, provider_id, visit start_date and end_date, visit_concept_id( that are all unique based on the source domain, 32810= visit_concept_id is used for claims data.  
    )
    SELECT
    o.*
    , cast(conv(substr(medicaid_hashed_observation_id, 1, 15), 16, 10) as bigint) as observation_id
    , m.macro_visit_long_id
    FROM medicaid_observation o
    left join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit_with_micro_visit` m on m.visit_occurrence_id = o.visit_occurrence_id

 

