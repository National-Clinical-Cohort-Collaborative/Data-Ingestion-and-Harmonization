CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/measurement` AS
    --source from ot/hcpcs // ip icd10cm // lt icd10cm 
    with ot_hcpcs as(
        SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
       --- ot service may not have care site, use srvc prvdr npi if null  
      ,  COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as start_date
      , to_date(SRVC_END_DT, 'ddMMMyyyy') as end_date
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , visit_concept_id
      , 'OT' as source_domain
    FROM `ri.foundry.main.dataset.9987237b-6b2b-410d-9547-fba7aa0827e1` ot
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ot.src_code = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where ot.src_code is not null and xw.mapped_code_system in ( 'HCPCS', 'CPT4')
    ),
    ot_hcpcs_visit as (
    SELECT DISTINCT
      ot.medicaid_person_id
    , ot.care_site_npi as care_site_id
    , ot.provider_npi as provider_id
    , ot.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , ot.target_concept_id as measurement_concept_id
    , ot.source_concept_id as measurement_source_concept_id
    , ot.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , ot.source_domain
    , CAST(null as long) AS visit_detail_id
  FROM ot_hcpcs ot
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    ON  ot.medicaid_person_id = v.person_id 
    AND ot.start_date = v.visit_start_date
    AND ot.end_date = v.visit_end_date
    and ot.care_site_npi = v.care_site_id 
    and v.source_domain = 'OT'
   ), 
   ip_dx as (
       SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
       --- ot service may not have care site, use srvc prvdr npi if null  
      ,  COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(ADMSN_DT, 'ddMMMyyyy') as start_date
      , to_date(DSCHRG_DT, 'ddMMMyyyy') as end_date
      --, ADMSN_HR
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , visit_concept_id
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.f1559e91-2441-456e-b831-a7ef4103fc23` ip
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ip.src_code = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where ip.src_code is not null and xw.mapped_code_system ='ICD10CM'
   ),
    ip_dx_visit as (
     SELECT DISTINCT
      ip.medicaid_person_id
    , ip.care_site_npi as care_site_id
    , ip.provider_npi as provider_id
    , ip.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , ip.target_concept_id as measurement_concept_id
    , ip.source_concept_id as measurement_source_concept_id
    , ip.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , ip.source_domain
    , CAST(null as long) AS visit_detail_id
  FROM ip_dx ip
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    ON  ip.medicaid_person_id = v.person_id 
    AND ip.start_date = v.visit_start_date
    AND ip.end_date = v.visit_end_date
    and ip.care_site_npi = v.care_site_id 
    and v.source_domain = 'IP'
    ), 
    lt_dx as (
        SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
       --- ot service may not have care site, use srvc prvdr npi if null  
      ,  COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as start_date
      , to_date(SRVC_END_DT, 'ddMMMyyyy') as end_date
      --, ADMSN_HR
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , visit_concept_id
      , 'LT' as source_domain
    FROM `ri.foundry.main.dataset.66b02062-9730-49e0-b5e1-9cc04ed76a2c` lt
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON lt.src_code = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where lt.src_code is not null and xw.mapped_code_system ='ICD10CM'
    ),
    lt_dx_visit as (
    SELECT DISTINCT
      lt.medicaid_person_id
    , lt.care_site_npi as care_site_id
    , lt.provider_npi as provider_id
    , vo.visit_end_date as measurement_date
    , CAST(vo.visit_end_date as TIMESTAMP) as measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , lt.target_concept_id as measurement_concept_id
    , lt.source_concept_id as measurement_source_concept_id
    , lt.source_value as measurement_source_value
    , vd.visit_detail_concept_id as visit_concept_id
    , vd.visit_occurrence_id 
    , lt.source_domain
    , CAST(null as long) AS visit_detail_id -- , vd.visit_detail_id
    from lt_dx lt
    JOIN `ri.foundry.main.dataset.1da3baeb-61cc-428d-9728-7e001f8dde6f` vd
      on lt.medicaid_person_id = vd.person_id
      and lt.start_date = vd.visit_detail_start_date
      and lt.end_date = vd.visit_detail_end_date
      -- and ltdx.provider_npi = vd.provider_id 
      and lt.care_site_npi = vd.care_site_id ---location_id -------------
      and vd.source_domain = 'LT' 
      ---and vd.visit_concept_id = ltdx.visit_concept_id
    JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` vo
      ON vo.visit_occurrence_id = vd.visit_occurrence_id
    ), 
    ip_px as (
        SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
       --- ot service may not have care site, use srvc prvdr npi if null  
      ,  COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(ADMSN_DT, 'ddMMMyyyy') as start_date
      , to_date(DSCHRG_DT, 'ddMMMyyyy') as end_date
      --, ADMSN_HR
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , visit_concept_id
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.4fa0af1c-b45e-4e5e-bd29-6365da31a5a8` px
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON px.src_code = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where px.src_code is not null and xw.mapped_code_system ='ICD10PCS'

    ),
    ip_px_visit AS (
        SELECT DISTINCT
      ip.medicaid_person_id
    , ip.care_site_npi as care_site_id
    , ip.provider_npi as provider_id
    , ip.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , ip.target_concept_id as measurement_concept_id
    , ip.source_concept_id as measurement_source_concept_id
    , ip.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , ip.source_domain
    , CAST(null as long) AS visit_detail_id
  FROM ip_px ip
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    ON  ip.medicaid_person_id = v.person_id 
    AND ip.start_date = v.visit_start_date
    AND ip.end_date = v.visit_end_date
    and ip.care_site_npi = v.care_site_id 
    and v.source_domain = 'IP'
    ),
    final_measurement as (
       select * from ot_hcpcs_visit
       UNION 
       select * from ip_dx_visit
       UNION 
       select * from ip_px_visit 
       UNION
       select * FROM lt_dx_visit 
   ),
   medicaid_measurement as (
        select  distinct 
        md5(concat_ws(
              ';'
        , COALESCE(medicaid_person_id, '')
        , COALESCE(care_site_id, '')
        , COALESCE(provider_id, '')
        , COALESCE(measurement_date, '')
        , COALESCE(measurement_concept_id, '')
        , COALESCE(measurement_source_concept_id, '')
        , COALESCE(measurement_source_value, '')
        , COALESCE(visit_concept_id, '')
        , COALESCE(source_domain, '')
        , COALESCE(visit_occurrence_id, '')
        , COALESCE('medicaid', '')
        )) as medicaid_hashed_measurement_id
        , medicaid_person_id as person_id
    , measurement_concept_id
    , measurement_date
    , measurement_datetime
    , measurement_time
    , measurement_type_concept_id
    , CAST(null as int) AS operator_concept_id 
    , CAST(null as float) AS value_as_number 
    , CAST(null as int) AS value_as_concept_id
    , CAST(null as int) AS unit_concept_id
    , CAST(null as float) AS range_low
    , CAST(null as float) AS range_high
    , provider_id
    , visit_occurrence_id
    , visit_detail_id  
    , CAST(null as string) AS measurement_source_value
    , measurement_source_concept_id
    ,  CAST(null as string ) AS unit_source_value
    ,  CAST(null as string ) AS value_source_value
    , source_domain
  FROM final_measurement 
  where measurement_concept_id is not null
  )
 
 SELECT
     mm.*
    , cast(conv(substr(medicaid_hashed_measurement_id, 1, 15), 16, 10) as bigint) as measurement_id
    , m.macro_visit_long_id
    FROM medicaid_measurement mm
    left join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit_with_micro_visit` m on m.visit_occurrence_id = mm.visit_occurrence_id
