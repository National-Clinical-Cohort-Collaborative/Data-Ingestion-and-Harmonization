CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/measurement` AS
    --source from pb/hcpcs // pb/icd10 // ip/icd10 // hs/hcpcs // hs/icd10 // hh/icd10 // dm/icd10
    with pb_hcpcs as(
        SELECT DISTINCT
        BID as cms_person_id
      , BLGNPI as care_site_id
      , to_date(EXPNSDT1, 'ddMMMyyyy') as start_date
      , to_date(EXPNSDT2, 'ddMMMyyyy') as end_date
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , 'PB' as source_domain
    FROM `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` xw
    ON pb.HCPCS_CD = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where pb.HCPCS_CD is not null and xw.src_vocab_code in ( 'HCPCS', 'CPT4')
    ),
    pb_hcpcs_visit as (
    SELECT DISTINCT
      pb.cms_person_id
    , pb.care_site_id
    , pb.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , pb.target_concept_id as measurement_concept_id
    , pb.source_concept_id as measurement_source_concept_id
    , pb.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , pb.source_domain
    FROM pb_hcpcs pb
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    ON pb.cms_person_id = v.person_id 
    AND pb.start_date = v.visit_start_date
    AND pb.end_date = v.visit_end_date
    AND v.source_domain = 'PB'
    ),
    pb_icd10 as(
        SELECT DISTINCT
        BID as cms_person_id
      , BLGNPI as care_site_id
      , to_date(EXPNSDT1, 'ddMMMyyyy') as start_date
      , to_date(EXPNSDT2, 'ddMMMyyyy') as end_date
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , 'PB' as source_domain
    FROM `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` xw
    ON pb.PDGNS_CD = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where pb.PDGNS_CD is not null and xw.src_vocab_code in ( 'ICD10CM')
    ),
    pb_icd10_visit as (
    SELECT DISTINCT
      pb.cms_person_id
    , pb.care_site_id
    , pb.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , pb.target_concept_id as measurement_concept_id
    , pb.source_concept_id as measurement_source_concept_id
    , pb.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , pb.source_domain
    FROM pb_icd10 pb
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    ON pb.cms_person_id = v.person_id 
    AND pb.start_date = v.visit_start_date
    AND pb.end_date = v.visit_end_date
    AND v.source_domain = 'PB'
    ),
    ip_icd10 as(
        SELECT DISTINCT 
        BID as cms_person_id
      , PROVIDER as care_site_id
      , to_date(ADMSN_DT, 'ddMMMyyyy') as start_date
      , to_date(DSCHRGDT, 'ddMMMyyyy') as end_date
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6` ip
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` xw
    ON ip.PDGNS_CD = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where ip.PDGNS_CD is not null and xw.src_vocab_code in ( 'ICD10CM')
    ),
    ip_icd10_visit as (
    SELECT DISTINCT
      ip.cms_person_id
    , ip.care_site_id
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
    FROM ip_icd10 ip
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    ON ip.cms_person_id = v.person_id 
    AND ip.start_date = v.visit_start_date
    AND ip.end_date = v.visit_end_date
    AND v.source_domain = 'IP'
    ),
    hs_hcpcs as(
        SELECT DISTINCT
        BID as cms_person_id
      , PROVIDER as care_site_id
      , to_date(FROM_DT, 'ddMMMyyyy') as start_date
      , to_date(THRU_DT, 'ddMMMyyyy') as end_date
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , 'HS' as source_domain
    FROM `ri.foundry.main.dataset.327edaac-37d2-4692-8c25-d07eb03fc21c` hs
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` xw
    ON hs.HCPSCD = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where hs.HCPSCD is not null and xw.src_vocab_code in ( 'HCPCS', 'CPT4')
    ),
    hs_hcpcs_visit as (
    SELECT DISTINCT
      hs.cms_person_id
    , hs.care_site_id
    , hs.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , hs.target_concept_id as measurement_concept_id
    , hs.source_concept_id as measurement_source_concept_id
    , hs.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , hs.source_domain
    FROM hs_hcpcs hs
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    ON hs.cms_person_id = v.person_id 
    AND hs.start_date = v.visit_start_date
    AND hs.end_date = v.visit_end_date
    AND v.source_domain = 'HS'
    ),
    hs_icd10 as(
        SELECT DISTINCT
        BID as cms_person_id
      , PROVIDER as care_site_id
      , to_date(FROM_DT, 'ddMMMyyyy') as start_date
      , to_date(THRU_DT, 'ddMMMyyyy') as end_date
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , 'HS' as source_domain
    FROM `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823` hs
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` xw
    ON hs.PDGNS_CD = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where hs.PDGNS_CD is not null and xw.src_vocab_code in ( 'ICD10CM')
    ),
    hs_icd10_visit as (
    SELECT DISTINCT
      hs.cms_person_id
    , hs.care_site_id
    , hs.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , hs.target_concept_id as measurement_concept_id
    , hs.source_concept_id as measurement_source_concept_id
    , hs.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , hs.source_domain
    FROM hs_icd10 hs
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    ON hs.cms_person_id = v.person_id 
    AND hs.start_date = v.visit_start_date
    AND hs.end_date = v.visit_end_date
    AND v.source_domain = 'HS'
    ),
    hh_icd10 as(
        SELECT DISTINCT
        BID as cms_person_id
      , PROVIDER as care_site_id
      , to_date(FROM_DT, 'ddMMMyyyy') as start_date
      , to_date(THRU_DT, 'ddMMMyyyy') as end_date
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , 'HH' as source_domain
    FROM `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4` hh
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` xw
    ON hh.PDGNS_CD = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where hh.PDGNS_CD is not null and xw.src_vocab_code in ( 'ICD10CM')
    ),
    hh_icd10_visit as (
    SELECT DISTINCT
      hh.cms_person_id
    , hh.care_site_id
    , hh.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , hh.target_concept_id as measurement_concept_id
    , hh.source_concept_id as measurement_source_concept_id
    , hh.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , hh.source_domain
    FROM hh_icd10 hh
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    ON hh.cms_person_id = v.person_id 
    AND hh.start_date = v.visit_start_date
    AND hh.end_date = v.visit_end_date
    AND v.source_domain = 'HH'
    ),
    dm_icd10 as(
        SELECT DISTINCT
        BID as cms_person_id
      , TAX_NUM_ID as care_site_id
      , to_date(EXPNSDT1, 'ddMMMyyyy') as start_date
      , to_date(EXPNSDT2, 'ddMMMyyyy') as end_date
      , xw.target_concept_id 
      , xw.source_concept_id 
      , xw.cms_src_code as source_value
      , 'DM' as source_domain
    FROM `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0` dm
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` xw
    ON dm.PDGNS_CD = xw.cms_src_code and xw.target_domain_id = 'Measurement' 
    where dm.PDGNS_CD is not null and xw.src_vocab_code in ( 'ICD10CM')
    ),
    dm_icd10_visit as (
    SELECT DISTINCT
      dm.cms_person_id
    , dm.care_site_id
    , dm.start_date as measurement_date
    , CAST(start_date as TIMESTAMP) AS measurement_datetime
    , CAST(null as string) AS measurement_time
    , 32810 as measurement_type_concept_id
    , dm.target_concept_id as measurement_concept_id
    , dm.source_concept_id as measurement_source_concept_id
    , dm.source_value as measurement_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , dm.source_domain
    FROM dm_icd10 dm
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    ON dm.cms_person_id = v.person_id 
    AND dm.start_date = v.visit_start_date
    AND dm.start_date = v.visit_end_date
    --AND dm.end_date = v.visit_end_date - dm visits are only 1 day of encounter regardless of the start and end date of the claim, therefore match on start date
    AND v.source_domain = 'DM'
    ),

    final_measurement as (
       select * from pb_hcpcs_visit
       UNION 
       select * from pb_icd10_visit
       UNION 
       select * from ip_icd10_visit
       UNION 
       select * from hs_hcpcs_visit
       UNION 
       select * from hs_icd10_visit
       UNION 
       select * from hh_icd10_visit
       UNION 
       select * from dm_icd10_visit
   ),

   cms_measurement as (
        select  distinct 
        md5(concat_ws(
              ';'
        , COALESCE(cms_person_id, '')
        --, COALESCE(care_site_id, '')
        , COALESCE(care_site_id, '')
        , COALESCE(measurement_date, '')
        , COALESCE(measurement_concept_id, '')
        , COALESCE(measurement_source_concept_id, '')
        , COALESCE(measurement_source_value, '')
        , COALESCE(visit_concept_id, '')
        , COALESCE(source_domain, '')
        , COALESCE(visit_occurrence_id, '')
        , COALESCE('cms', '')
        )) as cms_hashed_measurement_id
        , cms_person_id as person_id
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
    , CAST(null AS long) as provider_id
    , CAST(care_site_id AS long) AS care_site_id
    , visit_occurrence_id
    , CAST(null as long) AS visit_detail_id
    , CAST(null as string) AS measurement_source_value
    , measurement_source_concept_id
    ,  CAST(null as string) AS unit_source_value
    ,  CAST(null as string) AS value_source_value
    , source_domain
  FROM final_measurement 
  where measurement_concept_id is not null
  )
 
 SELECT
      *
    , cast(conv(substr(cms_hashed_measurement_id, 1, 15), 16, 10) as bigint) as measurement_id
    FROM cms_measurement