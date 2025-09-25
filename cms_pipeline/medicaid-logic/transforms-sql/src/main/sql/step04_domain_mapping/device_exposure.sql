CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/device_exposure` AS
    -- pcs, hcpcs and NDC code from ip, lt, ot and rx where target domain is Device
    -- ip pcs-* ICD10PCS - PRCDR_CD_1 - 6 / ot hcpcs - CPT or HCPCS - LINE_PRCDR_CD
    with ip_pcs AS (
    SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
      , BLG_PRVDR_NPI as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(ADMSN_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(DSCHRG_DT, 'ddMMMyyyy') as device_exposure_end_date
      , xw.target_concept_id as device_concept_id
      , xw.source_concept_id as device_source_concept_id
      , ip.src_code as device_source_value
      , visit_concept_id
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.4fa0af1c-b45e-4e5e-bd29-6365da31a5a8` ip ----------------------------- replace with melted dataset
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ip.src_code = xw.cms_src_code and xw.target_domain_id = 'Device' 
    where ip.src_code is not null and xw.mapped_code_system ="ICD10PCS"
    ),
    -- ot hcpcs -- CPT or HCPCS - LINE_PRCDR_CD
    ot_hcpcs AS (
    SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
       --- ot service may not have care site, use srvc prvdr npi if null  
      ,  COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(SRVC_END_DT, 'ddMMMyyyy') as device_exposure_end_date
      , xw.target_concept_id as device_concept_id
      , xw.source_concept_id as device_source_concept_id
      , LINE_PRCDR_CD as device_source_value
      , visit_concept_id
      , 'OT' as source_domain
    FROM `ri.foundry.main.dataset.9987237b-6b2b-410d-9547-fba7aa0827e1` othcpcs ------------------use melted long when ready
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON othcpcs.src_code = xw.cms_src_code and xw.target_domain_id = 'Device' 
    where othcpcs.src_code is not null and xw.mapped_code_system in ( 'HCPCS', 'CPT4')
    ),
    rx_ndc as (
    SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
       --- ot service may not have care site, use srvc prvdr npi if null  
      ,  COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), PRSCRBNG_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , PRSCRBNG_PRVDR_NPI as provider_npi 
      , to_date(MDCD_PD_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(MDCD_PD_DT, 'ddMMMyyyy') as device_exposure_end_date
      , xw.target_concept_id as device_concept_id
      , xw.source_concept_id as device_source_concept_id
      , rx.NDC as device_source_value
      , cast( 581458 as int) as visit_concept_id
      , 'RX' as source_domain
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/rx` rx------------------use melted long when ready
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON rx.NDC = xw.cms_src_code and xw.target_domain_id = 'Device' 
    where rx.NDC is not null and xw.mapped_code_system ='NDC'
    ),
    ---------------------visits
   ippcs_visit as (
      SELECT DISTINCT
      ip.medicaid_person_id
    , ip.provider_npi as provider_id
    , ip.device_exposure_start_date
    , ip.device_exposure_end_date
    , ip.device_concept_id
    , ip.device_source_concept_id
    , ip.device_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , ip.care_site_npi as care_site_id
    , ip.source_domain
  FROM ip_pcs ip
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    ON  ip.medicaid_person_id = v.person_id 
    AND ip.device_exposure_start_date = v.visit_start_date
    AND ip.device_exposure_end_date = v.visit_end_date
    ---AND ip.care_site_npi = v.provider_id  -- match on care site should be sufficient for ip
    and ip.care_site_npi = v.care_site_id ---location_id ----------
    ---and ip.visit_concept_id = v.visit_concept_id
    and v.source_domain = 'IP'
   ),
   othcpcs_visit as (
   SELECT DISTINCT
      ot.medicaid_person_id
    , ot.provider_npi as provider_id
    , ot.device_exposure_start_date
    , ot.device_exposure_end_date
    , ot.device_concept_id
    , ot.device_source_concept_id
    , ot.device_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , ot.care_site_npi as care_site_id
    , ot.source_domain
  FROM ot_hcpcs ot
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    ON  ot.medicaid_person_id = v.person_id 
    AND ot.device_exposure_start_date = v.visit_start_date
    AND ot.device_exposure_end_date = v.visit_end_date
    --- AND ot.provider_npi = v.provider_id -- is match on provider ? necessary?
   and ot.care_site_npi = v.care_site_id 
    ---and ot.visit_concept_id = v.visit_concept_id
    and v.source_domain = 'OT'
   ),
   rx_visit as (
      SELECT DISTINCT
      rx.medicaid_person_id
    , rx.provider_npi as provider_id
    , rx.device_exposure_start_date
    , rx.device_exposure_end_date
    , rx.device_concept_id
    , rx.device_source_concept_id
    , rx.device_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , rx.care_site_npi as care_site_id
    , rx.source_domain
  FROM rx_ndc rx
  LEFT JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    ON  rx.medicaid_person_id = v.person_id 
    AND rx.device_exposure_start_date = v.visit_start_date
    AND rx.device_exposure_end_date = v.visit_end_date
    AND rx.care_site_npi = v.care_site_id 
    AND v.source_domain = 'RX'
   ),
   lt_rx as ( -- (cribbed from Measurement)
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
    ON lt.src_code = xw.cms_src_code and xw.target_domain_id = 'Device' 
    where lt.src_code is not null and xw.mapped_code_system = 'HCPCS'
    ),
    lt_rx_visit as (
    SELECT DISTINCT
      lt.medicaid_person_id
    , lt.provider_npi as provider_id
    , vo.visit_start_date
    , vo.visit_end_date 
    , lt.target_concept_id as device_concept_id
    , lt.source_concept_id as device_source_concept_id
    , lt.source_value as device_source_value
    , vd.visit_detail_concept_id as visit_concept_id
    , vd.visit_occurrence_id 
    , lt.care_site_npi as care_site_id
    , lt.source_domain
    from lt_rx lt
    JOIN `ri.foundry.main.dataset.1da3baeb-61cc-428d-9728-7e001f8dde6f` vd
      on lt.medicaid_person_id = vd.person_id
      and lt.start_date = vd.visit_detail_start_date
      and lt.end_date = vd.visit_detail_end_date
      -- and ltdx.provider_npi = vd.provider_id 
      and lt.care_site_npi = vd.care_site_id ---location_id -------------
      and vd.source_domain = 'LT' -- not domain-agnostic?
      ---and vd.visit_concept_id = ltdx.visit_concept_id
    JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` vo
      ON vo.visit_occurrence_id = vd.visit_occurrence_id
      -- and vo.source_domain = 'LT' -- seems like visits would be domain-agnostic and the domain data would ID a source file
    ),

   final as (
    select distinct * from ippcs_visit
    union 
    select distinct * from othcpcs_visit
    UNION
    select distinct * from rx_visit
    UNION
    select distinct * from lt_rx_visit
   ),
   medicaid_device as (
    select distinct
    md5(concat_ws(
              ';'
        , COALESCE(f.medicaid_person_id, '')
        , COALESCE(f.provider_id, '')
        , COALESCE(f.device_exposure_start_date, '')
        , COALESCE(f.device_exposure_end_date, '')
        , COALESCE(f.device_concept_id, '')
        , COALESCE(f.device_source_concept_id, '')
        , COALESCE(f.device_source_value, '')
        , COALESCE(f.source_domain, '')
        , COALESCE( f.care_site_id, '')
        , COALESCE( visit_occurrence_id, '')
        , COALESCE( 'medicaid', '')
        )) as medicaid_hashed_device_exposure_id
    ,f.medicaid_person_id as person_id
    , CAST( f.device_concept_id AS int) device_concept_id
    ,f.provider_id as provider_id
    , CAST( f.device_exposure_start_date AS date) device_exposure_start_date
    , cast(f.device_exposure_start_date as timestamp) as device_exposure_start_datetime
    , CAST( device_exposure_end_date as date) as device_exposure_end_date
    , cast(f.device_exposure_end_date as timestamp) as device_exposure_end_datetime
    , 32810 as device_type_concept_id 
    , cast(null as string ) as unique_device_id
    , cast( null as int ) as quantity
    , CAST( f.device_source_value AS string) as device_source_value
    , CAST( f.device_source_concept_id AS int) as device_source_concept_id
    , cast( visit_occurrence_id as long ) as visit_occurrence_id 
    , cast( null as long ) as visit_detail_id
    , f.source_domain
    , f.care_site_id
   from final f
   )
   SELECT DISTINCT 
      d.*
    , cast(conv(substr(medicaid_hashed_device_exposure_id, 1, 15), 16, 10) as bigint)  as device_exposure_id
    , m.macro_visit_long_id
    FROM medicaid_device d
    left join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit_with_micro_visit` m on m.visit_occurrence_id = d.visit_occurrence_id

 
    

     
