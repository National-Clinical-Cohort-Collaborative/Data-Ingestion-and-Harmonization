 CREATE TABLE `ri.foundry.main.dataset.a8b3c95b-aa5e-4f20-9108-61041ec8ed8b` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, EXECUTOR_CORES_EXTRA_LARGE, DRIVER_MEMORY_EXTRA_EXTRA_LARGE') AS

    --- dm_hcpcs is in HCPCS_CD
    --- dm/opl/ip/hh/sn/hs - hcpcs pcs todo-ndc
    with dm_hcpcs AS (
    SELECT DISTINCT
        BID
      , TAX_NUM_ID as care_site_id
      , to_date(EXPNSDT1, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(EXPNSDT2, 'ddMMMyyyy') as device_exposure_end_date
      , cxwalk.target_concept_id as device_concept_id
      , cxwalk.source_concept_id as device_source_concept_id
      , HCPCS_CD as device_source_value
      , vxwalk.target_concept_id as cms_visit_concept_id
      , 'DM' as source_domain
    FROM `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0` dm
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON dm.HCPCS_CD = cxwalk.cms_src_code and cxwalk.target_domain_id = 'Device' 
    LEFT JOIN `ri.foundry.main.dataset.bea9f90c-f90a-4c25-a5f6-8a9d7c87fe4b` vxwalk
    ON dm.PLCSRVC = vxwalk.source_concept_code
    where dm.HCPCS_CD is not null and dm.PLCSRVC is not null
    ),
    opl_prcdrcd as (
     SELECT  DISTINCT   
        o.BID
      , o.PROVIDER as care_site_id
      , to_date(o.REV_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(o.REV_DT, 'ddMMMyyyy') as device_exposure_end_date
      , cxwalk.target_concept_id as device_concept_id
      , cxwalk.source_concept_id as device_source_concept_id
      , PRCDRCD as device_source_value
      , 'OPL' as source_domain
    FROM `ri.foundry.main.dataset.685e6bb4-19a9-41af-ba6b-eaf17ebc9244` o
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON o.PRCDRCD = cxwalk.cms_src_code and cxwalk.target_domain_id = 'Device'
    AND cxwalk.src_vocab_code = 'ICD10PCS' 
    where o.PRCDRCD is not NULL
    ), ip_pcs AS (
    SELECT DISTINCT
        BID
      , PROVIDER as care_site_id
      , to_date(ADMSN_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(DSCHRGDT, 'ddMMMyyyy') as device_exposure_end_date
      , cxwalk.target_concept_id as device_concept_id
      , cxwalk.source_concept_id as device_source_concept_id
      , cxwalk.source_concept_code as device_source_value
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.e1beec2e-37da-4a05-8094-fcf4ff6cb2a1` i
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON i.PRCDRCD = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Device' and cxwalk.src_vocab_code = 'ICD10PCS'
    where  i.PRCDRCD is not null
    ), 
    hh_hcpcs as (
    SELECT DISTINCT
        BID
      , PROVIDER as care_site_id
      , to_date(FROM_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(THRU_DT, 'ddMMMyyyy') as device_exposure_end_date
      , cxwalk.target_concept_id as device_concept_id
      , cxwalk.source_concept_id as device_source_concept_id
      , cxwalk.source_concept_code as device_source_value
      , 'HH' as source_domain
    FROM `ri.foundry.main.dataset.5684c0da-2366-4bef-aa97-2f6703137252` h
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON h.HCPSCD = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Device' and cxwalk.src_vocab_code = 'HCPCS'
    where  h.HCPSCD is not null and PMT_AMT > 0
    ), 
    sn_hcpcs as (
      SELECT DISTINCT
        BID
      , PROVIDER as care_site_id
      --, to_date(ADMSN_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(FROM_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(THRU_DT, 'ddMMMyyyy') as device_exposure_end_date
      , cxwalk.target_concept_id as device_concept_id
      , cxwalk.source_concept_id as device_source_concept_id
      , cxwalk.source_concept_code as device_source_value
      , 'SN' as source_domain
    FROM `ri.foundry.main.dataset.79489370-9e8d-4743-b0b6-da040f1f2f02` sn
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON sn.HCPSCD = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Device' and cxwalk.src_vocab_code = 'HCPCS'
    where sn.HCPSCD is not null and sn.PMT_AMT > 0
    ), 
    hs_hcpcs as (
    SELECT  DISTINCT
        BID
      --, CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(FROM_DT, 'ddMMMyyyy') as device_exposure_start_date
      , to_date(THRU_DT, 'ddMMMyyyy') as device_exposure_end_date
      , cxwalk.target_concept_id as device_concept_id
      , cxwalk.source_concept_id as device_source_concept_id
      , cxwalk.source_concept_code as device_source_value
      , 'HS' as source_domain
    FROM `ri.foundry.main.dataset.327edaac-37d2-4692-8c25-d07eb03fc21c` hs
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON hs.HCPSCD = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Device' and cxwalk.src_vocab_code = 'HCPCS'
    where hs.HCPSCD is not null and hs.PMT_AMT > 0
    ),
    -- some pb hcpcs gets mapped to device, include device source from pb claims
    pb_hcpcs as (
    SELECT DISTINCT
      BID
      , BLGNPI AS care_site_id
      , to_date(EXPNSDT1 , 'ddMMMyyyy') as device_exposure_start_date
      , to_date(EXPNSDT2 , 'ddMMMyyyy') as device_exposure_end_date
     --- , PLCSRVC
      , cxwalk.target_concept_id as device_concept_id
      , cxwalk.source_concept_id as device_source_concept_id
      , cxwalk.source_concept_code as device_source_value
      , 'PB' as source_domain
      FROM  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/pb` pb
      LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
      ON pb.HCPCS_CD = cxwalk.cms_src_code and cxwalk.target_domain_id = 'Device' and cxwalk.src_vocab_code ='HCPCS'
      where length( trim(pb.HCPCS_CD)) > 0 and pb.PMT_AMT >= 0 
    ), 
    all_visits as (
      SELECT DISTINCT
      dm.BID
    , dm.care_site_id 
    , dm.device_exposure_start_date
    , dm.device_exposure_end_date
    , dm.device_concept_id
    , dm.device_source_concept_id
    , dm.device_source_value
    , visit.visit_concept_id 
    , visit.visit_occurrence_id
    , null as visit_detail_id
    , dm.source_domain
  FROM dm_hcpcs dm
   JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
    ON dm.BID = visit.person_id
    AND dm.device_exposure_start_date = visit.visit_start_date -- dm visits are always one day from the claim
    AND dm.device_exposure_start_date = visit.visit_end_date --i.e. dm visits start and end date will match the start date of dm i.e. EXPNSDT1
    AND dm.care_site_id = visit.care_site_id
    and dm.cms_visit_concept_id = visit.visit_concept_id
    union 
    select distinct 
     op.BID
    , op.care_site_id
    , op.device_exposure_start_date
    , op.device_exposure_end_date
    , op.device_concept_id
    , op.device_source_concept_id
    , op.device_source_value
    , v.visit_concept_id
    , v.visit_occurrence_id 
    , null as visit_detail_id
    , 'OPER' AS source_domain
  FROM opl_prcdrcd op
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
  ON op.BID = v.person_id 
  AND op.device_exposure_start_date = v.visit_start_date
  AND op.device_exposure_end_date = v.visit_end_date
  --AND op.device_exposure_end_date = v.visit_end_date
  /*AND op.as cms_visit_concept_id = visit.visit_concept_id */
  AND op.care_site_id = v.care_site_id
  AND v.visit_concept_id = 9203
  UNION
   select distinct 
     op.BID
    , op.care_site_id
    , op.device_exposure_start_date
    , op.device_exposure_end_date
    , op.device_concept_id
    , op.device_source_concept_id
    , op.device_source_value
    , v.visit_concept_id
    , v.visit_occurrence_id 
    , null as visit_detail_id
    , 'OPL' AS source_domain
  FROM opl_prcdrcd op
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
  ON op.BID = v.person_id 
  AND op.device_exposure_start_date = v.visit_start_date
  AND op.device_exposure_end_date = v.visit_end_date
  AND op.care_site_id = v.care_site_id
  AND v.visit_concept_id = 9202
    union 
    SELECT DISTINCT
      ip.BID
    , ip.care_site_id
    , ip.device_exposure_start_date
    , ip.device_exposure_end_date
    , ip.device_concept_id
    , ip.device_source_concept_id
    , ip.device_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , 'IP' as source_domain
  FROM ip_pcs ip 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = ip.BID 
  AND ip.device_exposure_start_date = visit.visit_start_date
  AND ip.device_exposure_end_date = visit.visit_end_date
  AND ip.care_site_id = visit.care_site_id
  AND visit.visit_concept_id = 9201 
  UNION
   SELECT DISTINCT
      ip.BID
    , ip.care_site_id
    , ip.device_exposure_start_date
    , ip.device_exposure_end_date
    , ip.device_concept_id
    , ip.device_source_concept_id
    , ip.device_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , 'IPER' as source_domain
  FROM ip_pcs ip 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = ip.BID 
  AND ip.device_exposure_start_date = visit.visit_start_date
  AND ip.device_exposure_end_date = visit.visit_end_date
  AND ip.care_site_id = visit.care_site_id
  AND visit.visit_concept_id = 262
    union 
    SELECT DISTINCT 
      hh.BID
    , hh.care_site_id
    , hh.device_exposure_start_date
    , hh.device_exposure_end_date
    , hh.device_concept_id
    , hh.device_source_concept_id
    , hh.device_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , hh.source_domain
  FROM hh_hcpcs hh  
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = hh.BID 
  AND hh.device_exposure_start_date = visit.visit_start_date
  AND hh.device_exposure_end_date = visit.visit_end_date
  AND visit.care_site_id = hh.care_site_id
  AND visit.visit_concept_id = 581476

  union
  SELECT DISTINCT
      hs.BID
    , hs.care_site_id
    , hs.device_exposure_start_date
    , hs.device_exposure_end_date
    , hs.device_concept_id
    , hs.device_source_concept_id
    , hs.device_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , hs.source_domain
  FROM hs_hcpcs hs 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = hs.BID 
  AND hs.device_exposure_start_date = visit.visit_start_date
  AND hs.device_exposure_end_date = visit.visit_end_date
  AND visit.care_site_id = hs.care_site_id
  AND visit.visit_concept_id = 8546

    union
     SELECT DISTINCT
       sn.BID
  --   , sn.CLAIM_ID
     , sn.care_site_id
     , sn.device_exposure_start_date
     , sn.device_exposure_end_date
     , sn.device_concept_id
     , sn.device_source_concept_id
     , sn.device_source_value
     , vd.visit_detail_concept_id as visit_concept_id
     , vd.visit_occurrence_id 
     , vd.visit_detail_id
     , sn.source_domain
   FROM sn_hcpcs sn 
   JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_detail` vd
   ON vd.person_id = sn.BID 
   AND vd.visit_detail_start_date = sn.device_exposure_start_date
   AND vd.visit_detail_end_date = sn.device_exposure_end_date
   AND vd.care_site_id = sn.care_site_id
   AND vd.visit_detail_concept_id in (42898160, 9203)
   and vd.source_domain = 'SN'
     union
SELECT DISTINCT 
     hs.BID
    , hs.care_site_id
    , hs.device_exposure_start_date
    , hs.device_exposure_end_date
    , hs.device_concept_id
    , hs.device_source_concept_id
    , hs.device_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , hs.source_domain
  FROM hs_hcpcs hs
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = hs.BID 
  AND visit.visit_start_date = hs.device_exposure_start_date
  AND visit.visit_end_date = hs.device_exposure_end_date
  AND visit.care_site_id = hs.care_site_id
  AND visit.visit_concept_id = 8546
  UNION
  --add pb hcpcs that maps to device
  SELECT DISTINCT
    pb.BID
    , pb.care_site_id
    , pb.device_exposure_start_date
    , pb.device_exposure_end_date
    , pb.device_concept_id
    , pb.device_source_concept_id
    , pb.device_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , pb.source_domain
  FROM pb_hcpcs pb
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = pb.BID 
  AND visit.visit_start_date = pb.device_exposure_start_date
  AND visit.visit_end_date = pb.device_exposure_end_date
  AND visit.care_site_id = pb.care_site_id
  ),
  cms_device as (
    select distinct
    md5(concat_ws(
              ';'
        , COALESCE(f.BID, '')
        , COALESCE(f.care_site_id, '')
        , COALESCE(f.device_exposure_start_date, '')
        , COALESCE(f.device_exposure_end_date, '')
        , COALESCE(f.device_concept_id, '')
        , COALESCE(f.device_source_concept_id, '')
        , COALESCE(f.device_source_value, '')
        , COALESCE(f.source_domain, '')
        , COALESCE( visit_occurrence_id, '')
        )) as cms_hashed_device_exposure_id
    , CAST( f.BID as long) as person_id
    , CAST( f.device_concept_id AS int) device_concept_id
    , CAST( null as long ) provider_id
    , CAST( f.care_site_id as long ) care_site_id
    , CAST( f.device_exposure_start_date AS date) device_exposure_start_date
    , cast( f.device_exposure_start_date as timestamp) as device_exposure_start_datetime
    , CAST( device_exposure_end_date as date) as device_exposure_end_date
    , cast( f.device_exposure_end_date as timestamp) as device_exposure_end_datetime
    , 32810 as device_type_concept_id 
    , cast( null as string ) as unique_device_id
    , cast( null as int ) as quantity
    , CAST( f.device_source_value AS string) as device_source_value
    , CAST( f.device_source_concept_id AS int) as device_source_concept_id
    , cast( visit_occurrence_id as bigint ) as visit_occurrence_id 
    , cast( visit_detail_id as bigint ) as visit_detail_id
    , f.source_domain
   from all_visits f
   where device_concept_id is not null
   )
   SELECT
      *
    , cast(conv(substr(cms_hashed_device_exposure_id, 1, 15), 16, 10) as bigint)  as device_exposure_id
    FROM cms_device
 