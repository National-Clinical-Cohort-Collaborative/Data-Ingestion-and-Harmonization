CREATE TABLE `ri.foundry.main.dataset.e60b4299-f2f3-4135-94ec-f60572a05f3b` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, EXECUTOR_CORES_EXTRA_LARGE, DRIVER_MEMORY_EXTRA_EXTRA_LARGE') AS
/* procedures can be from both ICD10CM codes and HCPCS codes */
WITH op AS (
  SELECT DISTINCT
      BID
    , PROVIDER as care_site_id
    , to_date(REV_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(REV_DT, 'ddMMMyyyy') as procedure_end_date
    , op.HCPCS_CD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'OPL' as source_domain
    , 'HCPCS_CD' as procedure_status_source_value
    , cast(1 as int) as procedure_status_concept_id
   FROM `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` op
   LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON op.HCPCS_CD = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Procedure'  and  cxwalk.source_vocabulary_id = 'HCPCS'
    WHERE op.HCPCS_CD is not NULL  and REV_DT is not null
  
), 
pb AS (
    SELECT DISTINCT
      BID
    , BLGNPI as care_site_id /* TODO: check with Acumen this is correct to use as a provider primary key */
    , to_date(EXPNSDT1, 'ddMMMyyyy') as procedure_start_date
    , to_date(EXPNSDT2, 'ddMMMyyyy') as procedure_end_date
    , HCPCS_CD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , vxwalk.target_concept_id as cms_visit_concept_id
    , 'PB' as source_domain
    , 'HCPCS_CD' as procedure_status_source_value
    , cast(1 as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
  ON pb.HCPCS_CD = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Procedure'  and  cxwalk.source_vocabulary_id = 'HCPCS'
  LEFT JOIN `ri.foundry.main.dataset.bea9f90c-f90a-4c25-a5f6-8a9d7c87fe4b` vxwalk 
  ON pb.PLCSRVC = vxwalk.source_concept_code
  WHERE pb.HCPCS_CD is not NULL  and EXPNSDT1 is not null and EXPNSDT2 is not null
), 
dm AS (
    SELECT DISTINCT
      BID
    , TAX_NUM_ID as care_site_id /* TODO: check with Acumen this is correct to use as a provider primary key */
    , to_date(EXPNSDT1, 'ddMMMyyyy') as procedure_start_date
    , to_date(EXPNSDT2, 'ddMMMyyyy') as procedure_end_date
    , HCPCS_CD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , vxwalk.target_concept_id as cms_visit_concept_id
    , dm.PLCSRVC
    , 'DM' as source_domain
    , 'HCPCS_CD' as procedure_status_source_value
    , cast(1 as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0` dm
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk 
  ON dm.HCPCS_CD = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Procedure' and  cxwalk.source_vocabulary_id = 'HCPCS'
  LEFT JOIN `ri.foundry.main.dataset.bea9f90c-f90a-4c25-a5f6-8a9d7c87fe4b` vxwalk 
  ON dm.PLCSRVC = vxwalk.source_concept_code
  where dm.HCPCS_CD is not NULL and EXPNSDT1 is not null and EXPNSDT2 is not null
),
/* ip / sn / hh / hs have 45 HCPSCDXX columns */
ip_hcpscd AS ( /* HCPSCD01-45 and REV_DT01-45 */
  SELECT DISTINCT
      BID
    , PROVIDER as care_site_id
    , to_date(ADMSN_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(DSCHRGDT, 'ddMMMyyyy') as procedure_end_date
    , HCPSCD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'IP' as source_domain
    , HCPS_col as procedure_status_source_value
    , cast(col_num as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.774bb207-ecda-4ce6-922f-20e71cb4f55c` ip
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk 
  ON ip.HCPSCD = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Procedure' and  cxwalk.source_vocabulary_id = 'HCPCS'
  where ip.HCPSCD is not NULL and ADMSN_DT is not null and DSCHRGDT is not null
), sn_hcpscd AS (
  SELECT DISTINCT
      BID
    , PROVIDER as care_site_id
    --, to_date(ADMSN_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(FROM_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(THRU_DT, 'ddMMMyyyy') as procedure_end_date
    , HCPSCD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'SN' as source_domain
    , HCPS_col as procedure_status_source_value
    , cast(col_num as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.79489370-9e8d-4743-b0b6-da040f1f2f02` sn
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk 
  ON sn.HCPSCD = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Procedure' 
    and  cxwalk.source_vocabulary_id in ('HCPCS', 'ICD10PCS', 'CPT4')
    --and ADMSN_DT is not null  and THRU_DT is not null 
    and FROM_DT is not null  and THRU_DT is not null 
), 
hh_hcpscd AS (
  SELECT DISTINCT
      BID
    , PROVIDER as care_site_id
    , to_date(FROM_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(THRU_DT, 'ddMMMyyyy') as procedure_end_date
    , HCPSCD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'HH' as source_domain
    , HCPS_col as procedure_status_source_value
    , cast(col_num as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.5684c0da-2366-4bef-aa97-2f6703137252` hh
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk 
  ON hh.HCPSCD = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Procedure'  and  cxwalk.source_vocabulary_id = 'HCPCS'
  WHERE hh.HCPSCD is not NULL and FROM_DT is not null and THRU_DT is not null 
), 
hs_hcpscd AS (
  SELECT DISTINCT
      BID
    , PROVIDER as care_site_id
    , to_date(FROM_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(THRU_DT, 'ddMMMyyyy') as procedure_end_date
    , HCPSCD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'HS' as source_domain
    , HCPS_col as procedure_status_source_value
    , cast(col_num as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.327edaac-37d2-4692-8c25-d07eb03fc21c` hs
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk 
  ON hs.HCPSCD = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Procedure' and  cxwalk.source_vocabulary_id = 'HCPCS'
  WHERE hs.HCPSCD is not NULL and FROM_DT is not null and THRU_DT is not null
),
/* ip / opl / sn / hs have 25 PRCDRCDXX columns */
ip_prcdrcd AS (
  SELECT DISTINCT
      BID
    , PROVIDER as care_site_id
    , to_date(ADMSN_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(DSCHRGDT, 'ddMMMyyyy') as procedure_end_date
    , PRCDRCD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'IP' as source_domain
    --PRCDRCD_col
    , PRCDRCD_col as procedure_status_source_value
    , cast(col_num as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.e1beec2e-37da-4a05-8094-fcf4ff6cb2a1` ip
  INNER JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
  ON ip.PRCDRCD = cxwalk.source_concept_code AND cxwalk.source_vocabulary_id = 'ICD10PCS' and  cxwalk.target_domain_id = 'Procedure'
  where ip.PRCDRCD is not null and ADMSN_DT is not null and DSCHRGDT is not null
), 
sn_prcdrcd AS (
  SELECT
      BID
    , PROVIDER as care_site_id
    --, to_date(ADMSN_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(FROM_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(THRU_DT, 'ddMMMyyyy') as procedure_end_date
    , PRCDRCD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'SN' as source_domain
    , PRCDRCD_col as procedure_status_source_value
    , cast(col_num as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.5b365c5b-e849-460f-886a-5004b5b4523d` sn
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON (sn.PRCDRCD = cxwalk.source_concept_code AND cxwalk.source_vocabulary_id = 'ICD10PCS')
    and  cxwalk.target_domain_id = 'Procedure' -- added
  where sn.PRCDRCD is not null
), 
opl_prcdrcd AS (
  SELECT DISTINCT
      BID
    , PROVIDER as care_site_id
    , to_date(REV_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(REV_DT, 'ddMMMyyyy') as procedure_end_date
    , PRCDRCD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'OPL' as source_domain
    , PRCDRCD_col as procedure_status_source_value
    , cast(col_num as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.685e6bb4-19a9-41af-ba6b-eaf17ebc9244` opl
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
  ON opl.PRCDRCD = cxwalk.source_concept_code AND cxwalk.source_vocabulary_id = 'ICD10PCS' AND cxwalk.target_domain_id = 'Procedure'
  where opl.PRCDRCD is not null AND REV_DT is not null
), 
hs_prcdrcd AS (
  SELECT DISTINCT
      BID
    , PROVIDER as care_site_id
    , to_date(FROM_DT, 'ddMMMyyyy') as procedure_start_date
    , to_date(THRU_DT, 'ddMMMyyyy') as procedure_end_date
    , PRCDRCD as procedure_source_value
    , cxwalk.target_concept_id as procedure_concept_id
    , cxwalk.source_concept_id as procedure_source_concept_id
    , 'HS' as source_domain
    , PRCDRCD_col as procedure_status_source_value
    , cast(col_num as int) as procedure_status_concept_id
  FROM `ri.foundry.main.dataset.7d13ebf9-c2a8-4648-9ef4-4d9ed3dff48c` hs
  INNER JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
  ON hs.PRCDRCD = cxwalk.source_concept_code AND cxwalk.source_vocabulary_id = 'ICD10PCS' AND cxwalk.target_domain_id = 'Procedure'
  where hs.PRCDRCD is not null AND FROM_DT is not null and THRU_DT is not null 
),

  /* 
  We join OP events back to the visit table on BID, date of op event, and 
  visit_concept_id (which could be either 9202 or 9203) -----------------
  */
  procedure as (
    /* op */
  SELECT DISTINCT
      op.BID
    , op.care_site_id
    , op.procedure_start_date
    , op.procedure_end_date
    , op.procedure_concept_id
    , op.procedure_source_concept_id
    , op.procedure_source_value
    , v.visit_concept_id
    , v.visit_occurrence_id 
    , null as visit_detail_id
    , 'OPL' as source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM op JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
  ON op.BID = v.person_id 
  AND op.procedure_start_date = v.visit_start_date
  AND op.procedure_end_date = v.visit_end_date
  AND op.care_site_id = v.care_site_id
  AND v.visit_concept_id = 9203 --ER visits
  UNION

  SELECT DISTINCT 
      op.BID
    , op.care_site_id
    , op.procedure_start_date
    , op.procedure_end_date
    , op.procedure_concept_id
    , op.procedure_source_concept_id
    , op.procedure_source_value
    , v.visit_concept_id
    , v.visit_occurrence_id 
    , null as visit_detail_id
    ,'OPL' AS source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM op JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
  ON op.BID = v.person_id 
  AND op.procedure_start_date = v.visit_start_date
  AND op.procedure_end_date = v.visit_end_date
  AND op.care_site_id = v.care_site_id
  AND v.visit_concept_id = 9202 -- outpatient visits
  UNION

  SELECT DISTINCT
      pb.BID
    , pb.care_site_id
    , pb.procedure_start_date
    , pb.procedure_end_date
    , pb.procedure_concept_id
    , pb.procedure_source_concept_id
    , pb.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , pb.source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
    FROM pb 
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
    ON pb.BID = visit.person_id
    AND pb.procedure_start_date = visit.visit_start_date
    AND pb.procedure_end_date = visit.visit_end_date
    AND pb.care_site_id = visit.care_site_id
    and pb.cms_visit_concept_id = visit.visit_concept_id
  UNION

  SELECT DISTINCT
      dm.BID
    , dm.care_site_id
    , dm.procedure_start_date
    , dm.procedure_end_date
    , dm.procedure_concept_id
    , dm.procedure_source_concept_id
    , dm.procedure_source_value
    , visit.visit_concept_id 
    , visit.visit_occurrence_id
    , null as visit_detail_id
    , dm.source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM dm
   JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
    ON dm.BID = visit.person_id 
    AND dm.procedure_start_date = visit.visit_start_date
    AND dm.procedure_start_date = visit.visit_end_date
    AND dm.care_site_id = visit.care_site_id
    and dm.cms_visit_concept_id = visit.visit_concept_id
  UNION

  /* We join IP events back to the visit table on BID and admission date of IP event */
  SELECT DISTINCT
      ip_hcpscd.BID
    , ip_hcpscd.care_site_id
    , ip_hcpscd.procedure_start_date
    , ip_hcpscd.procedure_end_date
    , ip_hcpscd.procedure_concept_id
    , ip_hcpscd.procedure_source_concept_id
    , ip_hcpscd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , 'IP' as source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM ip_hcpscd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = ip_hcpscd.BID 
  AND visit.visit_start_date = ip_hcpscd.procedure_start_date
  AND visit.visit_end_date = ip_hcpscd.procedure_end_date
  AND ip_hcpscd.care_site_id = visit.care_site_id
  AND visit.visit_concept_id = 9201 
  UNION

    SELECT DISTINCT
      ip_hcpscd.BID
    , ip_hcpscd.care_site_id
    , ip_hcpscd.procedure_start_date
    , ip_hcpscd.procedure_end_date
    , ip_hcpscd.procedure_concept_id
    , ip_hcpscd.procedure_source_concept_id
    , ip_hcpscd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , 'IP' AS source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM ip_hcpscd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = ip_hcpscd.BID 
  AND visit.visit_start_date = ip_hcpscd.procedure_start_date
  AND visit.visit_end_date = ip_hcpscd.procedure_end_date
  AND ip_hcpscd.care_site_id = visit.care_site_id
  AND visit.visit_concept_id = 262
  UNION

  /* We join SN events back to the visit table on BID and admission date of SN event */
  SELECT DISTINCT
      sn_hcpscd.BID
    , sn_hcpscd.care_site_id
    , sn_hcpscd.procedure_start_date
    , sn_hcpscd.procedure_start_date as procedure_end_date -- fills the gap, but makes less distinct. We don't use end_date.
    -- only start_date makes into the hash that creates the PK.
    -- , sn_hcpscd.procedure_end_date
    , sn_hcpscd.procedure_concept_id
    , sn_hcpscd.procedure_source_concept_id
    , sn_hcpscd.procedure_source_value
    , vd.visit_detail_concept_id as visit_concept_id
    , vd.visit_occurrence_id 
    , vd.visit_detail_id
    , sn_hcpscd.source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM sn_hcpscd 
    JOIN `ri.foundry.main.dataset.57c52f98-102f-487a-94c6-9d2c792a8fcb` vd
    ON vd.person_id = cast(sn_hcpscd.BID as long)
    AND vd.visit_detail_start_date = sn_hcpscd.procedure_start_date
    AND vd.visit_detail_end_date = sn_hcpscd.procedure_end_date
    AND vd.care_site_id = sn_hcpscd.care_site_id
    AND (vd.visit_detail_concept_id = 42898160 or vd.visit_detail_concept_id = 9203)
    and vd.source_domain = 'SN'
  UNION 

  SELECT DISTINCT
      hh_hcpscd.BID
    , hh_hcpscd.care_site_id
    , hh_hcpscd.procedure_start_date
    , hh_hcpscd.procedure_end_date
    , hh_hcpscd.procedure_concept_id
    , hh_hcpscd.procedure_source_concept_id
    , hh_hcpscd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , hh_hcpscd.source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM hh_hcpscd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = hh_hcpscd.BID 
  AND visit.visit_start_date = hh_hcpscd.procedure_start_date
  AND visit.visit_end_date = hh_hcpscd.procedure_end_date
  AND visit.care_site_id = hh_hcpscd.care_site_id
  AND visit.visit_concept_id = 581476
  UNION

  SELECT DISTINCT
      hs_hcpscd.BID
    , hs_hcpscd.care_site_id
    , hs_hcpscd.procedure_start_date
    , hs_hcpscd.procedure_end_date
    , hs_hcpscd.procedure_concept_id
    , hs_hcpscd.procedure_source_concept_id
    , hs_hcpscd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , hs_hcpscd.source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM hs_hcpscd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = hs_hcpscd.BID 
  AND visit.visit_start_date = hs_hcpscd.procedure_start_date
  AND visit.care_site_id = hs_hcpscd.care_site_id
  AND visit.visit_concept_id = 8546
  UNION

  SELECT DISTINCT
      ip_prcdrcd.BID
    , ip_prcdrcd.care_site_id
    , ip_prcdrcd.procedure_start_date
    , ip_prcdrcd.procedure_end_date
    , ip_prcdrcd.procedure_concept_id
    , ip_prcdrcd.procedure_source_concept_id
    , ip_prcdrcd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id
    , null as visit_detail_id
    , 'IP' as source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM ip_prcdrcd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = ip_prcdrcd.BID 
  AND visit.visit_start_date = ip_prcdrcd.procedure_start_date
  AND visit.visit_end_date = ip_prcdrcd.procedure_end_date
  AND visit.care_site_id = ip_prcdrcd.care_site_id
  AND visit.visit_concept_id = 9201 
  UNION

   SELECT DISTINCT
      ip_prcdrcd.BID
    , ip_prcdrcd.care_site_id
    , ip_prcdrcd.procedure_start_date
    , ip_prcdrcd.procedure_end_date
    , ip_prcdrcd.procedure_concept_id
    , ip_prcdrcd.procedure_source_concept_id
    , ip_prcdrcd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id
    , null as visit_detail_id
    , 'IP' as source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM ip_prcdrcd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = ip_prcdrcd.BID 
  AND visit.visit_start_date = ip_prcdrcd.procedure_start_date
  AND visit.visit_end_date = ip_prcdrcd.procedure_end_date
  AND visit.care_site_id = ip_prcdrcd.care_site_id
  AND visit.visit_concept_id = 262

  UNION
  SELECT DISTINCT
      sn_prcdrcd.BID
    , sn_prcdrcd.care_site_id
    , sn_prcdrcd.procedure_start_date
    , sn_prcdrcd.procedure_start_date as procedure_end_date -- fills the gap, but makes less distinct. We don't use end_date.
    -- , sn_prcdrcd.procedure_end_date
    , sn_prcdrcd.procedure_concept_id
    , sn_prcdrcd.procedure_source_concept_id
    , sn_prcdrcd.procedure_source_value
    , vd.visit_detail_concept_id as visit_concept_id
    , vd.visit_occurrence_id 
    , vd.visit_detail_id
    , sn_prcdrcd.source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM sn_prcdrcd 
  JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_detail` vd 
  ON vd.person_id = cast(sn_prcdrcd.BID  as long)
  AND vd.visit_detail_start_date = sn_prcdrcd.procedure_start_date
  AND vd.visit_detail_end_date = sn_prcdrcd.procedure_end_date
  AND vd.care_site_id = sn_prcdrcd.care_site_id
  AND vd.visit_detail_concept_id in (42898160, 9203)
  and vd.source_domain = 'SN'
  UNION

  SELECT DISTINCT
      opl_prcdrcd.BID
    , opl_prcdrcd.care_site_id
    , opl_prcdrcd.procedure_start_date
    , opl_prcdrcd.procedure_end_date
    , opl_prcdrcd.procedure_concept_id
    , opl_prcdrcd.procedure_source_concept_id
    , opl_prcdrcd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , 'OPL'source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM opl_prcdrcd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = opl_prcdrcd.BID 
  AND visit.visit_start_date = opl_prcdrcd.procedure_start_date
  AND visit.visit_end_date = opl_prcdrcd.procedure_end_date
  AND visit.care_site_id = opl_prcdrcd.care_site_id
  AND visit.visit_concept_id = 9203 
  UNION

   SELECT DISTINCT
      opl_prcdrcd.BID
    , opl_prcdrcd.care_site_id
    , opl_prcdrcd.procedure_start_date
    , opl_prcdrcd.procedure_end_date
    , opl_prcdrcd.procedure_concept_id
    , opl_prcdrcd.procedure_source_concept_id
    , opl_prcdrcd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    ,'OPL' as source_domain
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM opl_prcdrcd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = opl_prcdrcd.BID 
  AND visit.visit_start_date = opl_prcdrcd.procedure_start_date
  AND visit.visit_end_date = opl_prcdrcd.procedure_end_date
  AND visit.care_site_id = opl_prcdrcd.care_site_id
  AND visit.visit_concept_id = 9202

  UNION

  SELECT DISTINCT
      hs_prcdrcd.BID
    , hs_prcdrcd.care_site_id
    , hs_prcdrcd.procedure_start_date
    , hs_prcdrcd.procedure_end_date
    , hs_prcdrcd.procedure_concept_id
    , hs_prcdrcd.procedure_source_concept_id
    , hs_prcdrcd.procedure_source_value
    , visit.visit_concept_id
    , visit.visit_occurrence_id 
    , null as visit_detail_id
    , hs_prcdrcd.source_domain 
    , procedure_status_source_value
    , procedure_status_concept_id
  FROM hs_prcdrcd 
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` visit
  ON visit.person_id = hs_prcdrcd.BID 
  AND visit.visit_start_date = hs_prcdrcd.procedure_start_date
  AND visit.visit_end_date = hs_prcdrcd.procedure_end_date
  AND visit.care_site_id = hs_prcdrcd.care_site_id
  AND visit.visit_concept_id = 8546
  ),
 cms_procedure as(
  select  distinct 
      md5(concat_ws(
              ';'
        , COALESCE(BID, '')
        , COALESCE(care_site_id, '')
        , COALESCE(procedure_start_date, '')
        , COALESCE(procedure_concept_id, '')
        , COALESCE(procedure_source_concept_id, '')
        , COALESCE(visit_concept_id, '')
        , COALESCE(source_domain, '')
        , COALESCE(visit_occurrence_id, '')
        , COALESCE(visit_detail_id, '') -- we don't have procedure_end_date to distinguish nearly identical rows, so use this.
        , COALESCE(procedure_status_source_value, '') --needed for HH data
        , COALESCE(procedure_status_concept_id, '')
        , COALESCE('cms', '')
        )) as cms_hashed_procedure_occurrence_id
        , cast (BID as long ) as person_id
        , cast( procedure_concept_id as int) as procedure_concept_id
        , cast( procedure_start_date as date) as procedure_date
        , cast( procedure_start_date as timestamp ) as procedure_datetime
        , 32810 as procedure_type_concept_id
        , cast( 0 as int) as modifier_concept_id
        , cast( 0 as int) quantity
        , cast( null as long) as provider_id
        , cast( care_site_id as long) as care_site_id
        , cast( visit_occurrence_id as bigint) visit_occurrence_id
        , cast(visit_detail_id as bigint ) as visit_detail_id
        , cast( procedure_source_value as string) as procedure_source_value
        , cast( procedure_source_concept_id as int) as procedure_source_concept_id
        , cast(null as string) as modifier_source_value
        , cast( procedure_status_source_value as string) as procedure_status_source_value
        , cast (procedure_status_concept_id as int) as procedure_status_concept_id
        , source_domain
  FROM procedure
  where procedure_concept_id is not null
  )
 
 SELECT
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    , cast(conv(substr(cms_hashed_procedure_occurrence_id, 1, 15), 16, 10) as bigint) as procedure_occurrence_id
    FROM cms_procedure
      