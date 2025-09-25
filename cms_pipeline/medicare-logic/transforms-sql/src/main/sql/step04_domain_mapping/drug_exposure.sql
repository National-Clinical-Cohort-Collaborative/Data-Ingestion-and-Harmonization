CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/drug_exposure` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, EXECUTOR_CORES_EXTRA_LARGE, DRIVER_MEMORY_EXTRA_EXTRA_LARGE') AS

with pde_drug as (
SELECT
    CAST(BID as long) as person_id
    , RECID
    , cxwalk.target_concept_id as drug_concept_id
    , TO_DATE(RX_DOS_DT, 'ddMMMyyyy') as drug_exposure_start_date
    ,  CASE WHEN CAST(FILL_NUM as int) = 0 THEN DATE_ADD(TO_DATE(RX_DOS_DT, 'ddMMMyyyy'), CAST(DAYS_SUPPLY as int) )
          ELSE DATE_ADD(TO_DATE(RX_DOS_DT, 'ddMMMyyyy'), (CAST(DAYS_SUPPLY as int) * CAST( FILL_NUM as int) )) 
          END as drug_exposure_end_date
      ---fill_num can be 0 so check for 0 before adding dates, DATE_ADD(TO_DATE(RX_DOS_DT, 'ddMMMyyyy'), (CAST(DAYS_SUPPLY as int) * CAST( FILL_NUM as int) )) as RX_END_DT
  , CAST(null AS date) AS verbatim_end_date
  /* --- the source is claims / Prescription dispensed in pharmacy - is this always true? Comes from the OHDSI mapping file*/
  , 32869 as drug_type_concept_id  ---38000175 as drug_type_concept_id  --32810 = claims 32869 Pharmacy claim
  , CAST(null AS string) AS stop_reason
  , CAST( FILL_NUM as Integer)  as refills ---this is 2 character string need to converts to int 01 to 57
  , CAST(QUANTITY_DISPENSED AS float) as quantity
  , CAST(DAYS_SUPPLY as Integer) as days_supply
  , CAST(null AS string) AS sig
  , CAST(null as int) as route_concept_id
  , CAST(null AS string) AS lot_number
  , CAST(PRESCRIBER_ID AS long) AS care_site_id
  , CAST(NULL as long) as  visit_occurrence_id
  , CAST(null AS long) AS visit_detail_id
  , CAST(PROD_SERVICE_ID as string) as  drug_source_value
  , cxwalk.source_concept_id as drug_source_concept_id
  , CAST(SRVC_PROVIDER_ID as long) as cms_provider_id 
  , CAST( NULL as string) as route_source_value
  , CAST( NULL as string) as dose_unit_source_value
  , PHARM_SVC_TYPE
  , 'PDE' as source_domain
  FROM `ri.foundry.main.dataset.654c7522-38f1-4ca6-8a09-62abbbbb1991` pde
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON pde.PROD_SERVICE_ID = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Drug' 
    WHERE pde.PROD_SERVICE_ID is not NULL
),
-- comment deleted on June 6th, 2025
dm2drug as (
  SELECT DISTINCT 
  CAST(BID as long) as person_id
    , cxwalk.target_concept_id as drug_concept_id
    ,TAX_NUM_ID as care_site_id
    , TO_DATE(EXPNSDT1, 'ddMMMyyyy') as drug_exposure_start_date
    , TO_DATE(EXPNSDT1, 'ddMMMyyyy') as  drug_exposure_end_date
      ---fill_num can be 0 so check for 0 before adding dates, DATE_ADD(TO_DATE(RX_DOS_DT, 'ddMMMyyyy'), (CAST(DAYS_SUPPLY as int) * CAST( FILL_NUM as int) )) as RX_END_DT
  , CAST(null AS date) AS verbatim_end_date
  /* --- the source is claims / Prescription dispensed in pharmacy - is this always true? Comes from the OHDSI mapping file*/
  , 32869 as drug_type_concept_id  ---38000175 as drug_type_concept_id  --32810 = claims 32869 Pharmacy claim
  , CAST(null AS string) AS stop_reason
  , CAST( null as Integer)  as refills ---this is 2 character string need to converts to int 01 to 57
  , CAST(null AS float) as quantity
  , CAST(null as Integer) as days_supply
  , CAST(null AS string) AS sig
  , CAST(null as int) as route_concept_id
  , CAST(null AS string) AS lot_number
  , CAST(null AS long) AS provider_id
  , CAST(NULL as long) as  visit_occurrence_id
  , CAST(null AS long) AS visit_detail_id
  , CAST(null as string) as  drug_source_value
  , cxwalk.source_concept_id as drug_source_concept_id
  , CAST(null as long) as cms_provider_id 
  , CAST( NULL as string) as route_source_value
  , CAST( NULL as string) as dose_unit_source_value
  , CAST(NULL AS STRING) AS PHARM_SVC_TYPE
  , 'DM' as source_domain
  , dm.target_concept_id as visit_concept_id
  FROM `ri.foundry.main.dataset.e44ac418-9ac2-4714-b57a-b86d5a722077` dm
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON dm.HCPCS_CD = cxwalk.source_concept_code AND cxwalk.target_domain_id = 'Drug' 
    WHERE dm.HCPCS_CD is not NULL

),
pde_visit as (
  select distinct 
  cast ( d.person_id as long) as person_id
  ,CAST( d.drug_concept_id as int) drug_concept_id
  ,CAST( d.drug_exposure_start_date AS DATE) AS drug_exposure_start_date
  , CAST(drug_exposure_start_date AS timestamp) as drug_exposure_start_datetime
  , CAST( drug_exposure_end_date AS date) as drug_exposure_end_date
  , CAST(drug_exposure_end_date AS timestamp) as drug_exposure_end_datetime
  , cast( null as date ) as verbatim_end_date
  , drug_type_concept_id
  , stop_reason
  , refills ---this is 2 character string need to converts to int 01 to 57
  , cast(quantity as float) as quantity
  , days_supply
  , sig
  , route_concept_id
  , lot_number
  , d.care_site_id
  --,cast(null as int) as provider_id
  , v.visit_occurrence_id
  , v.visit_concept_id
  , visit_detail_id
  , drug_source_value
  , drug_source_concept_id
  , cast(cms_provider_id as long) as provider_id
  , route_source_value
  , dose_unit_source_value
  , PHARM_SVC_TYPE as place_of_service_source_value
  , d.source_domain
  FROM pde_drug d
   JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
   on d.person_id = v.person_id
   and d.care_site_id = v.care_site_id
   and v.visit_start_date = d.drug_exposure_start_date
   --and v.visit_end_date = d.drug_exposure_end_date
   and v.visit_concept_id = 581458
   and v.source_domain= d.source_domain
),

  dm2drug_visit AS (
  select distinct 
  cast ( dm.person_id as long) as person_id
  , CAST( dm.drug_concept_id as int) drug_concept_id
  , CAST( dm.drug_exposure_start_date AS DATE) AS drug_exposure_start_date
  , CAST( dm.drug_exposure_start_date AS timestamp) as drug_exposure_start_datetime
  , CAST( dm.drug_exposure_end_date AS date) as drug_exposure_end_date
  , CAST( dm.drug_exposure_end_date AS timestamp) as drug_exposure_end_datetime
  , cast( null as date ) as verbatim_end_date
  , drug_type_concept_id
  , stop_reason
  , refills 
  , cast(quantity as float) as quantity
  , days_supply
  , sig
  , route_concept_id
  , lot_number
  , dm.care_site_id
  --,cast(null as int) as provider_id
  , v.visit_occurrence_id
  , v.visit_concept_id
  , visit_detail_id
  , drug_source_value
  , drug_source_concept_id
  , cast(cms_provider_id as long) as provider_id
  , route_source_value
  , dose_unit_source_value
  , PHARM_SVC_TYPE as place_of_service_source_value
  , dm.source_domain
  FROM dm2drug dm
  JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
  on dm.person_id = v.person_id
    and dm.care_site_id = v.care_site_id
    and v.visit_start_date = dm.drug_exposure_start_date
    and v.visit_end_date = dm.drug_exposure_start_date
    and v.source_domain= dm.source_domain
    and v.visit_concept_id= dm.visit_concept_id


  ),
final_drug as (
  select * from pde_visit
  union 
  select * from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/drug/pb_hcpcs_drug` -- PB 2 Drug
  union 
 -- select * from ip_visit
  select * from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/drug/ip_hcpcs_drug`
  UNION 
  SELECT * FROM dm2drug_visit
), 

cms_drug as (
   SELECT DISTINCT 
    md5(concat_ws(
              ';'
        , COALESCE(person_id, '')
        , COALESCE(drug_concept_id, '')
        , COALESCE(drug_exposure_start_date, '')
        , COALESCE(drug_exposure_end_date, '')
        , COALESCE(drug_concept_id, '')
        , COALESCE(drug_source_concept_id, '')
        , COALESCE(drug_source_value, '' )
        , COALESCE(refills, '' )
        , COALESCE(quantity, '' )
        , COALESCE(days_supply, '')
        , COALESCE(care_site_id, '' )
        , COALESCE(provider_id, '')
        , COALESCE(visit_occurrence_id, '')
        , COALESCE( place_of_service_source_value, '')
        , COALESCE('Medicare', '')
        )) as cms_hashed_drug_exposure_id
    , d.*
    from final_drug d
    where drug_concept_id is not null
    )
    
    SELECT
      *
    , cast(conv(substr(cms_hashed_drug_exposure_id, 1, 15), 16, 10) as bigint) as drug_exposure_id
    FROM cms_drug