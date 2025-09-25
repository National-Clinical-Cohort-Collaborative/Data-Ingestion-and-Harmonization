CREATE TABLE `ri.foundry.main.dataset.9acf56dc-536a-4dfc-b670-cf147067a0d5` AS
  with pb_hcpcs as ( 
    SELECT DISTINCT 
    CLAIM_ID
    , BID
    ,HCPCS_CD
    ,EXPNSDT1 
    ,EXPNSDT2 
    ,BLGNPI
    ,TAX_NUM
    ,TAX_NUM_ID
    ,PLCSRVC 
    ,'PB' as source_domain
    FROM  `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb 
  ), 
  pb_drug as (--300 rows from pb for Drug domain
    SELECT DISTINCT 
        CAST(BID AS long) as person_id
        ,TO_DATE ( EXPNSDT1, 'ddMMMyyyy') as drug_exposure_start_date
        ,TO_DATE ( EXPNSDT2, 'ddMMMyyyy') as drug_exposure_end_date
        ,cxw.target_concept_id as drug_concept_id
        ,32810 as drug_type_concept_id --32810 claim type
        ,BLGNPI as provider 
        ,TAX_NUM_ID as cms_provider_id
        ,HCPCS_CD as drug_source_value 
        ,cxw.source_concept_id as drug_source_concept_id
        ,vxwalk.target_concept_id as visit_concept_id ----place_of_service_concept_id roll-up based on  OMOP CMS place of visit.
        --,pb.PLCSRVC ||'-'|| vxwalk.source_concept_name AS place_of_service_source_value
        , pb.PLCSRVC as place_of_service_source_value
        ,cxw.target_domain_id as target_domain_id
        , 'PB' as source_domain
  FROM pb_hcpcs pb
  LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxw ON pb.HCPCS_CD = cxw.cms_src_code AND cxw.target_domain_id = 'Drug' 
  LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
    ON pb.PLCSRVC = vxwalk.source_concept_code AND pb.PLCSRVC is not NULL
  WHERE BID is not null and CLAIM_ID is not null and cxw.target_concept_id is not null 
  ),
  pb_drug_with_visit as (
 select distinct -- order of these columns are important for the drug_exposure
  cast( d.person_id as long) as person_id
  , drug_concept_id
  , drug_exposure_start_date
  ,  CAST(drug_exposure_start_date AS timestamp)drug_exposure_start_datetime
  , drug_exposure_end_date
  , CAST(drug_exposure_end_date AS timestamp) as drug_exposure_end_datetime
  , cast( null as date ) as verbatim_end_date
  , drug_type_concept_id
  , CAST(null AS string) AS stop_reason
  , CAST( null as Integer)  as refills ---this is 2 character string need to converts to int 01 to 57
  , CAST(null AS float) as quantity
  , CAST(null as Integer) as days_supply
  , CAST(null AS string) AS sig
  , CAST(null as int) as route_concept_id
  , CAST(null AS string) AS lot_number
  , CAST(provider AS long) AS provider_id
  , visit_occurrence_id
  , v.visit_concept_id
  , CAST(null AS long) AS visit_detail_id
  , d.drug_source_value
  , d.drug_source_concept_id
  , d.cms_provider_id  
  , cast(null as string) route_source_value
  , cast(null as string ) dose_unit_source_value
  , place_of_service_source_value -- matched on place of service value, two digit code to find the correct visit id, with visit
  , d.source_domain
  FROM pb_drug d
  left JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
   on d.person_id = v.person_id
   and d.provider = v.provider_id
   and v.visit_start_date = d.drug_exposure_start_date
   and v.visit_end_date = d.drug_exposure_end_date
   and v.visit_concept_id = d.visit_concept_id-- it rollup based on the CMS place of visit. 
   and v.visit_source_value = d.place_of_service_source_value --same person same start and end data , there may be number of different place of visit in claims. therefore match on the plcsrvc raw value
   and v.source_domain= d.source_domain
 )

  select * from pb_drug_with_visit