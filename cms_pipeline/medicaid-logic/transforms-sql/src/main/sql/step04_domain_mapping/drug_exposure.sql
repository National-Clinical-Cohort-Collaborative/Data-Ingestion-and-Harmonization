CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/drug_exposure` AS
--rx 
with rx_drug as (
SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
      , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), PRSCRBNG_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , PRSCRBNG_PRVDR_NPI as provider_npi 
      , cast( 581458 as int) as visit_concept_id
      , to_date(MDCD_PD_DT, 'ddMMMyyyy') as drug_exposure_start_date
      -- note from meeting with Tom, only add the days_supply to the start date. Cannot rely on the refill number
      ---, date_add(to_date (MDCD_PD_DT, 'ddMMMyyyy'  ), cast(DAYS_SUPPLY AS INT) )as drug_exposure_end_date
      , to_date (MDCD_PD_DT, 'ddMMMyyyy' ) as drug_exposure_end_date ---DAYS_SUPPLY
  /* --- the source is claims / Prescription dispensed in pharmacy - is this always true? Comes from the OHDSI mapping file*/
  , target_concept_id as drug_concept_id
  , 32810 as drug_type_concept_id  ---38000175 as drug_type_concept_id  --32810 claims 
  , CAST(null AS string) AS stop_reason
  , CAST( NEW_RX_REFILL_NUM as Integer)  as refills ---this is 2 character string need to converts to int 01 to 57
  , CAST(NDC_QTY AS float) as quantity
  , CAST(DAYS_SUPPLY as Integer) as days_supply
  , CAST(null AS string) AS sig
  , CAST(null as int) as route_concept_id
  , CAST(null AS string) AS lot_number
  , CAST(PRSCRBNG_PRVDR_NPI AS long) AS provider_id
  , CAST(NULL as long) as  visit_occurrence_id
  , CAST(null AS long) AS visit_detail_id
  , CAST(NDC as string) as  drug_source_value
  , xw.source_concept_id as drug_source_concept_id
  , CAST(BLG_PRVDR_NPI as long) as care_site_id
  , CAST( NULL as string) as route_source_value
  , CAST( NULL as string) as dose_unit_source_value
  --todo when ready
  --/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/rx_ndc_long_prepared
  FROM `ri.foundry.main.dataset.292106b5-cd0c-4a32-bc2d-6f9369ffac0b` rx
  JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON rx.NDC = xw.source_concept_code AND xw.mapped_code_system = 'NDC' AND xw.target_domain_id = 'Drug' 
    WHERE rx.NDC is not NULL and MDCD_PD_AMT >=0 and xw.target_concept_id is not null
    --todo : do you add only when MDCD_PD_AMT > 0 
),
--add drug from OT, hcpcs and dx 
ot_hcpcs as (
  SELECT DISTINCT
  PSEUDO_ID AS medicaid_person_id,
  to_date(SRVC_BGN_DT, 'ddMMMyyyy') as start_date,
  to_date(SRVC_END_DT, 'ddMMMyyyy') as end_date, 
  COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as medicaid_care_site_id, -- if missing use srvc prvdr npi
  SRVC_PRVDR_NPI as medicaid_provider_id,
  LINE_PRCDR_CD as hcpcs_src_code,
  src_column, 
  xw.target_domain_id,
  xw.source_concept_id,
  xw.target_concept_id, 
  xw.target_concept_name, 
  ---REV_CNTR_CD
  case when REV_CNTR_CD in ("0450", "0451", "0452", "0456", "0459", "0981" ) then 9203 ----Emergency Room Visit	9203
        else 9202 --Outpatient Visit	9202
        end as visit_concept_id
  FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/ot_hcpc_long_prepared` ot
  JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ot.LINE_PRCDR_CD = xw.source_concept_code AND xw.mapped_code_system in ( 'HCPCS', 'CPT4') AND xw.target_domain_id = 'Drug' 
    WHERE LINE_PRCDR_CD is not NULL and MDCD_PD_AMT >=0
), 
ot_drug as (
SELECT DISTINCT
      medicaid_person_id
      , medicaid_care_site_id as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , medicaid_provider_id as provider_npi 
      , visit_concept_id
      , start_date as drug_exposure_start_date
      , end_date  as drug_exposure_end_date
      , target_concept_id as drug_concept_id
      , CAST(null AS date) AS verbatim_end_date
  /* --- the source is claims / Prescription dispensed in pharmacy - is this always true? Comes from the OHDSI mapping file*/
  , 32810 as drug_type_concept_id  ---38000175 as drug_type_concept_id  --32810 claims 
  , CAST(null AS string) AS stop_reason
  , CAST(null as Integer)  as refills ---this is 2 character string need to converts to int 01 to 57
  , CAST(null AS float) as quantity
  , CAST(null as Integer) as days_supply
  , CAST(null AS string) AS sig
  , CAST(null as int) as route_concept_id
  , CAST(null AS string) AS lot_number
  , CAST(medicaid_provider_id AS long) AS provider_id
  , CAST(NULL as long) as  visit_occurrence_id
  , CAST(null AS long) AS visit_detail_id
  , CAST(hcpcs_src_code as string) as  drug_source_value
  , CAST(source_concept_id as int) as drug_source_concept_id
  , CAST(medicaid_care_site_id as long) as care_site_id
  , CAST( NULL as string) as route_source_value
  , CAST( NULL as string) as dose_unit_source_value
  , 'OT' as source_domain
  FROM ot_hcpcs
),
ot_drug_visit as (
  select distinct 
    medicaid_person_id as person_id
  , CAST( d.drug_concept_id as int) drug_concept_id
  , CAST( d.drug_exposure_start_date AS DATE) AS drug_exposure_start_date
  , CAST(drug_exposure_start_date AS timestamp) as drug_exposure_start_datetime
  , CAST( drug_exposure_end_date AS date) as drug_exposure_end_date
  , CAST(drug_exposure_end_date AS timestamp) as drug_exposure_end_datetime
  , CAST( NULL AS string ) as admit_hr
  , cast( null as date ) as verbatim_end_date
  , drug_type_concept_id
  , stop_reason
  , refills ---this is 2 character string need to converts to int 01 to 57
  , quantity
  , days_supply
  , sig
  , route_concept_id
  , lot_number
  , d.care_site_npi as care_site_id --provider_npi
  , d.provider_npi as  provider_id
  , v.visit_occurrence_id
  , v.visit_concept_id
  , cast( null as int) as visit_detail_id
  , drug_source_value
  , drug_source_concept_id
  , route_source_value
  , dose_unit_source_value
  , d.source_domain
  FROM ot_drug d
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
   on v.person_id = d.medicaid_person_id 
   --- and v.provider_id = d.provider_npi 
   and v.care_site_id = d.care_site_npi
   and v.visit_start_date = d.drug_exposure_start_date
   and v.visit_end_date = d.drug_exposure_end_date
   ---and v.visit_concept_id = d.visit_concept_id
   and v.source_domain = d.source_domain
),
rx_drug_visit as (
  select distinct 
    medicaid_person_id as person_id
  , drug_concept_id
  , CAST( d.drug_exposure_start_date AS DATE) AS drug_exposure_start_date
  , CAST(drug_exposure_start_date AS timestamp) as drug_exposure_start_datetime
  ---, CAST(drug_exposure_end_date AS timestamp) as drug_exposure_end_datetime
  --- add days_supply to the start date, end_date is same as start_date in RX
  ---, CAST( drug_exposure_end_date AS date) as drug_exposure_end_date
  , CAST( date_add(drug_exposure_end_date, cast(days_supply AS INT)) AS date) as drug_exposure_end_date 
  , CAST( date_add(drug_exposure_end_date, cast(days_supply AS INT)) as timestamp) as drug_exposure_end_datetime 
  , CAST( NULL AS string ) as admit_hr
  , CAST( null as date ) as verbatim_end_date
  , drug_type_concept_id
  , stop_reason
  , refills ---this is 2 character string need to converts to int 01 to 57
  , cast(quantity as float) as quantity
  , days_supply
  , sig
  , route_concept_id
  , lot_number
  , care_site_npi as care_site_id
  , d.provider_id
  , v.visit_occurrence_id
  , v.visit_concept_id
  , visit_detail_id
  , drug_source_value
  , drug_source_concept_id
  , route_source_value
  , dose_unit_source_value
  , 'RX' as source_domain
  FROM rx_drug d
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
   on d.medicaid_person_id = v.person_id
   ---and d.provider_id = v.provider_id
   and v.care_site_id = d.care_site_npi
   and v.visit_start_date = d.drug_exposure_start_date -- these dates should match the dates in the visit
   and v.visit_end_date = d.drug_exposure_end_date
   ----and v.visit_concept_id = d.visit_concept_id
   and v.source_domain = 'RX'
),
ip_ndc as ( 
  SELECT DISTINCT
  PSEUDO_ID AS medicaid_person_id
  , to_date(ADMSN_DT, 'ddMMMyyyy') as start_date
  , to_date(DSCHRG_DT, 'ddMMMyyyy') as end_date
  , admission_datetime
  , ADMSN_HR
  , ADMSN_TYPE_CD as admitting_source_value
  , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_id -- if missing use srvc prvdr npi
  , SRVC_PRVDR_NPI as provider_id
  , REV_CNTR_CD
  , NDC as ndc
  , NDC_QTY as qty
  , NDC_UOM_CD
  , xw.target_domain_id
  , xw.source_concept_id
  , xw.target_concept_id
  , xw.target_concept_name
  ---REV_CNTR_CD
  FROM `ri.foundry.main.dataset.423f6d98-c5ca-4bce-a649-736cd11da712` ip
  JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ip.NDC = xw.source_concept_code AND xw.mapped_code_system = 'NDC' AND xw.target_domain_id = 'Drug' 
    WHERE ip.NDC is not NULL and MDCD_PD_AMT >=0
),
ip_drug_visit as (
  select distinct 
       medicaid_person_id as person_id
      , target_concept_id as drug_concept_id 
      , start_date as drug_exposure_start_date
      , admission_datetime as drug_exposure_start_datetime
      , end_date  as drug_exposure_end_date
      , cast( end_date as timestamp) as drug_exposure_end_datetime
      , ADMSN_HR as admit_hr
      , CAST(null AS date) AS verbatim_end_date
      , 32810 as drug_type_concept_id  ---38000175 as drug_type_concept_id  --32810 claims --581373--Physician administered drug (identified from EHR order)
      , CAST(null AS string) AS stop_reason
      , CAST(null as Integer)  as refills ---this is 2 character string need to converts to int 01 to 57
      , CAST(qty AS float) as quantity
      , CAST(1 as Integer) as days_supply
      , CAST(null AS string) AS sig
      , CAST(null as int) as route_concept_id
      , CAST(null AS string) AS lot_number
      , ipn.care_site_id 
      , ipn.provider_id
      , v.visit_occurrence_id--no need to match on this value, visit_concept_id
      , v.visit_concept_id
      , CAST(null AS long) AS visit_detail_id
      , CAST(ndc as string) as  drug_source_value
      , CAST(source_concept_id as int) as drug_source_concept_id
      , CAST( NULL as string) as route_source_value
      , CAST( NULL as string) as dose_unit_source_value -- two different dose_unit_source values are supplied in the NDC_UOM_CD
      , 'IP' as source_domain
    FROM ip_ndc ipn
    JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    on v.person_id = ipn.medicaid_person_id 
    and v.care_site_id = ipn.care_site_id
    and v.visit_start_date = ipn.start_date
    and v.visit_end_date = ipn.end_date
    and v.source_domain ='IP'
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
    ON lt.src_code = xw.cms_src_code and xw.target_domain_id = 'Drug' 
    where lt.src_code is not null and xw.mapped_code_system in ('CVX', 'HCPCS')
    ),
    lt_rx_visit as (
    SELECT DISTINCT
      lt.medicaid_person_id
    , lt.target_concept_id as drug_concept_id
    , vo.visit_start_date as drug_exposure_start_date
    , CAST(vo.visit_start_date as TIMESTAMP) as drug_exposure_start_datetime
    , vo.visit_end_date as drug_exposure_end_date
    , CAST(vo.visit_end_date as TIMESTAMP) as drug_exposure_end_datetime
    , '00' as admit_hr
      , CAST(null AS date) AS verbatim_end_date
      , 32810 as drug_type_concept_id  ---38000175 as drug_type_concept_id  --32810 claims --581373--Physician administered drug (identified from EHR order)
      , CAST(null AS string) AS stop_reason
      , CAST(null as Integer)  as refills ---this is 2 character string need to converts to int 01 to 57
      , CAST(null AS float) as quantity
      , CAST(1 as Integer) as days_supply
      , CAST(null AS string) AS sig
      , CAST(null as int) as route_concept_id
      , CAST(null AS string) AS lot_number
      , lt.care_site_npi as care_site_id
      , lt.provider_npi as provider_id
      , vd.visit_occurrence_id 
      , vd.visit_detail_concept_id as visit_concept_id
      , CAST(null AS long) AS visit_detail_id
      , source_value as  drug_source_value
      , CAST(source_concept_id as int) as drug_source_concept_id
      , CAST( NULL as string) as route_source_value
      , CAST( NULL as string) as dose_unit_source_value -- two different dose_unit_source values are supplied in the NDC_UOM_CD
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


final_drug as (
  select distinct * from rx_drug_visit
  UNION
  select distinct * from ot_drug_visit
  UNION
  SELECT distinct * from ip_drug_visit
  UNION
  SELECT distinct * from lt_rx_visit
),

medicaid_drug as (
   SELECT DISTINCT 
    md5(concat_ws(
              ';'
        , COALESCE(person_id, '')
        , COALESCE(drug_exposure_start_date, '')
        , COALESCE(drug_exposure_end_date, '')
        , COALESCE( admit_hr, '')
        , COALESCE(drug_concept_id, '')
        , COALESCE(drug_source_concept_id, '')
        , COALESCE(drug_source_value, '' )
        , COALESCE(refills, '' )
        , COALESCE(quantity, '' )
        , COALESCE(days_supply, '')
        , COALESCE( dose_unit_source_value, '')
        , COALESCE(provider_id, '' )
        , COALESCE( care_site_id, '')
        , COALESCE( visit_occurrence_id, '')
        , COALESCE( source_domain, '')
        , COALESCE('medicaid', '')
        )) as medicaid_hashed_drug_exposure_id
    , d.*
    from final_drug d
    where drug_concept_id is not null
    )
    
    SELECT
      d.*
    , cast(conv(substr(medicaid_hashed_drug_exposure_id, 1, 15), 16, 10) as bigint) as drug_exposure_id
    , m.macro_visit_long_id
    FROM medicaid_drug d
    left join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit_with_micro_visit` m on m.visit_occurrence_id = d.visit_occurrence_id