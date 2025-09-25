CREATE TABLE `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` AS
--ip, start and end date use care_site and billing provider as the visit identifier
--note, OPRTG_PRVDR_NPI	The National Provider ID (NPI) of the provider who performed the surgical procedure(s).	Operating Provider NPI
--- 11.6 mil 4/12/2023
    WITH ip as (
      -- patient / start and end date/ care site/ provider
      SELECT DISTINCT
        CLAIMNO
        , PSEUDO_ID as medicaid_person_id
        , to_date(ADMSN_DT, 'ddMMMyyyy') as start_date
        , to_date(DSCHRG_DT, 'ddMMMyyyy') as end_date
        , admission_datetime
        , ADMSN_TYPE_CD as admitting_source_value--- CMS Place of Service ( 1EMERGENCY=>23Emergency Room - Hospital/ 2URGENT=>20Urgent Care Facility, 3ELECTIVE = 22Outpatient Hospital,4NEWBORN=25BIRTHING ,5, 9 UNKNOWN)
        --, BLG_PRVDR_SPCLTY_CD as place_of_service -- if A0 then it is hospital
        , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
        , SRVC_PRVDR_NPI as provider_npi  --health care professional who delivers or completes a particular medical service or non-surgical procedure.
        , REV_CNTR_CD
        --, visit_concept_id ------ BASED ON REV_CNTR_CD in step 03_5 determin ip or erip
        -- ,  case when ipp.REV_CNTR_CD IN ("0450", "0451", "0452", "0456", "0459", "0981" ) then  262 -- iper ERIP Emergency Room and Inpatient Visit
        --        else 9201 --- inpatient visit
        --       end as visit_concept_id 
        , 32021 as visit_type_concept_id 
        --need to rank afterwards
        --, RANK() OVER (PARTITION BY CLAIMNO, PSEUDO_ID, ADMSN_DT, BLG_PRVDR_NPI, SRVC_PRVDR_NPI, admission_datetime, ADMSN_TYPE_CD ORDER BY DSCHRG_DT DESC) as idx
      FROM `ri.foundry.main.dataset.4994c288-97f1-4b26-8867-14311851ef30` ipp
      WHERE MDCD_PD_AMT >= 0 /* Acumen suggest to check for payment > 0 */
      AND CLM_TYPE_CD NOT IN ( 'Z')
    ),
    ---ip rank after the string date fields are converted to date -----------
    ip_visit_detail as ( 
    SELECT DISTINCT
      medicaid_person_id -- person
      , care_site_npi -- care site
      , provider_npi -- provider 
      , start_date as visit_start_date
      , end_date as visit_end_date
      , ip.admission_datetime
      , ip.admitting_source_value
      , visit_type_concept_id 
      ,case when size(array_intersect( collect_set(REV_CNTR_CD), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0 then 262
            else 9201
            end as visit_concept_id
      ---,collect_set(REV_CNTR_CD) as rev_center_codes
      FROM ip
      GROUP BY 1, 2,3,4,5,6,7,8
   ), 
   ip_rank as (
        SELECT DISTINCT
        medicaid_person_id -- person
      , care_site_npi -- care site
      , provider_npi -- provider 
      , visit_start_date
      , visit_end_date
      , admission_datetime
      , admitting_source_value
      , visit_concept_id
      , visit_type_concept_id 
      , RANK() OVER (PARTITION BY medicaid_person_id, care_site_npi, provider_npi, visit_start_date ORDER BY visit_end_date DESC ) as idx 
      FROM ip_visit_detail
   ),
   --- ip claim visits ------------
   ip_visit as ( --- in case there are multiple
    SELECT DISTINCT
      'IP' as source_domain
      , medicaid_person_id
      , care_site_npi
      , provider_npi
      , visit_start_date
      , visit_end_date
      , ip.admission_datetime
      , ip.admitting_source_value
      , visit_concept_id
      , visit_type_concept_id 
    FROM ip_rank ip
    WHERE ip.idx = 1 
    ), 
    ------------------------------------------
    --- long term care/ outpatient / inpatient / 2nd Digit-Type of Facility=1 is Hospital --> inpatient, =2	is Skilled Nursing -->42898160, =3	Home Health --> outpatient
    --- lt_visit.BILL_TYPE_CD_digit2 determines the visit concept id. 
    ----if 1 = inpatient /hospital
    ----if 3 = then homehealth maps to outpatient / 9202 as visit_concept id
    ----if not 1 or 3 then long term care / 42898160 as visit concept id
    ---discharge dates are not well filled
  lt_visit as (
    SELECT distinct 
      'LT' as source_domain,
      vd.person_id,
      vd.care_site_id,
      vd.provider_id,   
      vd.span_start_date                    as visit_start_date, 
      vd.span_end_date                      as visit_end_date,  
      vd.span_start_date                    as admission_datetime,  -- CONVERT!!!!
      vd.admitting_source_value,
      vd.visit_detail_concept_id            as visit_concept_id, 
      vd.visit_detail_type_concept_id       as visit_type_concept_id  
  FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/lt_visit_detail_with_spans` vd
 ),
  ----------------------------------------
  -- outpatient ot or er 
  ot as (
   SELECT DISTINCT 
        CLAIMNO 
        ,PSEUDO_ID as medicaid_person_id
        , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as start_date
        , to_date(SRVC_END_DT, 'ddMMMyyyy') as end_date
        ---, BLG_PRVDR_NPI as care_site_npi --1236 null, if null replace with the service provider then the number of null goes down to for one person --840 for person M1003733251
        , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_npi  --840 only 840 null 
        , SRVC_PRVDR_NPI as provider_npi
        ---need to know if OT or OTER check revenue center code and determine the visit_concept_id
        ---, case when REV_CNTR_CD IN ("0450", "0451", "0452", "0456", "0459", "0981" ) then 9203
        ---else 9202 
        ---end as visit_concept_id
        , REV_CNTR_CD
        , POS_CD -- admitting source 
        , 32021 as visit_type_concept_id 
        ---, RANK() OVER (PARTITION BY PSEUDO_ID, SRVC_BGN_DT, BLG_PRVDR_NPI, SRVC_PRVDR_NPI ORDER BY SRVC_END_DT DESC) as idx
    FROM  `ri.foundry.main.dataset.7996f1b7-6f17-421a-b6a1-39aded1c3e4a` ot
    where CLM_TYPE_CD NOT IN ( 'Z') -- need to be a valid claim/ claim that is not denied
    ),
    ot_visit_detail(
       SELECT DISTINCT
       medicaid_person_id -- person
      , care_site_npi -- care site
      , provider_npi -- provider 
      , start_date as visit_start_date
      , end_date as visit_end_date
      , cast( start_date as timestamp) as admission_datetime
      , POS_CD AS admitting_source_value
      , visit_type_concept_id 
      ,case when size(array_intersect( collect_set(REV_CNTR_CD), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0 then 9203 -- 9203 ER visit
            else 9202 -- outpatient
            end as visit_concept_id
      ---,collect_set(REV_CNTR_CD) as rev_center_codes
      FROM ot
      GROUP BY 1, 2,3,4,5,6,7,8
    ), 
     ot_rank as (
        SELECT DISTINCT
        medicaid_person_id -- person
      , care_site_npi -- care site
      , provider_npi -- provider 
      , visit_start_date
      , visit_end_date
      , admission_datetime
      , admitting_source_value
      , visit_concept_id
      , visit_type_concept_id 
      , RANK() OVER (PARTITION BY medicaid_person_id, care_site_npi, provider_npi, visit_start_date ORDER BY visit_end_date DESC ) as idx 
      FROM ot_visit_detail
   ),
    ot_visit as (
    SELECT DISTINCT
      'OT' as source_domain
      , medicaid_person_id
      , care_site_npi as care_site_id
      , provider_npi
      , visit_start_date
      , visit_end_date
      ,  admission_datetime
      , admitting_source_value
      , visit_concept_id --Outpatient Visit	9202 /or  er visit 9203
      , visit_type_concept_id 
    FROM ot_rank
    where idx = 1
    ),
    rx as (
    --BLG_PRVDR_NPI/DSPNSNG_PRVDR_NPI/PRSCRBNG_PRVDR_NPI prs is most populated / NEW_RX_REFILL_NUM - two digit num/DAYS_SUPPLY is 1 or 3 digits
    SELECT DISTINCT 
      CLAIMNO
    , PSEUDO_ID as medicaid_person_id
    , to_date (MDCD_PD_DT, 'ddMMMyyyy'  ) as  MDCD_PD_DT
    ---, date_add(to_date (MDCD_PD_DT, 'ddMMMyyyy'  ), cast(DAYS_SUPPLY AS INT) )as rx_end_date
    , to_date (MDCD_PD_DT, 'ddMMMyyyy' ) as rx_end_date
    -- todo: check refillnum end date would need to be computed based on the refill and the days of supply
    , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), PRSCRBNG_PRVDR_NPI) as care_site_npi
    , PRSCRBNG_PRVDR_NPI as provider_npi --- the provider who issued the prescription
    , to_timestamp(MDCD_PD_DT, 'ddMMMyyyy' ) as admission_datetime
    , cast ('A5' as string) as admitting_source_value
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/rx_prepared` rx
    WHERE MDCD_PD_AMT >= 0   AND CLM_TYPE_CD NOT IN ( 'Z')
    ),
    rx_visit as ( 
    --rx
    SELECT DISTINCT 
    'RX' as source_domain
      , medicaid_person_id
      , care_site_npi
      , provider_npi
      , MDCD_PD_DT as visit_start_date
      , rx_end_date as visit_end_date
      , admission_datetime
      , admitting_source_value
      , 581458 as visit_concept_id --pharmacy visit
      , 32021 as visit_type_concept_id --Visit derived from encounter on claim
    FROM rx
     ),
    ---------------------------------------------------------------------------------------------------------------------------
    -- combine all visits from all medicaid source files, ip, lt, ot, rx
    -- all_visits
    all_visits as (
      select * from ip_visit 
      union
      select * from lt_visit
      union 
      select * from ot_visit 
      union
      select * from rx_visit
    ),

--------build visit ids from all_visits sub-query  
    visit_id as (
  SELECT DISTINCT
  md5(concat_ws(
              ';'
        , COALESCE(medicaid_person_id, '')
        , COALESCE(care_site_npi, '')
        , COALESCE(provider_npi, '')
        , COALESCE(visit_start_date, '')
        , COALESCE(visit_end_date, '')
        , COALESCE(visit_concept_id, '')
        , COALESCE(admitting_source_value, '')
        , COALESCE(plcxw.source_concept_id, '')
        , COALESCE(first(plcxw.target_concept_id), '')
        , COALESCE(source_domain, '')
        , COALESCE('medicaid', '')
     )) AS cms_medicaid_visit_occurrence_id
    , medicaid_person_id AS person_id
    , CAST(visit_concept_id AS int) as visit_concept_id
    , CAST( visit_start_date AS DATE) AS visit_start_date
    , CAST(visit_start_date as TIMESTAMP) as visit_start_datetime
    , CAST( visit_end_date AS DATE) AS visit_end_date
    , CAST(visit_end_date as TIMESTAMP) as visit_end_datetime
    , CAST( visit_type_concept_id AS int) as visit_type_concept_id
    --- USE THE care provider who provided the service for the patient in this claim should be referenced in the provider_id - like SRVC_PRVDR_NPI
    ----in medicaid both care site and the Servicing Provider NPI used as provider id
    , CAST( provider_npi as long ) AS provider_id
    ---location id of the care_site entity/ facility should be used in the care_site_id field - Note, from location : CAST(trim(ip.BLG_PRVDR_NPI) as long) as location_id
    , CAST( care_site_npi as long ) AS care_site_id
    ,  CAST(cxw.concept_name AS string) as visit_source_value
    , CAST(null AS int) AS visit_source_concept_id
    , CAST(plcxw.source_concept_id AS int) AS admitting_source_concept_id
    , CAST(admitting_source_value AS string) AS admitting_source_value
    , CAST(null AS int) AS discharge_to_concept_id
    , CAST(null AS string) AS discharge_to_source_value
    , CAST(null AS long) AS preceding_visit_occurrence_id
    , plcxw.source_concept_name as admitting_source_value_concept_name -- not in CDM 5.3
    -- see group-by below. OMOP has two mappings for A0 (OP and IP), for now, we just pick the first, which is working out to IP. 
    , first(plcxw.target_concept_id)  as admitting_source_target_concept_id -- not in CDM


    --, reimbursement_facility_type
    --, facilityxw.src_code_name as reimbursement_facility_type_name
    --, facilityxw.target_concept_id as reimbursement_facility_type_target_concept_id     
    , source_domain
  FROM all_visits v
  LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location` l on l.location_id = cast( v.care_site_npi as long)
  LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/cms_medicaid_place_of_visit_xwalk` plcxw on plcxw.source_concept_code = admitting_source_value  
  LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` cxw on cxw.concept_id = v.visit_concept_id and domain_id='Visit'
  --LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/medicaid_valueset_xwalk` facilityxw on column_name = 'MDCR_REIMBRSMT_TYPE_CD' and facilityxw.src_code = reimbursement_facility_type ---MDCR_REIMBRSMT_TYPE_CD
  where visit_end_date is not null and visit_start_date is not null
  GROUP BY medicaid_person_id, provider_npi, care_site_npi, visit_start_date, visit_end_date, 
           visit_type_concept_id, concept_name,  visit_concept_id, 
           admitting_source_value, source_concept_id, source_domain, source_concept_name
           -- trippy, these are the "as" names, above are the in-coming names
  ORDER BY person_id, provider_id, care_site_id, visit_start_date, visit_end_date, 
           visit_type_concept_id, visit_source_value,  visit_concept_id, 
           admitting_source_value, admitting_source_concept_id, source_domain, admitting_source_value_concept_name,  
           admitting_source_target_concept_id
  )
  SELECT distinct
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    --cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as domain_id_51_bit
    , cast(base_10_hash_value as bigint) as visit_occurrence_id
    FROM (
        SELECT
          *
        , conv(sub_hash_value, 16, 10) as base_10_hash_value
        FROM (
            SELECT
              *
            , substr(hashed_id, 1, 15) as sub_hash_value
            FROM (
                SELECT
                  *
                -- Create primary key by hashing local visit id id to 128bit hexademical with md5,
                -- and converting to 51 bit int by first taking first 15 hexademical digits and converting
                --  to base 10 (60 bit) and then bit masking to extract the first 51 bits
                , cms_medicaid_visit_occurrence_id as hashed_id
                FROM visit_id
            )
        )
    )