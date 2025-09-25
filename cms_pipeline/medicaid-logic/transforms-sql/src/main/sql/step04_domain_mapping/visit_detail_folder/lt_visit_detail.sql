CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_detail_folder/lt_visit_detail` AS

-- for admitting_source_value from ADMTG_PRVDR_SPCLTY_CD https://resdac.org/sites/datadocumentation.resdac.org/files/Admitting%20Provider%20Specialty%20Code.txt
-- analysis and notes in comments in this workbook: https://unite.nih.gov/workspace/vector/view/ri.vector.main.workbook.47e25b53-b1d9-4e9b-85d6-53119852cb78?branch=master 

WITH 
lt_visit_start as ( -- may have multiple instances of (person_id, provider_id, start, end), fix in next step
    SELECT 
        PSEUDO_ID as medicaid_person_id
        , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_id -- some of these have alpha characters and cast to null!       
        , SRVC_PRVDR_NPI as provider_id
        , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as visit_detail_start_date
        , to_date(SRVC_END_DT, 'ddMMMyyyy') as visit_detail_end_date
        , to_timestamp(SRVC_BGN_DT, 'ddMMMyyyy') as visit_detail_start_datetime
        , to_timestamp(SRVC_END_DT, 'ddMMMyyyy') as visit_detail_end_datetime
        , ADMTG_PRVDR_SPCLTY_CD as admitting_source_value
        , BILL_TYPE_CD_digit2 as BILL_TYPE_CD_digit2
        , 32021 as visit_detail_type_concept_id
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/lt_prepared`
    WHERE MDCD_PD_AMT >= 0 /* Acumen suggest to check for payment > 0 as a valid claim */
      AND CLM_TYPE_CD NOT IN ( 'Z') -- must be a valid claim type
),
lt_visit_singles as ( -- narrow it down to a single set of values per desired PK by choosing the first value of ones that repeat
    SELECT 
        medicaid_person_id
        , first(care_site_id)
          OVER(
            PARTITION BY medicaid_person_id, provider_id, visit_detail_start_date, visit_detail_end_date
            ORDER BY care_site_id DESC
          ) as care_site_id
        , provider_id
        , visit_detail_start_date
        , visit_detail_end_date
        , visit_detail_start_datetime
        , visit_detail_end_datetime
        , first(admitting_source_value)
          OVER( 
            PARTITION BY medicaid_person_id, provider_id, visit_detail_start_date, visit_detail_end_date
            ORDER BY admitting_source_value DESC
          ) as admitting_source_value
        , first(BILL_TYPE_CD_digit2) 
          OVER(
            PARTITION BY medicaid_person_id, provider_id, visit_detail_start_date, visit_detail_end_date
            ORDER BY BILL_TYPE_CD_digit2 DESC
          ) as BILL_TYPE_CD_digit2
        , visit_detail_type_concept_id
    FROM lt_visit_start
),
lt_visit_prelim as (
    SELECT -- DISTINCT
        medicaid_person_id, care_site_id, provider_id
        , visit_detail_start_date, visit_detail_end_date, visit_detail_start_datetime, visit_detail_end_datetime
        , COALESCE(NULLIF(admitting_source_value, ''), 'A0') as admitting_source_value -- change this in parallel with the else below for visit_detail_concept_id
        -- Plese Be warned, this only deals with situations where multiple values should come together and one is null or empty-string.
        -- Others, where you have two non-null and non-empty values that both map to the same visit_detail_concept_id will produce
        -- multiple rows for the same PK. I don't see any such today.
        , case when BILL_TYPE_CD_digit2 = '1' then 9201 --	Inpatient Visit
               when BILL_TYPE_CD_digit2 = '3' then 9202 -- 3 is homehealth so outpatient
               else 42898160 --- all other is longterm care claims  
          end as visit_detail_concept_id
        , 32021 as visit_detail_type_concept_id
    FROM lt_visit_singles
),
STEP_1 as (
SELECT
        md5(concat_ws(
                  ';'
            , COALESCE(medicaid_person_id, 0)
            , COALESCE(provider_id, '0')
            , COALESCE(care_site_id, '0')
            , COALESCE(visit_detail_start_date, '')
            , COALESCE(visit_detail_end_date, '')
            , COALESCE(visit_detail_concept_id, '')
            , COALESCE(first(admitting_source_value), '')
            , 'LT'
            , 'Medicaid'
     )) AS cms_visit_detail_id,
       medicaid_person_id as person_id,
       care_site_id, provider_id,
       visit_detail_start_date, visit_detail_start_datetime,
       visit_detail_end_date, visit_detail_end_datetime,
       admitting_source_value,
       visit_detail_concept_id, visit_detail_type_concept_id
FROM lt_visit_prelim       
GROUP BY  medicaid_person_id, care_site_id, provider_id,
       visit_detail_start_date, visit_detail_start_datetime,
       visit_detail_end_date, visit_detail_end_datetime,
       admitting_source_value,
       visit_detail_concept_id, visit_detail_type_concept_id
)
SELECT distinct
    *, cast(base_10_hash_value as bigint) as visit_detail_id
FROM (
    SELECT *, conv(sub_hash_value, 16, 10) as base_10_hash_value
    FROM (
        SELECT *, substr(hashed_id, 1, 15) as sub_hash_value
        FROM (
            SELECT *, cms_visit_detail_id as hashed_id
            FROM STEP_1
        )
    )
)