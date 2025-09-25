CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_detail_prelim` AS

WITH 
sn_prelim_hcps as (
      SELECT DISTINCT
          BID
        , to_date(FROM_DT, 'ddMMMyyyy') as FROM_DT
        , coalesce(PROVIDER, '0') as PROVIDER  
        , to_date(THRU_DT, 'ddMMMyyyy') as THRU_DT 
        , case when size(array_intersect( collect_set(RVCNTR), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0  
               then 9203 --er
               else 42898160 -- nhiv
            END as visit_detail_concept_id
         -- https://resdac.org/sites/datadocumentation.resdac.org/files/Revenue%20Center%20Code%20Code%20Table%20FFS.txt
      FROM `ri.foundry.main.dataset.79489370-9e8d-4743-b0b6-da040f1f2f02` sn_h_long
      LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
      ON sn_h_long.HCPSCD= cxwalk.cms_src_code  and cxwalk.target_domain_id in ('Observation', 'Procedure', 'Device')
      WHERE PMT_AMT >= 0  /*  PMT_AMT must > 0 to complete the stay */
      group by BID, FROM_DT, PROVIDER, THRU_DT
),
sn_hcps as ( 
  select *, rank() over(partition by BID, PROVIDER, FROM_DT, THRU_DT order by visit_detail_concept_id) as idx
  from sn_prelim_hcps),
sn_prelim_px as ( 
      SELECT DISTINCT
          BID
        , to_date(FROM_DT, 'ddMMMyyyy') as FROM_DT
        , coalesce(PROVIDER, '0') as PROVIDER 
        , to_date(THRU_DT, 'ddMMMyyyy') as THRU_DT 
        , case when size(array_intersect( collect_set(RVCNTR), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0  
             then 9203 --er
             else 42898160 -- nhiv
          END as visit_detail_concept_id
      FROM `ri.foundry.main.dataset.5b365c5b-e849-460f-886a-5004b5b4523d` sn_p_long
      LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
      ON sn_p_long.PRCDRCD = cxwalk.cms_src_code  and cxwalk.target_domain_id in ('Procedure')
-- ??      WHERE PMT_AMT >= 0 
     group by BID, FROM_DT, PROVIDER, THRU_DT
),
sn_px as ( 
  select *, rank() over(partition by BID, PROVIDER, FROM_DT, THRU_DT order by visit_detail_concept_id) as idx
  from sn_prelim_px),
sn_prelim_dx as ( 
      SELECT DISTINCT
          BID
        , to_date(FROM_DT, 'ddMMMyyyy') as FROM_DT
        , coalesce(PROVIDER, '0') as PROVIDER 
        , to_date(THRU_DT, 'ddMMMyyyy') as THRU_DT 
        , case when size(array_intersect( collect_set(RVCNTR), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0  
               then 9203 --er
               else 42898160 -- nhiv
            END as visit_detail_concept_id
        , collect_set(DX) as dx_set -- check for a diagnosis anywhere in the set of rows instead of each individually.
      FROM `ri.foundry.main.dataset.77e379a1-61af-4303-825a-cbc9e4c9cb9f` sn_d_long
      LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
      ON sn_d_long.DX = cxwalk.cms_src_code  and cxwalk.target_domain_id in ('Observation', 'Condition')
      where sn_d_long.THRU_DT is not null
      group by BID, FROM_DT, PROVIDER, THRU_DT
),
sn_dx as ( 
  select BID, FROM_DT, PROVIDER, THRU_DT, visit_detail_concept_id, 
      rank() over(partition by BID, PROVIDER, FROM_DT, THRU_DT order by visit_detail_concept_id) as idx
  from sn_prelim_dx
  where size(dx_set) > 0
  ),
sn as (
  select * from sn_hcps where idx =1
union ALL
  select * from sn_px where idx=1
union ALL
  select * from sn_dx where idx=1
),
all_visits as (
     /* Skilled nursing visits classified as LTC/Non-hospital Institutional Visit */
    /* ADMSN_DT and max(DSCHRG_DT)? */
     SELECT DISTINCT
       'SN' as source_domain
       , BID
       , PROVIDER as provider_id
       , FROM_DT as visit_detail_start_date
       , THRU_DT as visit_detail_end_date
       , visit_detail_concept_id
       --, 44818517 as visit_type_concept_id
       , 32021 as visit_detail_type_concept_id 
       --,'42898160' as visit_source_value
       , visit_detail_concept_id as visit_detail_source_value -- dubious because this was derived from a list of RVCNTR values, hard to represent the original *singal* value
       -- , CAST ('42898160' AS string) as place_of_service
     FROM sn
      where idx = 1 -- 9203 < 42898160 so ER takes precedence
),

-- Create the VISIT_DETAIL structure here (5.3 right?)
visit_detail_id as (
  SELECT DISTINCT
  md5(concat_ws(
              ';'
        , COALESCE(CAST(BID AS long), 0)
        , COALESCE(provider_id, '0')
        , COALESCE(visit_detail_start_date, '')
        , COALESCE(visit_detail_end_date, '')
        , COALESCE(visit_detail_concept_id, '')
        , COALESCE(visit_detail_source_value, '')
        , COALESCE(source_domain, '')
        , COALESCE('Medicare', '')
     )) AS cms_visit_detail_occurrence_id
    --  visit_detail_id (in later parts of query below)
    , CAST(BID AS long) AS person_id
    , CAST(visit_detail_concept_id AS int)       as visit_detail_concept_id
    , CAST(visit_detail_start_date AS DATE)      AS visit_detail_start_date
    , CAST(visit_detail_start_date as TIMESTAMP) as visit_detail_start_datetime
    , CAST(visit_detail_end_date AS DATE)        AS visit_detail_end_date
    , CAST(visit_detail_end_date as TIMESTAMP)   as visit_detail_end_datetime
    , CAST(visit_detail_type_concept_id AS int)  as visit_detail_type_concept_id
    , COALESCE(CAST(provider_id as long ), 0)    AS provider_id
    , CAST(null as long )                        AS care_site_id
    , CAST(visit_detail_source_value AS string)  as visit_detail_source_value
    , CAST(null AS int) AS visit_detail_source_concept_id
    , CAST(null AS int) AS admitting_source_concept_id
    , CAST(null AS string) AS admitting_source_value
    , CAST(null AS int) AS discharge_to_concept_id
    , CAST(null AS string) AS discharge_to_source_value
    , CAST(null AS long) AS preceeding_visit_detail_id
    , CAST(null AS long) AS visit_detail_parent_id
    , source_domain
    , CAST(null as long) AS visit_occurrence_id 
  FROM all_visits
  WHERE visit_detail_end_date is not null and visit_detail_start_date is not null
     AND CAST( visit_detail_end_date AS DATE) <= CURRENT_DATE()
  )

  SELECT distinct
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    --cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id_51_bit
    , cast(base_10_hash_value as bigint) as visit_detail_id
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
                , cms_visit_detail_occurrence_id as hashed_id
                FROM visit_detail_id
            )
        )
    )