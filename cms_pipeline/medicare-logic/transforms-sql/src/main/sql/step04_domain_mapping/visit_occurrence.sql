 CREATE TABLE `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, EXECUTOR_CORES_EXTRA_LARGE, DRIVER_MEMORY_EXTRA_EXTRA_LARGE') AS

--- ip files contain multiple revenue center code ( from 01 to 46) and segmnt number( 1 to 8) that can over flow into next rows 
 --- and admission type code can also specify the emergency room visit 
 ---1= Emergency: The patient requires immediate medical intervention as a result of severe, life threatening or potentially disabling conditions. 
 ---Generally, the patient is admitted through the emergency room.
 ---5= Trauma: The patient visits a trauma center (A trauma center means a facility licensed or designated by the State or local government authority authorized to do so, 
 ---or as verified by the American College of surgeons and involving a trauma activation.)
 ---https://resdac.org/cms-data/variables/admission-type-code
 --- note that when the code is 2= Urgent, the revenue center may contain ER visit code: The patient requires immediate attention for the care and treatment of a physical 
 ---or mental disorder. Generally, the patient is admitted to the first available and suitable accommodation.
 --- ip visit type will be ip or iper - determined by the revenue center code or the admission type
 --- if TYPE_ADM in ('1', '5') or if the revenue center contains emergency room codes then iper as visit_concept_id

    WITH ip (
      SELECT DISTINCT
          BID
        --, CLAIM_ID
        , to_date(ADMSN_DT, 'ddMMMyyyy') as admit_date
        , PROVIDER
        , to_date(DSCHRGDT, 'ddMMMyyyy') as discharge_date
        --, ORGNPINM as care_site_npi
        ---, SGMTLINE
        , TYPE_ADM
        --, visit_concept_id
        , case when (size(array_intersect( collect_set(REV_CTR), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0 ) OR TYPE_ADM IN ('1', '5') then 262 --erip 
            else 9201 -- inpatient
            END as ip_rev_visit_concept_id
        , collect_set(REV_CTR) as rev_cntr_codes
        , collect_set(SGMT_NUM) as sgmnt_nums
       --- , RANK() OVER (PARTITION BY BID, ADMSN_DT, PROVIDER ORDER BY DSCHRGDT DESC) as idx
      FROM `ri.foundry.main.dataset.236bd229-6e3b-4f53-a684-459bc6ca3c50`
      WHERE PMT_AMT >= 0  /* Acumen suggest to check for payment > 0 */ 
      -- visit dates must be valid discharge date cannot be in the future
      AND ADMSN_DT is not null  AND DSCHRGDT IS not null 
      AND to_date(DSCHRGDT, 'ddMMMyyyy') <= CURRENT_DATE()
      group by 1,2,3,4,5
      order by admit_date ASC
    ),
     opl AS (
      SELECT DISTINCT
          BID
   --     , CLAIM_ID causing dups with different claim number on a same day
        , to_date(REV_DT, 'ddMMMyyyy') as rev_date /* use REV_DT */
        , PROVIDER
        , case when size(array_intersect( collect_set(REV_CNTR), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0  then 9203 --er
            else 9202 -- outpatient
            END as opl_rev_visit_concept_id
       , collect_set(REV_CNTR) as rev_center_codes
       , collect_set(SGMT_NUM) as sgmnt_nums
      ,  cast(size(collect_set(SGMT_NUM)) as string) as sgnmt_num_len
      FROM `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a`
      WHERE PMT_AMT >= 0 /* Acumen suggest to drop claims where REVPMT = 0*/
      AND length(REV_DT) > 0 
      group by 1,2,3
      order by 1, 2 asc
    ),
    -- SN moved to visit_detail
    hs as (
      SELECT DISTINCT
          BID
        --, CLAIM_ID
        , to_date(FROM_DT, 'ddMMMyyyy') as FROM_DT
        , PROVIDER
        , to_date(THRU_DT, 'ddMMMyyyy') as THRU_DT
        --, ORGNPINM as care_site_nip
        , RANK() OVER (PARTITION BY BID, FROM_DT, PROVIDER ORDER BY THRU_DT DESC) as idx
      FROM `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823`       
      WHERE PMT_AMT >= 0 
    ), 
    dm as (
      SELECT DISTINCT
        BID
     -- , CLAIM_ID
      , to_date(EXPNSDT1 , 'ddMMMyyyy') as EXPNSDT1
      , TAX_NUM_ID as PROVIDER
      , to_date(EXPNSDT2 , 'ddMMMyyyy') as EXPNSDT2
      , PLCSRVC
      --, ORD_NPI as ordering_npi 
      , RANK() OVER (PARTITION BY BID, EXPNSDT1, TAX_NUM_ID ORDER BY EXPNSDT2 DESC) as idx
    FROM `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0` dm
    WHERE PMT_AMT >= 0 /*  PMT_AMT must > 0  */
    ),
    pb as (
      SELECT DISTINCT
        BID
      --, CLAIM_ID
      , to_date(EXPNSDT1 , 'ddMMMyyyy') as EXPNSDT1
      --TODO: , TAX_NUM is the actual provider so we would need to use care_site for provider and use tax_num as PROVIDER - we used the BLGNPI column to hold the provider information
      , BLGNPI as PROVIDER
      --TAX_NUM_ID as PROVIDER
      , to_date(EXPNSDT2 , 'ddMMMyyyy') as EXPNSDT2
      , PLCSRVC
      --, PRFNPI as performing_npi
      , RANK() OVER (PARTITION BY BID, EXPNSDT1, BLGNPI, PLCSRVC ORDER BY EXPNSDT2 DESC) as idx
    FROM `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    WHERE PMT_AMT >= 0 
    ),
    pde as (
      SELECT DISTINCT
      BID
      , RECID
      , TO_DATE(RX_DOS_DT, 'ddMMMyyyy') AS RX_DOS_DT
      , TO_DATE(RX_DOS_DT, 'ddMMMyyyy') AS RX_END_DT -- Siggie, Stephanie, Yvette agreed this is better. A 30-day drug supply is not a 30-day visit
      /*
      ,  CASE WHEN CAST(FILL_NUM as int) = 0 THEN DATE_ADD(TO_DATE(RX_DOS_DT, 'ddMMMyyyy'), CAST(DAYS_SUPPLY as int) )
          ELSE DATE_ADD(TO_DATE(RX_DOS_DT, 'ddMMMyyyy'), (CAST(DAYS_SUPPLY as int) * CAST( FILL_NUM as int) )) 
          END as RX_END_DT
          ---fill_num can be 0 so check for 0 before adding dates, DATE_ADD(TO_DATE(RX_DOS_DT, 'ddMMMyyyy'), (CAST(DAYS_SUPPLY as int) * CAST( FILL_NUM as int) )) as RX_END_DT
      */
      , PRESCRIBER_ID as PROVIDER
      --todo: which one is more accurate  -- need to confirm if this place of service for pde
      , SRVC_PROVIDER_ID AS SRVC_PROVIDER
      , RANK() OVER (PARTITION BY BID, RX_DOS_DT ORDER BY RX_DOS_DT DESC) as idx
      FROM `ri.foundry.main.dataset.654c7522-38f1-4ca6-8a09-62abbbbb1991` pde
      WHERE RX_DOS_DT is not null and DAYS_SUPPLY is not NULL and FILL_NUM is not NULL
    ),
    hh as ( /** multiple rev dates may exist, use from date to thru date **/
    SELECT DISTINCT
      BID
      --, CLAIM_ID
      , TO_DATE(FROM_DT, 'ddMMMyyyy') AS FROM_DT
      , TO_DATE(THRU_DT, 'ddMMMyyyy') AS THRU_DT
      , PROVIDER
      --, ORGNPINM as care_site_npi
      FROM `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4`
      WHERE PMT_AMT >= 0
      ),
    /* --union all visits together from the dataset created above ----------------------------------------------------------------------------*/
    /* visit_concept_id will contain the rolled up visit concept id  or visit_concept_id assoicated withe the source files----------*/
    /* ip/er if type_adm is 1 or 5*/

    all_visits as (
     -- ip/er visits using TYPE_ADM  - 262 or if rev_ctr is in('0450', '0451', '0452', '0456', '0459', '0981' )
    SELECT DISTINCT
      'IP' as source_domain
      , BID
      , PROVIDER as provider_id
      , admit_date as visit_start_date
      , discharge_date as visit_end_date
      , ip_rev_visit_concept_id as visit_concept_id
      ---, ip_visits.visit_concept_id as visit_concept_id
      --, 44818517 as visit_type_concept_id /* (Visit derived from encounter on claim) */ non-standard
      , 32021 as visit_type_concept_id 
      , CAST(('TYPE_ADM: ' || cast(TYPE_ADM as string)  || ' REV_CTR:'|| cast( rev_cntr_codes as string) ) as string) as visit_source_value
      ---, CAST(('TYPE_ADM:'||TYPE_ADM) as st_ing) as place_of_service
    FROM ip
    UNION 

    ------------ group all of the revenue code for a given beneficiary and check if any emergency code is present, if so then this claim gets the ER visit else OP visit
    --- from 127,756,154 rows of opl, 28,873,077 rows of visit is defined
    /* Find OP/ER visits using REV_CNTR */
    SELECT DISTINCT
      'OPL' as source_domain
      , BID
      , PROVIDER as provider_id
      , rev_date as visit_start_date
      , rev_date as visit_end_date /* Assume OP visits are always 1 day maximum */
      -- check the rev center codes across all segments to determine the visit concept ids 
      ,  opl_rev_visit_concept_id as visit_concept_id
      --, 44818517 as visit_type_concept_id 
      , 32021 as visit_type_concept_id 
      , CAST(rev_center_codes  as string ) as visit_source_value 
    FROM opl
    
    UNION

    /* Skilled nursing visits classified as LTC/Non-hospital Institutional Visit */
    -- tricky because we're selecting from the long table and get multiple rows for all the RVCNTR values,
    -- The Rank/Over is here to get back to one row per BID, PROVIDER, ADMSN_DT.
    -- BUT, we need to check for any caess where you get > 1 visit_type!!!!! *****
    SELECT distinct 
      vd.source_domain, --  SN
      vs.person_id, -- BID
      vs.provider_id,  -- 
      vs.span_start_date                    as visit_start_date, --
      vs.span_end_date                      as visit_end_date,  --
      vs.visit_detail_concept_id            as visit_concept_id, -- 
      vd.visit_detail_type_concept_id       as visit_type_concept_id, -- 
      vd.visit_detail_source_value          as visit_source_value
      --, vd.visit_detail_source_concept_id     as visit_source_concept_id,
    FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_detail_prelim` vd
    JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/sn_visit_spans_with_detail_id` vs
      ON vs.person_id = vd.person_id and vs.provider_id = vd.provider_id 
     AND vd.visit_detail_start_date = vs.visit_detail_start_date
     AND vd.visit_detail_end_date = vs.visit_detail_end_date
     AND vd.visit_detail_concept_id = vs.visit_detail_concept_id
     and vd.visit_detail_id = vs.visit_detail_id

     UNION ALL
    /* hh as home visit -- should use from_dt and thru_dt to include all dx and hcpcs for this encounter from hh, multiple rev_dtxx exist */
    SELECT DISTINCT
      'HH' as source_domain
      ,  BID
      , PROVIDER as provider_id
      , to_date(FROM_DT, 'ddMMMyyyy') as visit_start_date
      , to_date(THRU_DT, 'ddMMMyyyy') as visit_end_date
      , 581476 as visit_concept_id
      --, 44818517 as visit_type_concept_id
      , 32021 as visit_type_concept_id 
      ,'581476' as visit_source_value
      ---, CAST ('581476' as string ) as place_of_service
   --------------changed to use FROM `ri.foundry.main.dataset.efe1df0b-3f6b-4a25-bfdc-1cbd74a59134`
    FROM hh

    UNION
    /* hs as hospice */
    SELECT DISTINCT
       'HS' as source_domain
      ,  BID
      , PROVIDER as provider_id
      , FROM_DT as visit_start_date
      , THRU_DT as visit_end_date
      , 8546 as visit_concept_id
      --, 44818517 as visit_type_concept_id
      , 32021 as visit_type_concept_id 
      ,'8546' as visit_source_value
      --, CAST ('8546' as string ) as place_of_service
    FROM hs
    WHERE idx = 1 AND FROM_DT IS NOT NULL

    UNION

    /* dm -  */
    SELECT DISTINCT
        'DM' as source_domain
      ,  BID
      , PROVIDER as provider_id
      , EXPNSDT1 as visit_start_date
      , EXPNSDT1 as visit_end_date -- end data should be same as the start -- one day visit for dm
      , vxwalk.target_concept_id as visit_concept_id 
      --, 44818517 as visit_type_concept_id
      , 32021 as visit_type_concept_id 
      -- , concat_ws( ';', COALESCE(vxwalk.source_concept_code, '')
      --       , COALESCE(vxwalk.source_concept_name, '')
      --       , COALESCE(vxwalk.target_concept_id, '')) as visit_source_value
      ---, CAST( vxwalk.source_concept_code as string) as visit_source_value
      , CAST( PLCSRVC as string) as visit_source_value
      --, CAST( ('PLCSRVC: '|| PLCSRVC) as string) as place_of_service     
    FROM dm
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
    ON dm.PLCSRVC = vxwalk.source_concept_code
    where dm.idx = 1 and dm.EXPNSDT2 IS NOT NULL and dm.PLCSRVC is not null

    UNION

    /* pb */
    -- pb claims are contain pharmacy claims, if so make sure the pharmacy visits are only one day long
     SELECT DISTINCT
        'PB' as source_domain
      ,  BID
      , PROVIDER as provider_id
      , EXPNSDT1 as visit_start_date
      --- If the place of service is a pharmacy, we should ensure that the visit construct we are creating reflects a one-day visit.
      ---, EXPNSDT2 as visit_end_date
      ,  CASE 
          WHEN PLCSRVC = '01' THEN EXPNSDT1  -- For pharmacy, 1-day visit
          ELSE EXPNSDT2                      -- For all others, use the recorded end date
      END AS visit_end_date
      , vxwalk.target_concept_id as visit_concept_id  
      --, 44818517 as visit_type_concept_id
      , 32021 as visit_type_concept_id 
      -- , concat_ws( ';', COALESCE(vxwalk.source_concept_code, '')
      --       , COALESCE(vxwalk.source_concept_name, '')
      --       , COALESCE(vxwalk.target_concept_id, '')) as visit_source_value
      -- if there are multiple rows with different pclsrvc for a same patient /same start and end date/ they should all get one visit id 
      -- shong 3/11/2023
      -- todo: we may need to build visit details showing all other cms place of visit within same person's same start /end date of the visit. 
      , CAST(PLCSRVC as string) as visit_source_value
      --, CAST(('PLCSRVC: '|| PLCSRVC) as string) as place_of_service   
    FROM pb
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
    ON pb.PLCSRVC = vxwalk.source_concept_code
    where pb.idx = 1 and pb.EXPNSDT2 IS NOT NULL AND pb.PLCSRVC is not NULL
    UNION
    /** pde - pharmacy visit **/
    SELECT DISTINCT
        'PDE' as source_domain
      ,  BID
      , PROVIDER as provider_id
      , RX_DOS_DT as visit_start_date
      , RX_END_DT as visit_end_date
      , 581458 as visit_concept_id
      , 32021 as visit_type_concept_id
      ---, CAST(  581458 AS string ) as visit_source_value
      ---, CAST( ('service_provider_id :' ||SRVC_PROVIDER ) AS string ) as visit_source_value
       , CAST( ('service_provider_id :' ||SRVC_PROVIDER ) AS string ) as visit_source_value
      --, CAST( ('service_provider_id :' ||SRVC_PROVIDER ) AS string ) AS place_of_service 
    FROM pde  
  ),


  visit_id as (
  SELECT DISTINCT
  md5(concat_ws(
              ';'
        , COALESCE(BID, '')
        , COALESCE(provider_id, '')
        , COALESCE(visit_start_date, '')
        , COALESCE(visit_end_date, '')
        , COALESCE(visit_concept_id, '')
        , COALESCE(visit_source_value, '')
        , COALESCE(source_domain, '')
        , COALESCE('Medicare', '')
     )) AS cms_visit_occurrence_id

    , CAST(BID AS long) AS person_id
    , CAST(visit_concept_id AS int) as visit_concept_id
    , CAST( visit_start_date AS DATE) AS visit_start_date
    , CAST(visit_start_date as TIMESTAMP) as visit_start_datetime
    , CAST( visit_end_date AS DATE) AS visit_end_date
    , CAST(visit_end_date as TIMESTAMP) as visit_end_datetime

    , CAST( visit_type_concept_id AS int) as visit_type_concept_id
    ,  CAST( null as long ) AS provider_id
    , CAST( provider_id AS long) AS care_site_id
    ,  CAST(visit_source_value AS string) as visit_source_value
    , CAST(null AS int) AS visit_source_concept_id

    , CAST(null AS int) AS admitting_source_concept_id
    , CAST(null AS string) AS admitting_source_value
    , CAST(null AS int) AS discharge_to_concept_id
    , CAST(null AS string) AS discharge_to_source_value
    , CAST(null AS long) AS preceding_visit_occurrence_id
    , source_domain
  FROM all_visits
  WHERE visit_end_date is not null and visit_start_date is not null
  -- NOT future dates
  AND CAST( visit_end_date AS DATE) <= CURRENT_DATE()
  )
  SELECT distinct
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    --cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id_51_bit
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
                , cms_visit_occurrence_id as hashed_id
                FROM visit_id
            )
        )
    )