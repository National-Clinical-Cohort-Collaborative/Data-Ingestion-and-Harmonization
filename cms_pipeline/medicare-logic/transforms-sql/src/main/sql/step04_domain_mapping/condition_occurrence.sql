CREATE TABLE `ri.foundry.main.dataset.e15d5112-7dd5-49b3-a585-dc173f1d39af` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, EXECUTOR_CORES_EXTRA_LARGE, DRIVER_MEMORY_EXTRA_EXTRA_LARGE') AS
    --condition should be sourced from the following source files:   opl, ip, pb, sn, hh, dm, hs   
    --code map PDGNS_CD/DGNSCD01/AD_DGNS - add logic to make use of code in these columns--
    WITH code_lookup AS (
    SELECT
      c.concept_id as source_concept_id
    , replace(c.concept_code, '.', '') as source_concept_code /* CMS uses ICD10 codes with periods */
    , c.vocabulary_id as source_vocabulary
    , cr.concept_id_2 as target_concept_id
  FROM `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
  INNER JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr
  ON c.concept_id = cr.concept_id_1
  WHERE c.vocabulary_id = 'ICD10CM'
  ),
    ip AS (
    SELECT DISTINCT
        BID
      --, CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(ADMSN_DT, 'ddMMMyyyy') as condition_start_date
      , to_date(DSCHRGDT, 'ddMMMyyyy') as condition_end_date
      , cxwalk.source_concept_code as condition_source_value
      , cxwalk.target_concept_id as condition_concept_id
      , cxwalk.source_concept_id as condition_source_concept_id
      , 'IP' as source_domain
      , DX_col as condition_status_source_value
      , case when ip.DX_col = 'PDGNS_CD'  then 32902 --PDGNS_CD
          else 32908 --secondary diagnosis condition status 
          end as condition_status_concept_id
    FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/ip_diagnosis_long` ip
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` cxwalk
    ON ip.DX = cxwalk.cms_src_code AND cxwalk.src_vocab_code = 'ICD10CM'
    where cxwalk.target_domain_id = 'Condition' and ip.DX is not NULL and ADMSN_DT is not null and DSCHRGDT is not null and PMT_AMT >=0 
    -- the discharge date cannot be in the future 
    and to_date(DSCHRGDT, 'ddMMMyyyy') <= CURRENT_DATE()
    ),

    
    ----# Since we have a lot of duplicate diagnosis codes with different REV_CTR, we look for distinct date/diagnosis/visit types
    ---separate 9203 and 9202 in visits
   ---if ( "REV_CNTR").isin('0450', '0451', '0452', '0456', '0459', '0981'), F.lit(9203)) otherwise F.lit(9202)
   --- distinct select on "BID", "CLAIM_ID", "PROVIDER",  "FROM_DT", "THRU_DT", "DX_col", "DX", "visit_concept_id"
   ---- outpatient is 1 day
    opl as (
     SELECT  DISTINCT 
        op.BID
     -- , op.CLAIM_ID
      , op.PROVIDER as care_site_id
      , to_date(op.REV_DT, 'ddMMMyyyy') as condition_start_date
      , to_date(op.REV_DT, 'ddMMMyyyy') as condition_end_date
      , op.visit_concept_id
      , cxwalk.source_concept_code as condition_source_value
      , cxwalk.target_concept_id as condition_concept_id
      , cxwalk.source_concept_id as condition_source_concept_id
      , 'OPL' as source_domain
      , DX_col as condition_status_source_value
      , case when op.DX_col = 'PDGNS_CD'  then 32902 --PDGNS_CD - primary diagnosis condition status
          else 32908 --secondary diagnosis condition status 
          end as condition_status_concept_id
    FROM `ri.foundry.main.dataset.4925b4a7-9b22-41e6-b33c-707a09d0d81e` op
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` cxwalk
    ON op.DX = cxwalk.cms_src_code  and cxwalk.src_vocab_code = 'ICD10CM'
    where cxwalk.target_domain_id = 'Condition' and op.DX is not NULL and op.REV_DT is not null and PMT_AMT >= 0
    ), 

    hh as (
    SELECT DISTINCT
        BID
     -- , CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(FROM_DT, 'ddMMMyyyy') as condition_start_date
      , to_date(THRU_DT, 'ddMMMyyyy') as condition_end_date
      , cxwalk.source_concept_code as condition_source_value
      , cxwalk.target_concept_id as condition_concept_id
      , cxwalk.source_concept_id as condition_source_concept_id
      , CAST ( 581476 AS int ) as visit_concept_id
      , 'HH' as source_domain
      , col_num as source_domain_col_num
      , DX_col as condition_status_source_value
      , case when hh.DX_col = 'DGNSCD01' then 32902 --DGNSCD01 same as PDGNS_CD - primary diagnosis condition status
          else 32908 --secondary diagnosis condition status 
          end as condition_status_concept_id
      FROM `ri.foundry.main.dataset.8ae606e0-96dc-4b39-ae61-f3fa9f5fd947` hh
    ---multiple rev date can exist - use from and thru dt  
    /** ---FROM `ri.foundry.main.dataset.efe1df0b-3f6b-4a25-bfdc-1cbd74a59134` hh ---**/
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` cxwalk
    ON hh.DX = cxwalk.cms_src_code  --and cxwalk.src_vocab_code = 'ICD10CM'
    where cxwalk.target_domain_id = 'Condition' and hh.DX is not null and PMT_AMT > 0 and FROM_DT is not null and THRU_DT is not null 
    ),
    
    pb as (
      SELECT DISTINCT
        BID
     -- , CLAIM_ID
      , BLGNPI as care_site_id
      , to_date(EXPNSDT1 , 'ddMMMyyyy') as condition_start_date
      , to_date(EXPNSDT2 , 'ddMMMyyyy') as condition_end_date

      , cxwalk.source_concept_code as condition_source_value
      , cxwalk.target_concept_id as condition_concept_id
      , cxwalk.source_concept_id as condition_source_concept_id
      , case when pb.DX_col = 'PDGNS_CD'  then 32902
         when pb.DX_col ='LINEDGNS' then 4249118  -- reason for visit diagnosis, LINEDGNS code is the reason for performing the procedure, besure to add this status in procedure domain
         else 32908 --secondary diagnosis condition status 
         end as condition_status_concept_id
      , pb.DX_col as condition_status_source_value
      ---cms place of service 
      , pb.PLCSRVC as place_of_service_source_value
      , plc.target_concept_id as place_of_service_concept_id 
      , plc.source_concept_name as place_of_service_concept_name
      , plc.target_concept_id as visit_concept_id
      , 'PB' as source_domain
      , DX_col as source_domain_col
      , col_num as source_domain_col_num
    FROM `ri.foundry.main.dataset.84a03b0f-f74a-404f-a839-9f1457f4379c` pb
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` cxwalk
    ON pb.DX = cxwalk.cms_src_code  --and cxwalk.src_vocab_code = 'ICD10CM'
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` plc 
    on plc.source_concept_code = pb.PLCSRVC  
    where cxwalk.target_domain_id = 'Condition' and pb.DX is not null and PMT_AMT > 0 and EXPNSDT1 is not null and EXPNSDT2 is not null
    ),
   
    sn as (
      SELECT DISTINCT
        BID
    --  , CLAIM_ID
      , PROVIDER as care_site_id
      --, to_date(ADMSN_DT , 'ddMMMyyyy') as condition_start_date
      , to_date(FROM_DT , 'ddMMMyyyy') as condition_start_date
      , to_date(THRU_DT , 'ddMMMyyyy') as condition_end_date
      , cxwalk.source_concept_code as condition_source_value
      , cxwalk.target_concept_id as condition_concept_id
      , cxwalk.source_concept_id as condition_source_concept_id
      , 'SN' as source_domain  
      , col_num as source_domain_col_num
      , DX_col as condition_status_source_value
      , case when sn.DX_col = 'PDGNS_CD' then 32902 --DGNSCD01 same as PDGNS_CD => both should get the primary diagnosis condition status
          when sn.DX_col = 'AD_DGNS' then 32890 
          else 32908 --secondary diagnosis condition status 
          end as condition_status_concept_id 
    FROM `ri.foundry.main.dataset.77e379a1-61af-4303-825a-cbc9e4c9cb9f` sn
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` cxwalk
    ON sn.DX = cxwalk.cms_src_code  --and cxwalk.src_vocab_code = 'ICD10CM'
    where cxwalk.target_domain_id = 'Condition' and sn.DX is not null
    ), 
    dm as (
      SELECT DISTINCT
        BID
     -- , CLAIM_ID
      , TAX_NUM_ID as care_site_id
      , to_date(EXPNSDT1 , 'ddMMMyyyy') as condition_start_date
      , to_date(EXPNSDT2 , 'ddMMMyyyy') as condition_end_date
      , cxwalk.source_concept_code as condition_source_value
      , cxwalk.target_concept_id as condition_concept_id
      , cxwalk.source_concept_id as condition_source_concept_id
      , plc.target_concept_id as visit_concept_id
      , 'DM' as source_domain
      , col_num as source_domain_col_num
      , DX_col as condition_status_source_value
      , case when dm.DX_col = 'DGNSCD01' then 32902 --DGNSCD01 same as PDGNS_CD - primary diagnosis condition status
          else 32908 --secondary diagnosis condition status 
          end as condition_status_concept_id 
    FROM `ri.foundry.main.dataset.fc107626-b36b-4e0b-b036-dcf2f548d73d` dm
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` cxwalk
    ON dm.DX = cxwalk.cms_src_code  --and cxwalk.src_vocab_code = 'ICD10CM'
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` plc 
    on plc.source_concept_code = dm.PLCSRVC  
    where cxwalk.target_domain_id = 'Condition' and dm.DX is not null
    ),
    
    hs as ( 
      SELECT DISTINCT 
        BID
     -- , CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(FROM_DT , 'ddMMMyyyy') as condition_start_date
      , to_date(THRU_DT , 'ddMMMyyyy') as condition_end_date
      , cxwalk.source_concept_code as condition_source_value
      , cxwalk.target_concept_id as condition_concept_id
      , cxwalk.source_concept_id as condition_source_concept_id
      , 'HS' as source_domain
      , col_num as source_domain_col_num
      , DX_col as condition_status_source_value
      , case when hs.DX_col = 'DGNSCD01'  then 32902
         else 32908 --secondary diagnosis condition status 
         end as condition_status_concept_id
    FROM `ri.foundry.main.dataset.8f878ba2-acae-41d2-b5cc-bf15e26f5861` hs
    LEFT JOIN  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` cxwalk
    ON hs.DX = cxwalk.cms_src_code  --and cxwalk.src_vocab_code = 'ICD10CM'
    where cxwalk.target_domain_id = 'Condition' and hs.DX is not null
    ),

   ------------map visit from each claim sources 
   /* we join op events back to the visit table on bid data of op event 
      and visit_concept_id( could be either 9202 or 9203)
   */
  --  /*er visit or non-er visits */
  --  -- 9203 =Emergency Room Visit , 9202 = outpatient visit
  --     where visit.visit_concept_id in ( 9203, or 9202) 
    op_visit as (
      select distinct
      opl.BID
    --, opl.CLAIM_ID
    , opl.care_site_id
    , opl.condition_start_date
    , opl.condition_end_date
    , opl.condition_source_value
    , opl.condition_source_concept_id
    , opl.condition_concept_id
     --primary dx
    ,condition_status_source_value
    ,condition_status_concept_id
      --
    , visit.visit_occurrence_id 
    , visit.visit_concept_id
    , null as visit_detail_id -- (additional)
    , opl.source_domain
      from opl
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` visit
      on opl.BID = visit.person_id
      and opl.condition_start_date = visit.visit_start_date
      and opl.condition_end_date = visit.visit_end_date
      and opl.visit_concept_id = visit.visit_concept_id
      and opl.care_site_id = visit.care_site_id
      and visit.source_domain = 'OPL' 
      where opl.visit_concept_id is not null
    ),

    /** we join ip events back to the visit table on BID and admimission date of IP and provider **/
    erip_visit as ( -- 9201	IP	Inpatient Visit / 	Emergency Room Visit 9203 = ER visit
       SELECT distinct
        ip.BID
      --, ip.CLAIM_ID
      , ip.care_site_id
      , ip.condition_start_date
      , ip.condition_end_date
      , ip.condition_source_value
      , ip.condition_source_concept_id
      , ip.condition_concept_id
       --primary dx
      , condition_status_source_value
      , condition_status_concept_id
      --
      , v.visit_occurrence_id
      , v.visit_concept_id
      , null as visit_detail_id -- (additional)
      , ip.source_domain
      FROM ip
      JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` v
      on ip.BID = v.person_id
      and ip.condition_start_date = v.visit_start_date
      and ip.condition_end_date = v.visit_end_date
      and ip.care_site_id = v.care_site_id
      and visit_concept_id = 262
      and v.source_domain = 'IP'
    ),

    ip_visit as ( -- 9201	IP	Inpatient Visit / 	Emergency Room Visit 9203 = ER visit
       SELECT distinct
        ip.BID
     -- , ip.CLAIM_ID
      , ip.care_site_id
      , ip.condition_start_date
      , ip.condition_end_date
      , ip.condition_source_value
      , ip.condition_source_concept_id
      , ip.condition_concept_id
      --primary dx
      , condition_status_source_value
      , condition_status_concept_id
      -- 
      , v.visit_occurrence_id
      , v.visit_concept_id
      , null as visit_detail_id -- (additional)
      , ip.source_domain
      FROM ip
      JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` v
      on ip.BID = v.person_id
      and ip.condition_start_date = v.visit_start_date
      and ip.condition_end_date = v.visit_end_date
      and ip.care_site_id = v.care_site_id
      and visit_concept_id = 9201
      and v.source_domain = 'IP'
    ),

    pb_visit as (
     SELECT distinct
        pb.BID
     -- , pb.CLAIM_ID -- if the code is repeated for the same person with same provider for the same start and end dates but different claim no this data is not usuful in OMOP 
     -- select distinct without claimid and insert to condtion. 
      , pb.care_site_id
      , pb.condition_start_date
      , pb.condition_end_date
      , pb.condition_source_value
      , pb.condition_source_concept_id
      , pb.condition_concept_id
      -- primary diagnosis
      , pb.condition_status_source_value
      , pb.condition_status_concept_id
      -- do we need to know the original place of service ?
      , v.visit_occurrence_id
      , v.visit_concept_id
      , null as visit_detail_id -- (additional)
      , pb.source_domain
      FROM pb
      JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` v
      on pb.BID = v.person_id
      and pb.condition_start_date = v.visit_start_date
      and pb.condition_end_date = v.visit_end_date
      and pb.care_site_id = v.care_site_id 
      and pb.place_of_service_concept_id = v.visit_concept_id
      and v.source_domain = 'PB'
    ),
    
     sn_visit as (
        SELECT distinct
         sn.BID
       , sn.care_site_id
       , sn.condition_start_date
       , sn.condition_end_date
       , sn.condition_source_value
       , sn.condition_source_concept_id
       , sn.condition_concept_id 
       , sn.condition_status_source_value
       , sn.condition_status_concept_id
       , vd.visit_occurrence_id
       , vd.visit_detail_concept_id as visit_concept_id
       , vd.visit_detail_id -- (additional)
       , sn.source_domain
       FROM sn
       JOIN `ri.foundry.main.dataset.57c52f98-102f-487a-94c6-9d2c792a8fcb` vd
       on sn.BID = vd.person_id and sn.care_site_id = vd.care_site_id
       and sn.condition_start_date = vd.visit_detail_start_date and sn.condition_end_date = vd.visit_detail_end_date
       and vd.visit_detail_concept_id in (42898160, 9203)  -- **visit** concept, not domain_concept, not condition_concept_id. OK.
       and vd.source_domain = 'SN'
     ),


    hh_visit as (
     SELECT distinct
        hh.BID
      --, hh.CLAIM_ID
      , hh.care_site_id
      , hh.condition_start_date
      , hh.condition_end_date
      , hh.condition_source_value
      , hh.condition_source_concept_id
      , hh.condition_concept_id
      --primary dx
      , condition_status_source_value
      , condition_status_concept_id
      --
      , v.visit_occurrence_id
      , v.visit_concept_id
      , null as visit_detail_id -- (additional)
      , hh.source_domain
      FROM hh
      JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` v
      on hh.BID = v.person_id
      and hh.condition_start_date = v.visit_start_date
      and hh.condition_end_date = v.visit_end_date
      and hh.care_site_id = v.care_site_id
      and v.visit_concept_id = 581476
       and v.source_domain = 'HH'
    ), 
    dm_visit as (
      SELECT distinct
        dm.BID
     -- , dm.CLAIM_ID
      , dm.care_site_id
      , dm.condition_start_date as condition_start_date
      , dm.condition_start_date as condition_end_date -- dm visits are 1 day long how ever the domain dates should capture both dates from dm
      , dm.condition_source_value
      , dm.condition_source_concept_id
      , dm.condition_concept_id
      --primary dx
      , condition_status_source_value
      , condition_status_concept_id
      --
      , v.visit_occurrence_id
      , v.visit_concept_id
      , null as visit_detail_id -- (additional)
      , dm.source_domain
      FROM dm
      JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` v
      on dm.BID = v.person_id
      and dm.condition_start_date = v.visit_start_date
      and dm.condition_start_date = v.visit_end_date -- dm visits are one dy
      and dm.visit_concept_id = v.visit_concept_id 
      and dm.care_site_id = v.care_site_id
      and v.source_domain = 'DM'
    ),
    hs_visit as (
      SELECT distinct
        hs.BID
    --  , hs.CLAIM_ID
      , hs.care_site_id
      , hs.condition_start_date
      , hs.condition_end_date
      , hs.condition_source_value
      , hs.condition_source_concept_id
      , hs.condition_concept_id
      --primary dx
      , condition_status_source_value
      , condition_status_concept_id
      --
      , v.visit_occurrence_id
      , v.visit_concept_id
      , null as visit_detail_id -- (additional)
      , hs.source_domain
      FROM hs
      JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` v
      on hs.BID = v.person_id
      and hs.condition_start_date = v.visit_start_date
      and hs.condition_end_date = v.visit_end_date
      and hs.care_site_id = v.care_site_id
      and v.visit_concept_id = 8546
      and v.source_domain = 'HS'
    ),

    --ip, op,  pb, sn, hh, dm, hs
    final_co as (
    select distinct * from ip_visit
    union
    select distinct * from erip_visit
    union
    select distinct * from op_visit
    union
    select distinct * from pb_visit
    union
    select distinct * from sn_visit
    union 
    select distinct * from hh_visit
    union
    select distinct * from dm_visit
    union
    select distinct * from hs_visit
    ),

    cms_condition_occurrence as (
    select distinct
      md5(concat_ws(
              ';'
        , COALESCE(co.BID, '')
        , COALESCE(co.care_site_id, '')
        , COALESCE(co.condition_start_date, '')
        , COALESCE(co.condition_end_date, '')
        , COALESCE(co.condition_concept_id, '')
        , COALESCE(co.condition_source_value, '')
        , COALESCE(co.visit_concept_id, '' )
        , COALESCE(co.source_domain, '' )
        , COALESCE(co.condition_status_concept_id, '')
        , COALESCE(co.condition_status_source_value, '')
        , COALESCE(co.visit_occurrence_id, '')
        , COALESCE('Medicare', '')
        )) as cms_hashed_condition_occurrence_id
        , CAST( co.BID AS long ) as person_id 
            , cast(condition_concept_id as int) as condition_concept_id
            , cast( condition_start_date as date) as condition_start_date
            , cast( condition_start_date as timestamp ) as condition_start_datetime
            , cast( condition_end_date as date) as condition_end_date
            , cast( condition_end_date as timestamp )  as condition_end_datetime
            , cast( 32810 as int) as condition_type_concept_id
            , cast( condition_status_concept_id as int) condition_status_concept_id -- need to reference the original column names in order to determine the condition status
            , cast(null as string ) as stop_reason
            , cast( null as long) as provider_id
            , cast( co.care_site_id as long) as care_site_id
            , cast( co.visit_occurrence_id as bigint) as visit_occurrence_id
            , cast(visit_detail_id as bigint) as visit_detail_id
            , cast( co.condition_source_value as string) as condition_source_value
            , cast( co.condition_source_concept_id as int) as condition_source_concept_id
            , cast( co.condition_status_source_value as string ) as condition_status_source_value --- value for the condition_status_concept_id- how dx was givne -build using the column and the seqnum
            , co.source_domain
  from final_co co
  left join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/person` p on co.BID = p.person_id
  -- match visit on bid, provider_id, visit start_date and end_date, visit_concept_id( that are all unique based on the source domain, 32810= visit_type_concept_id is used for claims data.  
  /** TODO: join when person is ready person is empty**/  
  where condition_concept_id is not null
    )

    SELECT
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    --cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id_51_bit
    --, cast(conv(substr(cms_hashed_condition_occurrence_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id
    , cast(conv(substr(cms_hashed_condition_occurrence_id, 1, 15), 16, 10) as bigint) as condition_occurrence_id
    FROM cms_condition_occurrence