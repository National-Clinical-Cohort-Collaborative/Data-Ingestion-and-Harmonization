CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/condition_occurrence` AS

---need condition_concept_id, condition_start_datetime, condition_end_datetime, condition_type_concept_id, 
---condition_status_concept_id, stop_reason, condition_source_value, condition_source_concept_id,
---condition_status_source_value, source_domain, condition_occurrence_id, data_partner_id, condition_concept_name, condition_source_concept_name
    WITH ip AS (
        SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
        --, ADMSN_TYPE_CD -- only present in ip
        --, REV_CNTR_CD
        , BLG_PRVDR_NPI as care_site_id --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset, some of these have alpha characters and cast to null!
        , SRVC_PRVDR_NPI as provider_id
        , to_date(ADMSN_DT, 'ddMMMyyyy') as ADMSN_DT
        , to_date(DSCHRG_DT, 'ddMMMyyyy') as DSCHRG_DT
        , xw.source_concept_code as condition_source_value 
        , xw.target_concept_id as condition_concept_id
        , xw.source_concept_id as condition_source_concept_id
        , visit_concept_id
        , 32021 as visit_type_concept_id
        , case when ip.src_column = 'ADMTG_DGNS_CD' then 32890 ----32890 admission dx
               when ip.src_column ='DGNS_CD_1' then 32899 -- 32899 primary dx
               else 0 
               end as condition_status_concept_id
        , ip.src_column as condition_status_source_value
        , 'IP' as source_domain
        , RANK() OVER (PARTITION BY PSEUDO_ID, ADMSN_DT, BLG_PRVDR_NPI ORDER BY DSCHRG_DT DESC) as idx
        FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ip_dx_long_prepared` ip
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/codemap_medicaid_xwalk` xw
        ON ip.src_code = xw.cms_src_code AND xw.mapped_code_system = 'ICD10CM'
        where xw.target_domain_id = 'Condition' and ip.src_code is not NULL and ADMSN_DT is not NULL and DSCHRG_DT is not NULL 
        and MDCD_PD_AMT >= 0 
    ),
    

    ip_visit AS (
    select distinct
        ip.medicaid_person_id
        , ip.care_site_id
        , ip.provider_id
        , ip.ADMSN_DT as condition_start_date
        , ip.DSCHRG_DT as condition_end_date
        , ip.condition_source_value
        , ip.condition_concept_id
        , ip.condition_source_concept_id
        , visit.visit_occurrence_id
        , ip.visit_concept_id
        , ip.visit_type_concept_id
        , ip.condition_status_concept_id
        , ip.condition_status_source_value
        , ip.source_domain
        from ip
        left join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` visit
        on ip.medicaid_person_id = visit.person_id
        and ip.ADMSN_DT = visit.visit_start_date
        and ip.DSCHRG_DT = visit.visit_end_date
        ---and ip.visit_concept_id = visit.visit_concept_id
        and ip.care_site_id = visit.care_site_id -- match in care_site should be sufficient
        --and ip.provider_id = visit.provider_id --provider may be null 
        and visit.source_domain = 'IP'
        WHERE ip.idx = 1
    ),

    ot AS (
        SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
        --, BILL_TYPE_CD
        --, BILL_TYPE_CD_digit3
        --, REV_CNTR_CD
        , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_id 
        , SRVC_PRVDR_NPI as provider_id
        , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as SRVC_BGN_DT
        , to_date(SRVC_END_DT, 'ddMMMyyyy') as SRVC_END_DT
        , cxwalk.source_concept_code as condition_source_value 
        , cxwalk.target_concept_id as condition_concept_id
        , cxwalk.source_concept_id as condition_source_concept_id
        , 'OT' as source_domain
        , visit_concept_id -- place holder, is used when creating condition_occurrence_id
        , 32021 as visit_type_concept_id
        --DGNS_CD_1 primary
        ,case when ot.src_column ='DGNS_CD_1' then 32899 -- 32899 primary dx
               else 0
        end as condition_status_concept_id
        , ot.src_column as condition_status_source_value
        , RANK() OVER (PARTITION BY PSEUDO_ID, SRVC_BGN_DT, BLG_PRVDR_NPI ORDER BY SRVC_END_DT DESC) as idx
        FROM `ri.foundry.main.dataset.161d5539-7fa8-42cd-bf00-b980f8b70b26` ot
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/codemap_medicaid_xwalk` cxwalk
        ON ot.src_code= cxwalk.cms_src_code AND cxwalk.mapped_code_system = 'ICD10CM'
        where cxwalk.target_domain_id = 'Condition' and SRVC_BGN_DT is not NULL and SRVC_END_DT is not NULL
        AND CLM_TYPE_CD NOT IN ('Z') -- must be a valid claim type
    ),

    ot_visit as (
    SELECT DISTINCT
        ot.medicaid_person_id
        , ot.care_site_id
        , ot.provider_id
        , ot.SRVC_BGN_DT as condition_start_date
        , ot.SRVC_END_DT as condition_end_date
        , ot.condition_source_value
        , ot.condition_concept_id
        , ot.condition_source_concept_id
        , visit.visit_occurrence_id
        , ot.visit_concept_id
        , ot.visit_type_concept_id
        , condition_status_concept_id
        , condition_status_source_value
        , ot.source_domain
        from ot
        join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` visit
        on ot.medicaid_person_id = visit.person_id
        and ot.SRVC_BGN_DT = visit.visit_start_date
        and ot.SRVC_END_DT = visit.visit_end_date
        and ot.care_site_id = visit.care_site_id
        ---and ot.provider_id = visit.provider_id
        ---and ot.visit_concept_id = visit.visit_concept_id
        and visit.source_domain = 'OT' --check later
        WHERE ot.idx = 1
    ),

    lt as (
        SELECT DISTINCT
        PSEUDO_ID as medicaid_person_id
        , BILL_TYPE_CD_digit3
        --, REV_CNTR_CD
        --, BILL_TYPE_CD
        , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_id ---if null use SRVC_PRVDR_NPI
        , SRVC_PRVDR_NPI as provider_id -- SRVC_PRVDR_NPI as provider_npi
        , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as start_date 
        , to_date(SRVC_END_DT, 'ddMMMyyyy') as end_date
        , cxwalk.source_concept_code as condition_source_value 
        , cxwalk.target_concept_id as condition_concept_id
        , cxwalk.source_concept_id as condition_source_concept_id
        , visit_concept_id
        , 32021 as visit_type_concept_id
        , case when lt.src_column = 'ADMTG_DGNS_CD' then 32890 ----32890 admission dx
               when lt.src_column ='DGNS_CD_1' then 32899 -- 32899 primary dx
               else 0
        end as condition_status_concept_id
        , lt.src_column as condition_status_source_value ------order of the condition, we can use the dx column 
        , 'LT' as source_domain
        FROM `ri.foundry.main.dataset.66b02062-9730-49e0-b5e1-9cc04ed76a2c` lt
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/codemap_medicaid_xwalk` cxwalk
        ON lt.src_code = cxwalk.cms_src_code AND cxwalk.mapped_code_system = 'ICD10CM'
        where cxwalk.target_domain_id = 'Condition' and lt.src_code is not NULL and SRVC_BGN_DT is not NULL and SRVC_END_DT is not NULL 
        and MDCD_PD_AMT >= 0
        AND CLM_TYPE_CD NOT IN ( 'Z') -- must be a valid claim type
    ),
    lt_visit as (
    SELECT DISTINCT
        lt.medicaid_person_id
        , lt.care_site_id
        , lt.provider_id
        , vo.visit_start_date 
        , vo.visit_end_date
        , lt.condition_source_value
        , lt.condition_concept_id
        , lt.condition_source_concept_id
        , vd.visit_occurrence_id
        , lt.visit_concept_id
        , lt.visit_type_concept_id
        , condition_status_concept_id
        , condition_status_source_value
        , lt.source_domain
        from lt

        join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_detail` vd
         on lt.medicaid_person_id = vd.person_id
        and lt.care_site_id = vd.care_site_id --care site match should be sufficient
        -- and lt.provider_id = vd.provider_id -- CR
        and lt.start_date = vd.visit_detail_start_date
        and lt.end_date = vd.visit_detail_end_date
        and vd.source_domain = 'LT' -- LT source claim
        --and lt.visit_concept_id = visit.visit_concept_id

        join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` vo
          on vo.person_id = vd.person_id
         and vo.visit_occurrence_id = vd.visit_occurrence_id
    ),

    final_co as (
    select distinct * from ip_visit
    UNION
    select distinct * from ot_visit
    UNION
    select distinct * from lt_visit
    ),

    condition_id as (
    select DISTINCT
        md5(concat_ws(
            ';'
            , COALESCE(co.medicaid_person_id, '')
            , COALESCE(co.provider_id, '')
            , COALESCE(co.condition_start_date, '')
            , COALESCE(co.condition_end_date, '')
            , COALESCE(co.condition_concept_id, '')
            , COALESCE(co.condition_source_value, '')
            , COALESCE(co.condition_source_concept_id, '')
            , COALESCE(co.visit_concept_id, '')
            , COALESCE(co.visit_occurrence_id, '')
            , COALESCE(co.care_site_id, '')
            , COALESCE(co.source_domain, '')
            , COALESCE('medicaid', '')
        )) as cms_medicaid_condition_occurrence_id
        , co.medicaid_person_id AS person_id
        , CAST(co.condition_concept_id as int) as condition_concept_id
        , CAST(co.condition_start_date as date) as condition_start_date
        , CAST(co.condition_start_date as timestamp) as condition_start_datetime
        , CAST(co.condition_end_date as date) as condition_end_date
        , CAST(co.condition_end_date as timestamp) as condition_end_datetime
        , CAST(32810 as int) as condition_type_concept_id --need review
        , CAST(0 as int) as condition_status_concept_id --need review "-- need to reference the original column names in order to determine the condition status"
        , CAST(null as string) as stop_reason
        , CAST(co.provider_id as long) as provider_id -- some will fail to null b/c of alpha chars
        , CAST(co.care_site_id as long) as care_site_id
        , CAST(co.visit_occurrence_id as long) as visit_occurrence_id
        , CAST(null as long) as visit_detail_id
        , CAST(co.condition_source_value as string) as condition_source_value
        , CAST(co.condition_source_concept_id as int) as condition_source_concept_id
        , CAST(null as string) as condition_status_source_value -- need review "--- value for the condition_status_concept_id- how dx was givne -build using the column and the seqnum"
        , co.source_domain
    from final_co co
    left join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/person` p on co.medicaid_person_id = p.person_id
    where condition_concept_id is not null
    )

  SELECT distinct
      c.*
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    --cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as domain_id_51_bit
    , cast(c.base_10_hash_value as bigint) as condition_occurrence_id
    , m.macro_visit_long_id 
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
                , cms_medicaid_condition_occurrence_id as hashed_id
                FROM condition_id
            )
        )
    ) c
    left join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit_with_micro_visit` m on m.visit_occurrence_id = c.visit_occurrence_id