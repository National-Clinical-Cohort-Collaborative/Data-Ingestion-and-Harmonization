CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_detail_folder/ot_visit_detail` AS
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