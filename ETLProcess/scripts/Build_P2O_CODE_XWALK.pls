/******************************************************************************************************************************
---diagnosis -'ICD9CM', 'ICD10CM', 'ICD11CM', 'SNOMED'
---procedures -'ICD9CM', 'ICD9Proc', 'ICD10CM', 'ICD10PCS', 'OPCS4','CPT4', 'ICD11CM', 'LOINC', 'HCPCS', 'NDC'
---condition - 'ICD9CM', 'ICD10CM', 'ICD11CM', 'SNOMED', 'PCORNet'
---death cause - 'ICD9CM', 'ICD10CM' +2
---lab_results_cm - 'LOINC'--52,212 labs
---dispensing - 'NDC'
---med_admin - 'RxNorm','NDC'
---prescribing - 'RxNorm'
---obsclin_code - 'LOINC', 'SNOMED'
---obs_gen --- type = pc_covid code=3000 or 2000
---Description: stored procedure to create the code map translation table from PCORnet terminology to OMOP concept ids
---Reviewers:  Blacketer,Clair / Kostka, Kristin / 
---Author: Stephanie Hong / Blacketer,Clair
---Project: N3C
---
----- Unable to render PROCEDURE DDL for object CDMH_STAGING.BUILD_P2O_CODE_XWALK with DBMS_METADATA attempting internal generator.
Revisions:
     Ver          Date        Author        Description
     0.1       5/20/2020       SHONG        Initial Version
     0.2       6/16/2020      SHONG         Look up terminology only in the source vocabulary is found 
     0.3       6/26/2020      SNaredla      Added insert and commit after each domain map to address memory issue of holding
                                              onto one big map before the insert.
*******************************************************************************************************************************/

CREATE PROCEDURE                CDMH_STAGING.BUILD_P2O_CODE_XWALK (
  RECORDCOUNT OUT NUMBER
) AS
count1 number;
count2 number;
count3 number;
count4 number;
count5 number;
count6 number;
count7 number;
count8 number;
count9 number;
BEGIN

-- truncate before building - p2o_code_xwalk_standard
execute immediate 'truncate table CDMH_STAGING.p2o_code_xwalk_standard';
commit ;    
   
 INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)     
    select DISTINCT 'DIAGNOSIS' as cdm_tbl, x.src_code as src_code, x.src_code_type as src_code_type, x.src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
        from
        (     
    WITH CTE_VOCAB_MAP AS 
        (
                   SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID,
                                  c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
                                  c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON,
                                  c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
                                  c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
                   FROM CDMH_STAGING.CONCEPT C
                         JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
                                    ON C.CONCEPT_ID = CR.CONCEPT_ID_1
                                    AND CR.invalid_reason IS NULL
                                    AND lower(cr.relationship_id) = 'maps to'
                          JOIN CDMH_STAGING.CONCEPT C1
                                    ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
                                    AND C1.INVALID_REASON IS NULL
        )
        SELECT distinct 'DIAGNOSIS' as CDM_TBL, d.dx as src_code, d.dx_type src_code_type,
        case when d.dx_type = '10' then 'ICD10CM'  
             when d.dx_type = '9' then 'ICD9CM'  
             when d.dx_type = '11' then 'ICD11CM'
             when d.dx_type = 'SM' then 'SNOMED'
             else 'OT' end as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join native_pcorNet51_cdm.diagnosis d
        on source_code = d.dx
        where source_vocabulary_id in( 'ICD9CM', 'ICD10CM', 'ICD11CM', 'SNOMED') ---
        AND target_standard_concept = 'S'

    )x;
    count1:=sql%rowcount;
    COMMIT;
    
--    union all
    -- procedures
    INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
    select DISTINCT 'PROCEDURES' as cdm_tbl, y.src_code, y.src_code_type, src_vocab_code,  
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
    from
    (
        WITH CTE_VOCAB_MAP AS (
	       SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID, 
	                      c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, 
	                      c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON, 
	                      c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID, 
	                      c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
	       FROM CDMH_STAGING.CONCEPT C
	             JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
	                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
	                        AND CR.invalid_reason IS NULL
	                        AND lower(cr.relationship_id) = 'maps to'
	              JOIN CDMH_STAGING.CONCEPT C1
	                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
	                        AND C1.INVALID_REASON IS NULL

            )
        SELECT distinct 'PROCEDURES' as CDM_TBL, p.px as src_code, p.px_type src_code_type, -- CH cpt4 or hcpcs, OT, 10
         case when p.px_type = '10' then 'ICD10PCS'   
             when p.px_type = 'CH' then 'CPT4-HCPCS' -- or HCPCS 
             when p.px_type = 'OT' then 'OTHER'
              when p.px_type = 'NI' then 'NI'
             else 'OT' end as src_vocab_code,
             source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join native_pcorNet51_cdm.PROCEDURES p
        on source_code = p.px   
        where source_vocabulary_id in( 'ICD10CM', 'CPT4', 'HCPCS', 'ICD10PCS', 'SNOMED', 'RxNorm', 'NDC' ) ---DIAGNOSIS and PROCEDURES 35,441
        AND target_standard_concept = 'S' 
    ) y;
     count2:=sql%rowcount;
    COMMIT;
--    union all
    INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
    select DISTINCT 'CONDITION' as cdm_tbl, c.src_code, c.src_code_type, src_vocab_code,
    source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
    from
    (
        WITH CTE_VOCAB_MAP AS (
	       SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID, 
	                      c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, 
	                      c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON, 
	                      c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID, 
	                      c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
	       FROM CDMH_STAGING.CONCEPT C
	             JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
	                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
	                        AND CR.invalid_reason IS NULL
	                        AND lower(cr.relationship_id) = 'maps to'
	              JOIN CDMH_STAGING.CONCEPT C1
	                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
	                        AND C1.INVALID_REASON IS NULL

            )
        SELECT distinct 'CONDITION' as CDM_TBL, c.CONDITION as src_code, c.condition_type as src_code_type, 
        case when c.condition_type = '10' then 'ICD10CM'   
             when c.condition_type = '09' then 'ICD9CM' -- or HCPCS 
             when c.condition_type = 'AG' then 'AG'
             when c.condition_type = 'OT' then 'OTHER'
             else 'OT' end as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_PCORNET51_CDM.condition c
        on source_code = c.condition
        where source_vocabulary_id in( 'ICD9CM', 'ICD10CM') --- CONDITION.--50, types 9/10/ot/ag
        AND target_standard_concept = 'S' 
    ) c;
     count3:=sql%rowcount;
    COMMIT;
--    union all
    --death_cause
    INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
    select DISTINCT 'DEATH_CAUSE' as cdm_tbl, dt.src_code, dt.src_code_type, src_vocab_code,
    source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
    from
    (
        WITH CTE_VOCAB_MAP AS (
	       SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID, 
	                      c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, 
	                      c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON, 
	                      c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID, 
	                      c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
	       FROM CDMH_STAGING.CONCEPT C
	             JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
	                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
	                        AND CR.invalid_reason IS NULL
	                        AND lower(cr.relationship_id) = 'maps to'
	              JOIN CDMH_STAGING.CONCEPT C1
	                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
	                        AND C1.INVALID_REASON IS NULL

            )
        SELECT distinct 'DEATH_CAUSE' as CDM_TBL, dt.death_cause_code as src_code, dt.death_cause_type as src_code_type, 
        case when dt.death_cause_code = '10' then 'ICD10CM'   
             when dt.death_cause_code = '09' then 'ICD9CM'
             when dt.death_cause_code = 'OT' then 'OTHER'
             else 'OT' end as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        right outer join NATIVE_PCORNET51_CDM.death_cause dt
        on source_code = dt.death_cause
        where source_vocabulary_id in( 'ICD9CM', 'ICD10CM', 'SNOMED') --50,672/ death cause +2 9 or 10
        AND target_standard_concept = 'S' 
    ) dt;
     count4:=sql%rowcount;
    COMMIT;
--    union all
    --LAB_RESULTS_CM
    INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
   select DISTINCT 'LAB_RESULT_CM' as cdm_tbl, lab.src_code, lab.src_code_type, src_vocab_code,
    source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
    from
    (
        WITH CTE_VOCAB_MAP AS (
	       SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID, 
	                      c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, 
	                      c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON, 
	                      c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID, 
	                      c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
	       FROM CDMH_STAGING.CONCEPT C
	             JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
	                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
	                        AND CR.invalid_reason IS NULL
	                        AND lower(cr.relationship_id) = 'maps to'
	              JOIN CDMH_STAGING.CONCEPT C1
	                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
	                        AND C1.INVALID_REASON IS NULL

            )
        SELECT distinct 'LAB_RESULT_CM' as CDM_TBL, l.lab_loinc as src_code, 'LOINC' as src_code_type, 
        -- lab_px_type is all set to null, assuming LOINC
        --case when l.lab_px_type = '10' then 'ICD10CM'   
        --     when l.lab_px_type = '09' then 'ICD9CM'
        --     when l.lab_px_type = 'LC' then 'LOINC'
        --     when l.lab_px_type = 'OT' then 'OTHER'
        --     else 'OT' end as src_vocab_code,
        'LOINC' as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_PCORNET51_CDM.lab_result_cm l
        on source_code = l.lab_loinc
        where source_vocabulary_id in( 'LOINC') --50,672/ w/ 52,212 labs
        AND target_standard_concept = 'S' 
    ) lab;
     count5:=sql%rowcount;
    COMMIT;
    
    --dispensing - no data from UNC
--    union all
INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
     select DISTINCT 'DISPENSING' as cdm_tbl, dp.src_code, dp.src_code_type, src_vocab_code, 
     source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
    from
    (
        WITH CTE_VOCAB_MAP AS (
	       SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID, 
	                      c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, 
	                      c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON, 
	                      c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID, 
	                      c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
	       FROM CDMH_STAGING.CONCEPT C
	             JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
	                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
	                        AND CR.invalid_reason IS NULL
	                        AND lower(cr.relationship_id) = 'maps to'
	              JOIN CDMH_STAGING.CONCEPT C1
	                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
	                        AND C1.INVALID_REASON IS NULL

            )
        SELECT distinct 'DISPENSING' as CDM_TBL, dx.ndc as src_code, 'NDC' as src_code_type, 'NDC' as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_PCORNET51_CDM.dispensing dx
        on source_code=dx.ndc
        where source_vocabulary_id in( 'NDC') --50,672/ w/ 52,212 labs/ no dispensing record 
        AND target_standard_concept = 'S' 
    ) dp;
     count6:=sql%rowcount;
    COMMIT;
    --med_admin
--     union all
INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
     select DISTINCT 'MED_ADMIN' as cdm_tbl, ma.src_code, ma.src_code_type, src_vocab_code,
     source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id    
    from
    (
        WITH CTE_VOCAB_MAP AS (
	       SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID, 
	                      c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, 
	                      c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON, 
	                      c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID, 
	                      c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
	       FROM CDMH_STAGING.CONCEPT C
	             JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
	                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
	                        AND CR.invalid_reason IS NULL
	                        AND lower(cr.relationship_id) = 'maps to'
	              JOIN CDMH_STAGING.CONCEPT C1
	                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
	                        AND C1.INVALID_REASON IS NULL

            )
        SELECT distinct 'MED_ADMIN' as CDM_TBL, m.MEDADMIN_CODE  as src_code, m.MEDADMIN_TYPE as src_code_type, 
        case when m.medadmin_type  = 'ND' then 'NDC'   
            when m.medadmin_type = 'RX' then 'RxNorm'
            else 'NI'end as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_PCORNET51_CDM.MED_ADMIN m
        on source_code =  m.MEDADMIN_CODE
        where source_vocabulary_id in( 'RxNorm','NDC') --50,672/ w/ 52,212 labs/ no dispensing record /64,923 med_admin
        AND target_standard_concept = 'S' 
    ) ma;
     count7:=sql%rowcount;
    COMMIT;
    --prescribing
--    union all
INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
     select DISTINCT 'PRESCRIBING' as cdm_tbl, pr.src_code, pr.src_code_type, src_vocab_code, 
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
    from
    (
        WITH CTE_VOCAB_MAP AS (
	       SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID, 
	                      c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, 
	                      c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON, 
	                      c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID, 
	                      c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
	       FROM CDMH_STAGING.CONCEPT C
	             JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
	                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
	                        AND CR.invalid_reason IS NULL
	                        AND lower(cr.relationship_id) = 'maps to'
	              JOIN CDMH_STAGING.CONCEPT C1
	                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
	                        AND C1.INVALID_REASON IS NULL

            )
        SELECT distinct 'PRESCRIBING' as CDM_TBL, p.rxnorm_cui  as src_code, 'rxnorm_cui' as src_code_type, 'RxNorm' as src_vocab_code, 
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
         join NATIVE_PCORNET51_CDM.PRESCRIBING p
        on source_code= p.rxnorm_cui
        where source_vocabulary_id in( 'RxNorm','RxNorm Extension') --50,672/ w/ 52,212 labs/ no dispensing record /64,923 med_admin/70473 prescribing
        AND target_standard_concept = 'S' 
    ) pr;
     count8:=sql%rowcount;
    COMMIT;

    --obsclin_code
--    union all
INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
     select DISTINCT 'OBS_CLIN' as cdm_tbl, ob.src_code, ob.src_code_type, src_vocab_code,
     source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id
    from
    (
        WITH CTE_VOCAB_MAP AS (
           SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, c.concept_name AS SOURCE_CODE_DESCRIPTION, c.vocabulary_id AS SOURCE_VOCABULARY_ID, 
                          c.domain_id AS SOURCE_DOMAIN_ID, c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, 
                          c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON, 
                          c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME, c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID, c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID, 
                          c1.INVALID_REASON AS TARGET_INVALID_REASON, c1.standard_concept AS TARGET_STANDARD_CONCEPT
           FROM CDMH_STAGING.CONCEPT C
                 JOIN CDMH_STAGING.CONCEPT_RELATIONSHIP CR
                            ON C.CONCEPT_ID = CR.CONCEPT_ID_1
                            AND CR.invalid_reason IS NULL
                            AND lower(cr.relationship_id) = 'maps to'
                  JOIN CDMH_STAGING.CONCEPT C1
                            ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
                            AND C1.INVALID_REASON IS NULL

            )
        SELECT distinct 'OBS_CLIN' as CDM_TBL, o.obsclin_code src_code, o.obsclin_type as src_code_type, 
        case when o.obsclin_type = 'LC' then 'LOINC'   
             when o.obsclin_type = 'SM' then 'SNOMED'
            else 'OT' end as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
         join NATIVE_PCORNET51_CDM.OBS_CLIN o
        on source_code= o.obsclin_code
        where source_vocabulary_id in( 'LOINC', 'SNOMED') --50,672/ w/ 52,212 labs/ no dispensing record /64,923 med_admin/70473 prescribing
        AND target_standard_concept = 'S' 
    ) ob ;
     count9:=sql%rowcount;
    COMMIT;
    --select distinct obsclin_type, obsclin_code from NATIVE_PCORNET51_CDM.OBS_CLIN o ;-- obsgen_code = 3000 or 2000
    --PC_COVID	3000
    --PC_COVID	2000
    --No map exist - apply special rule, shong 6/12/20
   
    RECORDCOUNT  :=count1+count2+count3+count4+count5+count6+count7+count8+count9 ;
    
  DBMS_OUTPUT.put_line(RECORDCOUNT || ' PCORnet p2o_code_xwalk_standard table built successfully.'); 
--
    commit ;
    
END ;
