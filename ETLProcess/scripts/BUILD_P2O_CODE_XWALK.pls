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
---Description: stored procedure to create the code map PCORnet terminology to OMOP concept ids
---Stephanie Hong 6/9/2020
---
CREATE OR REPLACE PROCEDURE BUILD_P2O_CODE_XWALK 
AS
BEGIN
    execute immediate 'truncate table CDMH_STAGING.p2o_code_xwalk_standard';  
    
    INSERT INTO CDMH_STAGING.p2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)     
    select 'DIAGNOSIS' as cdm_tbl, x.src_code as src_code, x.src_code_type as src_code_type,
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
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join native_pcorNet51_cdm.diagnosis d
        on source_code = d.dx
        where source_vocabulary_id in( 'ICD9CM', 'ICD10CM', 'ICD11CM', 'SNOMED') ---9/10/11/SNOMED 
        AND target_standard_concept = 'S'
        and lower(target_domain_id) NOT LIKE 'spec anatomic site%'

    )x
    
     union all
    -- procedures
    select 'PROCEDURES' as cdm_tbl, y.src_code, y.src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
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
        SELECT distinct 'PROCEDURES' as CDM_TBL, p.px as src_code, p.px_type src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join native_pcorNet51_cdm.PROCEDURES p
        on source_code = p.px   
        where source_vocabulary_id in( 'ICD9CM', 'ICD9Proc', 'ICD10CM', 'ICD10PCS', 'OPCS4','CPT4', 'ICD11CM', 'LOINC', 'HCPCS', 'NDC') ---PROCEDURES/ can be meds
        AND target_standard_concept = 'S' 
    ) y
    union all
    select 'CONDITION' as cdm_tbl, c.src_code, c.src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
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
        SELECT distinct 'CONDITION' as CDM_TBL, c.CONDITION as src_code, c.condition_type as src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_PCORNET51_CDM.condition c
        on source_code = c.condition
        where source_vocabulary_id in( 'ICD9CM', 'ICD10CM', 'ICD11CM', 'SNOMED', 'PCORNet') --- CONDITION.--50,670
        AND target_standard_concept = 'S' 
    ) c
    union all
    select 'DEATH_CAUSE' as cdm_tbl, dth.src_code, dth.src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
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
        SELECT distinct 'DEATH_CAUSE' as CDM_TBL, dt.death_cause as src_code, dt.death_cause_type as src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        right outer join NATIVE_PCORNET51_CDM.death_cause dt
        on source_code = dt.death_cause
        where source_vocabulary_id in( 'ICD9CM', 'ICD10CM') --50,672/ death cause +2
        AND target_standard_concept = 'S' 
    ) dth
    union all
    --LAB_RESULTS_CM
    select 'LAB_RESULT_CM' as cdm_tbl, lab.src_code, lab.src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
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
        SELECT distinct 'LAB_RESULT_CM' as CDM_TBL, l.lab_loinc as src_code, 'LOINC' as src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_PCORNET51_CDM.lab_result_cm l
        on source_code = l.lab_loinc
        where source_vocabulary_id in( 'LOINC') --50,672/ w/ 52,212 labs
        AND target_standard_concept = 'S' 
    ) lab   
    --dispensing
    union all
     select 'DISPENSING' as cdm_tbl, dp.src_code, dp.src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
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
        SELECT distinct 'DISPENSING' as CDM_TBL, dx.ndc as src_code, 'NDC' as src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_PCORNET51_CDM.dispensing dx
        on source_code=dx.ndc
        where source_vocabulary_id in( 'NDC') --50,672/ w/ 52,212 labs/ no dispensing record 
        AND target_standard_concept = 'S' 
    ) dp
    --med_admin
     union all
     select 'MED_ADMIN' as cdm_tbl, ma.src_code, ma.src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
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
        SELECT distinct 'MED_ADMIN' as CDM_TBL, m.MEDADMIN_CODE  as src_code, m.MEDADMIN_TYPE as src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_PCORNET51_CDM.MED_ADMIN m
        on source_code =  m.MEDADMIN_CODE
        where source_vocabulary_id in( 'RxNorm','NDC') --50,672/ w/ 52,212 labs/ no dispensing record /64,923 med_admin
        AND target_standard_concept = 'S' 
    ) ma
    --prescribing
    union all
     select 'PRESCRIBING' as cdm_tbl, pr.src_code, pr.src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
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
        SELECT distinct 'PRESCRIBING' as CDM_TBL, p.rxnorm_cui  as src_code, 'rxnorm_cui' as src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
         join NATIVE_PCORNET51_CDM.PRESCRIBING p
        on source_code= p.rxnorm_cui
        where source_vocabulary_id in( 'RxNorm') --50,672/ w/ 52,212 labs/ no dispensing record /64,923 med_admin/70473 prescribing
        AND target_standard_concept = 'S' 
    ) pr

    --obsclin_code
    union all
     select 'OBS_CLIN' as cdm_tbl, ob.src_code, ob.src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
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
        SELECT distinct 'OBS_CLIN' as CDM_TBL, o.obsclin_code src_code, 'LOINC or other' as src_code_type, source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
         join NATIVE_PCORNET51_CDM.OBS_CLIN o
        on source_code= o.obsclin_code
        where source_vocabulary_id in( 'LOINC', 'SNOMED') --50,672/ w/ 52,212 labs/ no dispensing record /64,923 med_admin/70473 prescribing
        AND target_standard_concept = 'S' 
    ) ob
   ;


END ;