/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong
Stored Procedure : BUILD_T2O_CODE_XWALK

Description : insert to N3CDS_DOMAIN_MAP

****/
  CREATE OR REPLACE EDITIONABLE PROCEDURE "cdmh_Staging"."BUILD_T2O_CODE_XWALK" (
    recordcount OUT NUMBER
) AS
    diagcnt   NUMBER;
    proccnt   NUMBER;
    medscnt   NUMBER;
    labscnt   NUMBER;

begin
-- truncate before building - A2o_code_xwalk_standard
    EXECUTE IMMEDIATE 'truncate table cdmh_Staging.t2o_code_xwalk_standard';
    COMMIT;

/**********************************************************************************************************************************************
Description: BUILD dynamic code map with CODES FOUND IN THE following list of TRINetX tables. 
    Build the t2o_code_xwalk_standard cross walk table for terms in diagnosis, procedure, medication, labs_result with OMOP target concept ids.
Project : N3C
Procedure: BUILD_T2O_CODE_XWALK
Author: Stephanie Hong
Diagnosis--ICD10CM/ ICD9CM
Procedures--ICD10CM/ ICD9CM/ HCPCS/ CPT4
Labresult--LOINC:
Medication--RXNORM:

TNX type
EditDate Initial Modification History:
8/14/2020 SHONG Initial Version.

*******************************************************************************************************************************************/
    INSERT INTO cdmh_Staging.t2o_code_xwalk_standard (
        cdm_tbl,
        src_code,
        src_code_type,
        src_vocab_code,
        source_code,
        source_code_concept_id,
        source_code_description,
        source_vocabulary_id,
        source_domain_id,
        target_concept_id,
        target_concept_name,
        target_vocabulary_id,
        target_domain_id,
        target_concept_class_id
    )
        SELECT DISTINCT
            'DIAGNOSIS' AS cdm_tbl,
            x.src_code        AS src_code,
            x.src_code_type   AS src_code_type,
            x.src_vocab_code,
            source_code,
            source_concept_id,
            source_code_description,
            source_vocabulary_id,
            source_domain_id,
            target_concept_id,
            target_concept_name,
            target_vocabulary_id,
            target_domain_id,
            target_concept_class_id
        FROM
            (
                WITH cte_vocab_map AS (
                    SELECT
                        c.concept_code        AS source_code,
                        c.concept_id          AS source_concept_id,
                        c.concept_name        AS source_code_description,
                        c.vocabulary_id       AS source_vocabulary_id,
                        c.domain_id           AS source_domain_id,
                        c.concept_class_id    AS source_concept_class_id,
                        c.valid_start_date    AS source_valid_start_date,
                        c.valid_end_date      AS source_valid_end_date,
                        c.invalid_reason      AS source_invalid_reason,
                        c1.concept_id         AS target_concept_id,
                        c1.concept_name       AS target_concept_name,
                        c1.vocabulary_id      AS target_vocabulary_id,
                        c1.domain_id          AS target_domain_id,
                        c1.concept_class_id   AS target_concept_class_id,
                        c1.invalid_reason     AS target_invalid_reason,
                        c1.standard_concept   AS target_standard_concept
                    FROM
                        cdmh_staging.concept                c
                        JOIN cdmh_staging.concept_relationship   cr ON c.concept_id = cr.concept_id_1
                                                                     AND cr.invalid_reason IS NULL
                                                                     AND lower(cr.relationship_id) = 'maps to'
                        JOIN cdmh_staging.concept                c1 ON cr.concept_id_2 = c1.concept_id
                                                        AND c1.invalid_reason IS NULL
                )
                SELECT DISTINCT
                    'DIAGNOSIS' AS cdm_tbl,
                    d.mapped_code        AS src_code,
                    d.dx_code_system     src_code_type,
                    mapped_code_system   AS src_vocab_code,
                    source_code,
                    source_concept_id,
                    source_code_description,
                    source_vocabulary_id,
                    source_domain_id,
                    target_concept_id,
                    target_concept_name,
                    target_vocabulary_id,
                    target_domain_id,
                    target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
                FROM
                    cte_vocab_map
                    JOIN native_trinetx_cdm.diagnosis d ON source_code = d.mapped_code
                WHERE
                    source_vocabulary_id IN ( 'ICD9CM', 'ICD10CM','ICD10PCS' )--- UMLS:ICD9CM:[icd9_code]	/ UMLS:ICD10CM:[icd10cm_code]	
                    AND target_standard_concept = 'S'
            ) x;

    diagcnt := SQL%rowcount;
    COMMIT;
--    union all 

    -- procedures
    INSERT INTO cdmh_Staging.t2o_code_xwalk_standard (
        cdm_tbl,
        src_code,
        src_code_type,
        src_vocab_code,
        source_code,
        source_code_concept_id,
        source_code_description,
        source_vocabulary_id,
        source_domain_id,
        target_concept_id,
        target_concept_name,
        target_vocabulary_id,
        target_domain_id,
        target_concept_class_id
    )
        SELECT DISTINCT
            'PROCEDURE' AS cdm_tbl,
            y.src_code,
            y.src_code_type,
            src_vocab_code,
            source_code,
            source_concept_id,
            source_code_description,
            source_vocabulary_id,
            source_domain_id,
            target_concept_id,
            target_concept_name,
            target_vocabulary_id,
            target_domain_id,
            target_concept_class_id
        FROM
            (
                WITH cte_vocab_map AS (
                    SELECT
                        c.concept_code        AS source_code,
                        c.concept_id          AS source_concept_id,
                        c.concept_name        AS source_code_description,
                        c.vocabulary_id       AS source_vocabulary_id,
                        c.domain_id           AS source_domain_id,
                        c.concept_class_id    AS source_concept_class_id,
                        c.valid_start_date    AS source_valid_start_date,
                        c.valid_end_date      AS source_valid_end_date,
                        c.invalid_reason      AS source_invalid_reason,
                        c1.concept_id         AS target_concept_id,
                        c1.concept_name       AS target_concept_name,
                        c1.vocabulary_id      AS target_vocabulary_id,
                        c1.domain_id          AS target_domain_id,
                        c1.concept_class_id   AS target_concept_class_id,
                        c1.invalid_reason     AS target_invalid_reason,
                        c1.standard_concept   AS target_standard_concept
                    FROM
                        cdmh_staging.concept                c
                        JOIN cdmh_staging.concept_relationship   cr ON c.concept_id = cr.concept_id_1
                                                                     AND cr.invalid_reason IS NULL
                                                                     AND lower(cr.relationship_id) = 'maps to'
                        JOIN cdmh_staging.concept                c1 ON cr.concept_id_2 = c1.concept_id
                                                        AND c1.invalid_reason IS NULL
                )
                SELECT DISTINCT
                    'PROCEDURE' AS cdm_tbl,
                    p.mapped_code        AS src_code,
                    p.px_code_system     AS src_code_type, -- CH cpt4 or hcpcs, OT, 10
                    mapped_code_system   AS src_vocab_code,
                    source_code,
                    source_concept_id,
                    source_code_description,
                    source_vocabulary_id,
                    source_domain_id,
                    target_concept_id,
                    target_concept_name,
                    target_vocabulary_id,
                    target_domain_id,
                    target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
                FROM
                    cte_vocab_map
                    JOIN native_trinetx_cdm.procedure p ON source_code = p.mapped_code
                WHERE
                    source_vocabulary_id IN ( 'ICD10PCS', 'CPT4', 'HCPCS', 'RxNorm' ) 
                    AND target_standard_concept = 'S'
            ) y;

    proccnt := SQL%rowcount;
    COMMIT;

--    union all
    --LAB_RESULTS
    INSERT INTO cdmh_Staging.t2o_code_xwalk_standard (
    cdm_tbl,
    src_code,
    src_code_type,
    src_vocab_code,
    source_code,
    source_code_concept_id,
    source_code_description,
    source_vocabulary_id,
    source_domain_id,
    target_concept_id,
    target_concept_name,
    target_vocabulary_id,
    target_domain_id,
    target_concept_class_id) 
    SELECT DISTINCT
    'LAB_RESULT' AS cdm_tbl,
    lab.src_code,
    lab.src_code_type,
    src_vocab_code,
    source_code,
    source_concept_id,
    source_code_description,
    source_vocabulary_id,
    source_domain_id,
    target_concept_id,
    target_concept_name,
    target_vocabulary_id,
    target_domain_id,
    target_concept_class_id
    FROM
        ( WITH cte_vocab_map AS (
        SELECT
            c.concept_code        AS source_code,
            c.concept_id          AS source_concept_id,
            c.concept_name        AS source_code_description,
            c.vocabulary_id       AS source_vocabulary_id,
            c.domain_id           AS source_domain_id,
            c.concept_class_id    AS source_concept_class_id,
            c.valid_start_date    AS source_valid_start_date,
            c.valid_end_date      AS source_valid_end_date,
            c.invalid_reason      AS source_invalid_reason,
            c1.concept_id         AS target_concept_id,
            c1.concept_name       AS target_concept_name,
            c1.vocabulary_id      AS target_vocabulary_id,
            c1.domain_id          AS target_domain_id,
            c1.concept_class_id   AS target_concept_class_id,
            c1.invalid_reason     AS target_invalid_reason,
            c1.standard_concept   AS target_standard_concept
        FROM
            cdmh_staging.concept                c
            JOIN cdmh_staging.concept_relationship   cr ON c.concept_id = cr.concept_id_1
                                                         AND cr.invalid_reason IS NULL
                                                         AND lower(cr.relationship_id) = 'maps to'
            JOIN cdmh_staging.concept                c1 ON cr.concept_id_2 = c1.concept_id
                                            AND c1.invalid_reason IS NULL
    )
    SELECT DISTINCT
        'LAB_RESULT' AS cdm_tbl,
        l.mapped_code AS src_code,
        l.lab_code_system      AS src_code_type,
        mapped_code_system   AS src_vocab_code,
        source_code,
        source_concept_id,
        source_code_description,
        source_vocabulary_id,
        source_domain_id,
        target_concept_id,
        target_concept_name,
        target_vocabulary_id,
        target_domain_id,
        target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
    FROM
        cte_vocab_map
        JOIN native_trinetx_cdm.lab_result l ON source_code = l.mapped_code
    WHERE
        source_vocabulary_id IN ( 'LOINC' ) --LNC: LOINC codes
        AND target_standard_concept = 'S' 
    
        ) lab;
         labsCnt:=sql%rowcount;
        COMMIT;
        
        --union all 
        --medication
        
    INSERT INTO cdmh_Staging.t2o_code_xwalk_standard ( 
        CDM_TBL, src_code, src_code_type, src_vocab_code, 
        source_code, source_code_concept_id, source_code_description,
        source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)  
        select DISTINCT 
        'MEDICATION' as cdm_tbl, ma.src_code, ma.src_code_type, src_vocab_code,
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
        SELECT distinct 'MEDICATION' as CDM_TBL, m.MAPPED_CODE  as src_code, m.RX_CODE_SYSTEM as src_code_type, 
        MAPPED_CODE_SYSTEM as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join NATIVE_TRINETX_CDM.MEDICATION m
        on source_code =  m.MAPPED_CODE
        where source_vocabulary_id in( 'RxNorm','NDC') --
    ) ma;
    
    medsCnt:=sql%rowcount;
    COMMIT;
    
    recordcount := diagcnt + proccnt + labsCnt+ medscnt ;
    dbms_output.put_line(recordcount || ' t2o_code_xwalk_standard table built successfully.');
    COMMIT;
    
    --53446
END build_t2o_code_xwalk;

/
