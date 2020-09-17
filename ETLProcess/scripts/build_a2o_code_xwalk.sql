--------------------------------------------------------
--  File created - Thursday-September-17-2020   
--------------------------------------------------------
-- Unable to render PROCEDURE DDL for object CDMH_STAGING.BUILD_A2O_CODE_XWALK with DBMS_METADATA attempting internal generator.
CREATE PROCEDURE                           CDMH_STAGING.BUILD_A2O_CODE_XWALK 
(
  RECORDCOUNT out number 
) as 

/*************************************************************************************************************************************
    FileName:   build_a2o_code_xwalk
    Purpose:     BUILD CODES FOUND IN THE observation_fact table and build the a2o_code_xwalk_standard table with OMOP target concept ids
    Author: Stephanie Hong
    Edit History:
     Ver        Date        Author        Description
     0.1        7/17/20    SHONG          Initial version.
     0.2        9/14/20     shong         Most recent update. 
    Terminology found in the ACT: 
    --RXNORM:
    --CPT4:
    --UMLS:
    --LOINC:
    --ICD10CM:
    --HCPCS:
    0.3         9/16/20 SHONG Filter out local concept_cd with prefixes like 'DIST|%' or like 'VISIT|%'  --54221
************************************************************************************************************************************/

begin
-- truncate before building - A2o_code_xwalk_standard
execute immediate 'truncate table CDMH_STAGING.A2o_code_xwalk_standard';
commit ;


INSERT INTO CDMH_STAGING.a2o_code_xwalk_standard ( CDM_TBL, src_code, src_code_type, src_vocab_code, 
    source_code, source_code_concept_id, source_code_description,
    source_vocabulary_id, source_domain_id, target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id)     
    select DISTINCT 'OBSERVATION_FACT' as cdm_tbl, x.src_code as src_code, x.src_code_type as src_code_type, x.src_vocab_code,
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
        SELECT distinct 'OBSERVATION_FACT' as CDM_TBL, substr(concept_cd, instr(concept_cd, ':')+1, length(concept_cd)) as src_code, 
            substr(concept_cd, 0,instr(concept_cd, ':')-1) src_code_type,
            /*
            case when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'RXNORM' then 'RXNORM'  
             when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'CPT4' then 'CPT4'  
             when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'LOINC' then 'LOINC'
             when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'ICD10CM' then 'ICD10CM'
             when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'HCPCS' then 'HCPCS'
             when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'NDC' then 'NDC'
              when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'SNOMED' then 'SNOMED'
              when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'NUI' then 'NUI'
              when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'ICD9PROC' then 'ICD9PROC'
              when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'ICD10PCS' then 'ICD10PCS' 
              when substr(concept_cd, 0,instr(concept_cd, ':')-1) = 'ICD9CM' then 'ICD9CM'
             --when substr(concept_cd, 0,instr(concept_cd, ':')) is null then 'n/a'
             else 'OT' end as src_vocab_code, 
             ssh: not needed 
             */
             substr(concept_cd, 0,instr(concept_cd, ':')-1) as src_vocab_code,
        source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id,
        target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
        FROM cte_vocab_map
        join native_i2b2act_cdm.observation_fact f
        on source_code = substr(concept_cd, instr(concept_cd, ':')+1, length(concept_cd))
        where source_vocabulary_id in( 'RXNORM', 'CPT4', 'LOINC', 'ICD10CM', 'HCPCS', 'NDC', 'SNOMED', 'NUI', 'ICD10PCS','ICD9PROC', 'ICD9CM') ---ssh 7/17/20 uk only had RXNORM/CPT4/UMLS/LOINC/ICD10CM/HCPCS in the fact table
        AND target_standard_concept = 'S' 
        and concept_cd not like 'DEM|%' and concept_cd not like 'VISIT|%' and concept_cd not like 'DIST|%' 

    )x
; ---22,916
RECORDCOUNT  := sql%rowcount;

  DBMS_OUTPUT.put_line(RECORDCOUNT || ' i2b2ACT a2o_code_xwalk_standard table built successfully.'); 
--
    commit ;

end build_a2o_code_xwalk;
