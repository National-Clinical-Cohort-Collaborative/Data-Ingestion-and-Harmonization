-- View: V_P2O_DISPENSING_CODE_XWALK
-- Project: N3C DI&H
-- CDM source PCORnet 
-- DISPENSING code xwalk view - DISPENSING code to omop concept ids
-- Stephanie Hong, Clair Blacketer
-- June 1, 2020


  CREATE OR REPLACE FORCE EDITIONABLE VIEW "CDMH_STAGING"."V_P2O_DISPENSING_CODE_XWALK" 
  ("CDM_TBL", "SRC_CODE", "SRC_CODE_TYPE", "SOURCE_CODE", "SOURCE_CONCEPT_ID", "SOURCE_CODE_DESCRIPTION", "SOURCE_VOCABULARY_ID", "SOURCE_DOMAIN_ID", "TARGET_CONCEPT_ID", "TARGET_CONCEPT_NAME", "TARGET_VOCABULARY_ID", "TARGET_DOMAIN_ID", "TARGET_CONCEPT_CLASS_ID") AS 
  select 'DISPENSING' as cdm_tbl, src_code, src_code_type,  
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
	       FROM CONCEPT C
	             JOIN CONCEPT_RELATIONSHIP CR
	                        ON C.CONCEPT_ID = CR.CONCEPT_ID_1
	                        AND CR.invalid_reason IS NULL
	                        AND lower(cr.relationship_id) = 'maps to'
	              JOIN CONCEPT C1
	                        ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
	                        AND C1.INVALID_REASON IS NULL
	      
		)
		SELECT distinct 'DISPENSING' as CDM_TBL, d.ndc as src_code, 'NDC' as src_code_type, 
		source_code, source_concept_id, source_code_description, source_vocabulary_id, source_domain_id, 
		target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id, target_concept_class_id ---target_concept_id = omop concept id , target_concept_name = concept name target_domain_id = condition
		FROM cte_vocab_map
		right outer join native_pcorNet51_cdm.DISPENSING d
		on source_code in (d.ndc ) -- i.e. d.dx code-- in('U07.1')  
		where source_vocabulary_id in( 'NDC', 'RxNorm') 
		AND target_standard_concept = 'S' 
) x;
