CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/vent_code_xwalk` AS
    -- look up the concept_ids for the concept_code found in the observation_fact table
    SELECT DISTINCT
    'OBSERVATION_FACT' AS CDM_TBL,
    substring(trim(of.concept_cd), 8, LENGTH(trim(of.concept_cd) -8 )) as src_code,
    c.concept_code, 
    -- CASE WHEN of.concept_cd like 'N3C:%'  THEN 'N3C' 
    --     WHEN of.concept_cd like 'SNOMED:%' THEN 'SNOMEDCT'
    --     ELSE 'UNKNOWN'
    --     END as src_vocab_code,
    'SNOMEDCT' as src_vocab_code,
    c.vocabulary_id,
    -- CASE WHEN UPPER(trim(of.concept_cd)) = 'N3C:ROOM_AIR' then 2004208005 
    --      WHEN UPPER(trim(of.concept_cd)) = 'N3C:OT_O2_DEVICE' then 2004208004 
    -- ELSE COALESCE( c.concept_id, 0) as target_concept_id -- if no mapping is found set it to 0
    COALESCE( c.concept_id, 0) as target_concept_id
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` of
    LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON trim(c.concept_code) = substring(trim(of.concept_cd), 8, LENGTH(trim(concept_cd) -8 ))
            AND upper(c.vocabulary_id) = 'SNOMEDCT'
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    WHERE of.concept_cd like 'SNOMED:%' 
    --or of.concept_cd = 'N3C:ROOM_AIR' OR of.concept_cd = 'N3C:OT_O2_DEVICE'
    