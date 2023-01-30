CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/vent_code_xwalk` AS
    --all snomed ct codes are mapped without the OMOP relationship table
    SELECT DISTINCT
    'PROCEDURE' AS CDM_TBL,
    px_code as src_code,
    px_description, 
    c.concept_code, 
    -- TODO: shong, if sending custom code, may need to update how we are mapping the custom code entries
    px_code_system as src_vocab_code,
    PREPARED_COALESCED_MAPPED_CODE_SYSTEM as mapped_vocabulary_id, 
    c.vocabulary_id as target_vocabulary_id,
    -- TODO: shong, if sending custom code, may need to update how we are mapping the custom code entries
    -- CASE WHEN UPPER(trim(of.concept_cd)) = 'N3C:ROOM_AIR' then 2004208005 
    --      WHEN UPPER(trim(of.concept_cd)) = 'N3C:OT_O2_DEVICE' then 2004208004 
    -- ELSE COALESCE( c.concept_id, 0) as target_concept_id -- if no mapping is found set it to 0
    c.domain_id as target_domain_id,
    c.concept_name,
    COALESCE( c.concept_id, 0) as target_concept_id
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/procedure` p
    LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON trim(c.concept_code) = trim(px_code)
            AND upper(c.vocabulary_id) = PREPARED_COALESCED_MAPPED_CODE_SYSTEM
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
            AND c.standard_concept = 'S'
            AND c.domain_id ='Device'
    WHERE p.px_code_system like 'N3C%' or px_code_system like 'SNOMED%'

    
    