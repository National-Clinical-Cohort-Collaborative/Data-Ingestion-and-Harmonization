CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/vent_code_xwalk` AS
     -- PCORNet sites are sending in vent concept codes via the obs_gen in the obsgen_code column
     -- When obsgent_type is set to SM then all the codes in the obsgen_code should have the corresponding concept_id values
     -- These target_concept_id will be use to set the device_concept_id for DEVICE domain. 
     -- 5/9/2022 - Stephanie Hong, please refer to the data design reference document link is here:  
     -- https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/wiki/New-Data-Element:-O2-Device-(Tier-1)
    SELECT DISTINCT
    'OBS_GEN' AS CDM_TBL,
    obs.obsgen_code as src_code,
    c.concept_code, 
    CASE WHEN obs.obsgen_type = 'SM'  THEN 'SNOMED' 
        ELSE 'UNKNOWN'
        END as src_vocab_code,
    c.vocabulary_id,
    c.domain_id,
    COALESCE( c.concept_id, 0) as target_concept_id -- if no mapping is found set it to 0
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` obs
    LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON trim(c.concept_code) = trim(obs.obsgen_code) 
            AND upper(c.vocabulary_id) = 'SNOMED'
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    WHERE obs.obsgen_type = 'SM' and obs.obsgen_code is not null