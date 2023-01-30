CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/sdoh_answer_code_xwalk` AS
    -- ACT sites are sending in SDoH concepts via the observation_fact table with question codes in the the concept_cd column 
    -- and the answer codes in the valueflag_cd column 
    -- We will need to collect all possible answer codes the sites are sending with the current payload and generate
    -- the corresponding concept_ids to set in the value_as_concept_id column in Observation domain.
    -- Here we are generating the site's current payload's SDoH answer code xwalk lookup table by grabing all the 
    -- answer codes in the valueflag_cd column values like 'LA%'
    -- Stephanie Hong, 5/4/2022
    -- This data enhancement reference doc is here: https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/wiki/New-Data-Element:-Social-Determinants-of-Health#act
    -- Note, valueflag_cd is prefixed with 'LOINC:' and concept_cd is prefixed with 'LOINC:'
    SELECT DISTINCT
    'OBSERVATION_FACT' AS CDM_TBL,
    trim(substring(of.valueflag_cd, 7, length(of.valueflag_cd)-6)) as answer_code,
    c.concept_code, 
    'LOINC' as src_vocab_code, --valueflag_cd like 'LA%' are LOINC answer codes with Meas Value domain id
    c.vocabulary_id,
    c.concept_id as target_concept_id
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` of
    LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
             ON trim(c.concept_code) = trim(substring(of.valueflag_cd, 7, length(of.valueflag_cd)-6)) --match on code only
             AND upper(c.vocabulary_id) = 'LOINC'
             AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    WHERE of.valueflag_cd like 'LOINC:%' AND concept_cd like 'LOINC:%' 
    ----LA% are SDoH LOINC coded answer codes
    -- if SNOMEDCT is used the the SDoH concept then it can be passed in as the SNOMED but currently no ACT site is sending SNOMEDCT answer codes