CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/sdoh_answer_code_xwalk` AS
    -- TriNetX sites are sending in SDoH concepts via the lab_result table with question codes in the the lab_code column 
    -- and the answer codes in the text_result_val column 
    -- We will need to collect all possible LOINC coded SDoH answer codes the sites are sending with the current payload 
    -- and generate the corresponding concept_ids to set in the value_as_concept_id column in Observation domain.
    -- Here we are generating the site's current payload's SDoH answer code xwalk lookup table by grabing all the 
    -- LOINC coded answer codes in the TEXT_RESULT_VAL column values like 'LA%'
    -- Stephanie Hong, 5/4/2022
    -- This data enhancement reference doc is here: 
    -- https://github.com/National-COVID-Cohort-Collaborative/Phenotype_Data_Acquisition/wiki/New-Data-Element:-Social-Determinants-of-Health#tri
    -- accomodate different flavors or answer codes in the text_result_val - i.e. site 578
    -- if starts with LOINC: then it is LOINC answer codes i.e LOINC:LA30189-7 (I have a steady place to live)
    -- if LOINC read starting from : and read upto one digit after - for LOINC the answer codes
    -- if all numeric it will be snomed ct codes i.e. 410519009 = "at risk" can be used as an answer code
    -- shong, updated 5/31/2022
    -- read upto one digit after - (CHARINDEX('-', text_result_val) + 2)
    -- TODO:   
    -- if SNOMEDCT is used as the SDoH answer concept then we need a better way to capture that answer as it would be text type 
    -- but numeric answer in SNOMEDCT not sure if the answer will be passed in as text_result_val or numeric_result_val
    -- if they are numeric then they are SNOMEDCT codes
    with mapped_answer_code as (
        SELECT DISTINCT 
        l.text_result_val as answer_code,
        CASE 
            WHEN locate('LOINC:', l.text_result_val) > 0  then substr(l.text_result_val, 7, (locate('-', l.text_result_val, 1) + 2)-7)
            ELSE l.text_result_val
        END AS mapped_answer_code, 
        'LOINC' as src_vocab_code
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/lab_result` l
        WHERE l.lab_code_system = 'LOINC' AND l.result_type = 'T' AND l.text_result_val like '%LA%-%'
        --OR l.lab_code in ( '76513-1', '88122-7', '76511-5', '45404-1', '76542-0', '96779-4', '88123-5', '63503-7', '93033-9', '93030-5') 
    )

    SELECT DISTINCT
    l.answer_code,
    l.mapped_answer_code, 
    l.src_vocab_code,
    c.concept_code, 
    c.concept_class_id,
    c.vocabulary_id,
    COALESCE(c.concept_id, 0) as target_concept_id
    FROM mapped_answer_code l
    LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
             ON trim(c.concept_code) = trim(mapped_answer_code) --match on LOINC coded SDoH answer code
             AND upper(c.vocabulary_id) = 'LOINC'
             AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
             AND c.concept_class_id = 'Answer'                
  