CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` AS

WITH site_codes_to_source_concept_mapping AS (
    SELECT DISTINCT
        'DIAGNOSIS' as CDM_TBL, 
        d.dx as src_code, 
        d.dx_type as src_code_type,
        d.mapped_dx_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/diagnosis` d
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = d.dx
            AND upper(c.vocabulary_id) = upper(d.mapped_dx_type)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'PROCEDURES' as CDM_TBL, 
        p.px as src_code, 
        p.px_type as src_code_type,
        p.mapped_px_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/procedures` p
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = p.px  
            AND (upper(c.vocabulary_id) = upper(p.mapped_px_type) 
                OR 
                -- Look for either CPT4 or HCPCS codes when px_type is 'CH'
                (p.px_type = 'CH' AND c.vocabulary_id in ('CPT4', 'HCPCS')))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'CONDITION' as CDM_TBL, 
        cd.condition as src_code, 
        cd.condition_type as src_code_type, 
        cd.mapped_condition_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/condition` cd
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = cd.condition
            AND upper(c.vocabulary_id) = upper(cd.mapped_condition_type)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'DEATH_CAUSE' as CDM_TBL,
        dt.death_cause as src_code,
        dt.death_cause_code as src_code_type, 
        dt.mapped_death_cause_code as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/death_cause`dt
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = dt.death_cause
            AND upper(c.vocabulary_id) = upper(dt.mapped_death_cause_code)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'LAB_RESULT_CM' as CDM_TBL,
        l.lab_loinc as src_code,
        'LOINC' as src_code_type,
        'LOINC' as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/lab_result_cm` l
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            -- Only looking up records using the lab_loinc field, so filter to LOINC vocab
            ON c.concept_code = l.lab_loinc
            AND c.vocabulary_id = 'LOINC'
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'DISPENSING' as CDM_TBL,
        dx.ndc as src_code,
        'NDC' as src_code_type,
        'NDC' as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/dispensing` dx
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            -- Only looking up records using the ndc field, so filter to NDC vocab
            ON c.concept_code=dx.ndc
            AND c.vocabulary_id = 'NDC'
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'MED_ADMIN' as CDM_TBL,
        m.medadmin_code as src_code,
        m.medadmin_type as src_code_type, 
        m.mapped_medadmin_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/med_admin` m
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code =  m.medadmin_code
            AND upper(c.vocabulary_id) = upper(m.mapped_medadmin_type)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'PRESCRIBING' as CDM_TBL,
        p.rxnorm_cui as src_code,
        'rxnorm_cui' as src_code_type,
        'RxNorm' as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/prescribing` p
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = p.rxnorm_cui
            -- Only looking up records using the rxnorm_cui field, so filter to RxNorm and RxNorm Extension vocabs
            -- (There's no overlap in concept_codes between the RxNorm and RxNorm Extension vocabs, so no risk of multi-mapping)
            AND c.vocabulary_id in ('RxNorm', 'RxNorm Extension')
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'OBS_CLIN' as CDM_TBL,
        o.obsclin_code as src_code,
        o.obsclin_type as src_code_type, 
        o.mapped_obsclin_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_clin` o
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = o.obsclin_code
            AND upper(c.vocabulary_id) = upper(o.mapped_obsclin_type)
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    SELECT DISTINCT 
        'IMMUNIZATION' as CDM_TBL, 
        i.vx_code as src_code, 
        i.vx_code_type as src_code_type,
        i.mapped_vx_code_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/immunization` i
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = i.vx_code
            -- Look for either CPT4 or HCPCS codes when vx_code_type is 'CH'
            AND ((upper(c.vocabulary_id) = upper(i.mapped_vx_code_type)) 
                OR 
                (i.vx_code_type = 'CH' AND c.vocabulary_id in ('CPT4' 'HCPCS')))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
    UNION ALL
    -- there are LOINC codes coming in via the OBS_GEN
    select 
        'OBS_GEN' as CDM_TBL
        ,ob.obsgen_code as src_code
        ,ob.obsgen_type as src_code_type
        ,'LOINC' as src_vocab_code -- 'LC' types are LOINC
        ---i.mapped_vx_code_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` ob
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = ob.obsgen_code AND (upper(c.vocabulary_id) = upper('LOINC'))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
        where ob.obsgen_type = 'LC'
    UNION ALL 
    -- if there are SNOMED CT codes coming in via the OBS_GEN
    select 
        'OBS_GEN' as CDM_TBL
        ,ob.obsgen_code as src_code
        ,ob.obsgen_type as src_code_type
        ,'SNOMED' as src_vocab_code -- 'SM' types are SNOMED
        ---i.mapped_vx_code_type as src_vocab_code
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` ob
        LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c
            ON c.concept_code = ob.obsgen_code AND (upper(c.vocabulary_id) = upper('SNOMED'))
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
        where ob.obsgen_type = 'SM'             

),

target_concept_mapping AS (
    SELECT DISTINCT
          site_codes_to_source_concept_mapping.*
        , COALESCE(c2.concept_id, 0)     AS target_concept_id
        , COALESCE(
            c2.concept_name,
            'No matching concept')       AS target_concept_name
        , c2.vocabulary_id               AS target_vocabulary_id
        , COALESCE(
            c2.domain_id,
            'Observation')               AS target_domain_id
        , c2.concept_class_id            AS target_concept_class_id
        , c2.valid_start_date            AS target_valid_start_date
        , c2.valid_end_date              AS target_valid_end_date
        , c2.invalid_reason              AS target_invalid_reason
    FROM site_codes_to_source_concept_mapping 
    LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept_relationship` cr 
        ON site_codes_to_source_concept_mapping.source_concept_id = cr.concept_id_1
        AND cr.invalid_reason IS NULL 
        AND lower(cr.relationship_id) = 'maps to'
    LEFT JOIN `/N3C Export Area/OMOP Vocabularies/concept` c2
        ON cr.concept_id_2 = c2.concept_id
        AND c2.invalid_reason IS NULL -- invalid records will map to concept_id = 0
)

SELECT DISTINCT * FROM target_concept_mapping
