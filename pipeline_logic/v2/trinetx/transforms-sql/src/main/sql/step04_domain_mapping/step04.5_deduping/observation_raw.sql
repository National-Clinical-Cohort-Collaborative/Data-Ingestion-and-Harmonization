CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/04.5 - deduping/observation_raw` AS 

WITH pat AS (
    SELECT
        patient_id AS site_patient_id,
        -- From value lookup table: "Need to create a record for observation.concept_id = 3018063 and then populate the observation.value_as_concept_id"
        3018063 as observation_concept_id,
        CAST(null as date) AS observation_date,
        CAST(null as timestamp) AS observation_datetime, 
        0 AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(id_mapping_marital_status.TARGET_CONCEPT_ID as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        CAST(null as string) AS site_encounter_id,
        CAST(null as long) AS visit_detail_id,
        CAST(COALESCE(mapped_marital_status, marital_status) as string) AS observation_source_value,
        3018063 AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'PATIENT' as domain_source,
        -- Mapped columns to be used for joins on concept table 
        CAST(null as string) AS mapped_code_system,
        CAST(data_partner_id as int) as data_partner_id,
		payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/patient` patient
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` id_mapping_marital_status
        ON id_mapping_marital_status.CDM_TBL = 'patient'
        AND id_mapping_marital_status.CDM_TBL_COLUMN_NAME = 'mapped_marital_status'
        AND patient.mapped_marital_status = id_mapping_marital_status.SRC_CODE
),

diag_lab_proc_vital AS (
    SELECT 
        patient_id AS site_patient_id,
        CAST(diagnosis.date as date) AS observation_date,
        CAST(diagnosis.date as timestamp) AS observation_datetime, 
        0 AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as long) AS provider_id,
        diagnosis.encounter_id AS site_encounter_id,
        CAST(null as long) AS visit_detail_id,
        CAST(COALESCE(diagnosis.mapped_code, diagnosis.dx_code) as string) AS observation_source_value,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'DIAGNOSIS' as domain_source,
        -- Mapped columns to be used for joins on concept table 
        CAST(null as string) AS coalesced_text_result_val,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(data_partner_id as int) as data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/diagnosis` diagnosis

    UNION ALL

    SELECT
        patient_id AS site_patient_id,
        CAST(lab_result.test_date as date) AS observation_date,
        CAST(lab_result.test_date as timestamp) AS observation_datetime, 
        0 AS observation_type_concept_id,
        CAST(lab_result.numeric_result_val as float) AS value_as_number,
        '|text_result_val:' || COALESCE(lab_result.text_result_val, '') || '|mapped_text_result_val:' || COALESCE(lab_result.mapped_text_result_val, '') AS value_as_string,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as long) AS provider_id,
        lab_result.encounter_id AS site_encounter_id,
        CAST(null as long) AS visit_detail_id,
        CAST(COALESCE(lab_result.mapped_code, lab_result.lab_code) as string) as observation_source_value,
        CAST(lab_result.units_of_measure as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'LAB_RESULT' as domain_source,
        -- Mapped columns to be used for joins on concept table 
        COALESCE(lab_result.mapped_text_result_val, lab_result.text_result_val) AS coalesced_text_result_val,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(data_partner_id as int) as data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/lab_result` lab_result

    UNION ALL

    SELECT
        patient_id AS site_patient_id,
        CAST(procedure.date as date) AS observation_date,
        CAST(procedure.date as timestamp) AS observation_datetime, 
        0 AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as long) AS provider_id,
        procedure.encounter_id AS site_encounter_id,
        CAST(null as long) AS visit_detail_id,
        CAST(COALESCE(procedure.mapped_code, procedure.px_code) as string) as observation_source_value,
        CAST(null as STRING) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'PROCEDURE' as domain_source,
        -- Mapped columns to be used for joins on concept table 
        CAST(null as string) AS coalesced_text_result_val,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(data_partner_id as int) as data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/procedure` procedure 

    UNION ALL

    SELECT
        patient_id AS site_patient_id,
        CAST(vital_signs.measure_date as date) AS observation_date,
        CAST(null as timestamp) AS observation_datetime, 
        0 AS observation_type_concept_id,
        CAST(vital_signs.numeric_result_val as float) AS value_as_number,
        '|text_result_val:' || COALESCE(vital_signs.text_result_val, '') || '|mapped_text_result_val:' || COALESCE(vital_signs.mapped_text_result_val, '') AS value_as_string,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as long) AS provider_id,
        vital_signs.encounter_id AS site_encounter_id,
        CAST(null as long) AS visit_detail_id,
        CAST(COALESCE(vital_signs.mapped_code, vital_signs.vital_code) as string) as observation_source_value,
        CAST(vital_signs.unit_of_measure as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'VITAL_SIGNS' as domain_source,
        -- Mapped columns to be used for joins on concept table 
        COALESCE(vital_signs.mapped_text_result_val, vital_signs.text_result_val) AS coalesced_text_result_val,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(data_partner_id as int) as data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/vital_signs` vital_signs 
),

crosswalk_lookup AS (
    SELECT
      diag_lab_proc_vital.*
    , crosswalk.source_concept_id AS observation_source_concept_id
    , crosswalk.target_concept_id AS observation_concept_id
    FROM diag_lab_proc_vital
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/t2o_code_xwalk_standard` crosswalk
        ON upper(diag_lab_proc_vital.mapped_code_system) = upper(crosswalk.src_vocab_code)
        AND diag_lab_proc_vital.observation_source_value = crosswalk.source_code
        AND crosswalk.target_domain_id = 'Observation'
),

-- Get value_as_concept_ids for records with values in the valueset mapping 
value_as_concept_id_mapped AS (
    SELECT 
        crosswalk_lookup.*
      , CAST(COALESCE(valueset_mapping.TARGET_CONCEPT_ID, ansxw.target_concept_id, 0) as int) as value_as_concept_id
    FROM crosswalk_lookup
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` valueset_mapping
        ON valueset_mapping.CDM_TBL_COLUMN_NAME = 'text_result_val'
        AND upper(crosswalk_lookup.coalesced_text_result_val) = upper(valueset_mapping.SRC_CODE)
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/sdoh_answer_code_xwalk` ansxw
        ON upper(trim(crosswalk_lookup.coalesced_text_result_val)) = upper(trim(ansxw.answer_code))
        AND ( crosswalk_lookup.coalesced_text_result_val like 'LA%' OR  crosswalk_lookup.coalesced_text_result_val like 'LOINC:LA%' )
),

-- Get unit_concept_ids for records with values in the valueset mapping 
unit_concept_id_mapped AS (
    SELECT 
        value_as_concept_id_mapped.*
      , CAST(valueset_mapping.TARGET_CONCEPT_ID as int) as unit_concept_id
    FROM value_as_concept_id_mapped
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` valueset_mapping
        ON valueset_mapping.CDM_TBL_COLUMN_NAME = 'units_of_measure'
        AND value_as_concept_id_mapped.unit_source_value = valueset_mapping.SRC_CODE
),
---Grab the long covid specialty clinic visit data from encounter, TriNetX is submitting this data via encounter table.
-- if the is_long_covid flag is set to "t" then added to the observation domain 
long_covid as (
    SELECT 
    patient_id as site_patient_id
    -- 2004207791 is N3C defined custom concept id defined for the long covid specialty clinic visit
    , 2004207791 AS observation_concept_id
    , enc.start_date as observation_date
    , CAST(null as timestamp) AS observation_datetime
    , 32817 AS observation_type_concept_id
    , CAST(null as float) AS value_as_number
    , CAST(null as string) AS value_as_string
    , CAST(null as int) AS value_as_concept_id
    , CAST(null as int) AS qualifier_concept_id
    , CAST(null as int) AS unit_concept_id
    , CAST(null as int) AS provider_id
    , encounter_id AS site_encounter_id
    , CAST(null as long) AS visit_detail_id
    , 'N3C:LongCOVIDSpecialtyVisit' AS observation_source_value
    , 2004207791 AS observation_source_concept_id
    , CAST(null as string) AS unit_source_value
    , CAST(null as string) AS qualifier_source_value
    , 'ENCOUNTER' as domain_source
    , CAST( data_partner_id as int) as data_partner_id
    , payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/encounter` enc
    --- if the is_long_covid is set to t, or true. 
    --- Note, this string type is transformed to boolean type prior to step4 =>add to observation as long covid specialty clinic visit data
    WHERE enc.is_long_covid is true
),

-- Grab records from medication
-- These will be records that probably don't belong in the Observation table but as part of exception handling have been mapped to a concept_id = 0
other_records AS (
    SELECT
      patient_id AS site_patient_id
    , CAST(xw.target_concept_id as int) AS observation_concept_id
    , start_date AS observation_date
    , CAST(null as timestamp) AS observation_datetime
    , CAST(null as int) AS observation_type_concept_id
    , CAST(null as float) AS value_as_number
    , CAST(null as string) AS value_as_string
    , CAST(null as int) AS value_as_concept_id
    , CAST(null as int) AS qualifier_concept_id
    , CAST(null as int) AS unit_concept_id
    , CAST(null as int) AS provider_id
    , encounter_id AS site_encounter_id
    , CAST(null as long) AS visit_detail_id
    , 'site RX_CODE_SYSTEM:' || rx_code_system
        || '|mapped code system:' || PREPARED_COALESCED_MAPPED_CODE_SYSTEM 
        || '|RX_CODE:' || rx_code
        AS observation_source_value
    , CAST(xw.source_concept_id as int) AS observation_source_concept_id
    , CAST(null as string) AS unit_source_value
    , CAST(null as string) AS qualifier_source_value
    , 'MEDICATION' as domain_source
    , data_partner_id
    , payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/medication` med
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/t2o_code_xwalk_standard` xw
        ON upper(med.PREPARED_COALESCED_MAPPED_CODE_SYSTEM) = upper(xw.src_vocab_code)
        AND med.COALESCED_MAPPED_CODE = xw.src_code
        AND xw.target_domain_id = 'Observation'
),



final_table AS (
-- Finding only unique measurements, per discussion with Matvey Palchuk + Kristin Kosta 13-08-2020
    SELECT DISTINCT
          *
    FROM (
        SELECT 
              site_patient_id
            , observation_concept_id
            , observation_date
            , observation_datetime
            , observation_type_concept_id
            , value_as_number
            , value_as_string
            , value_as_concept_id
            , qualifier_concept_id
            , unit_concept_id
            , provider_id
            , site_encounter_id
            , visit_detail_id
            , observation_source_value
            , observation_source_concept_id
            , unit_source_value
            , qualifier_source_value
            , domain_source
            , data_partner_id
            , payload
        FROM pat
        UNION 
        SELECT 
              site_patient_id
            , observation_concept_id
            , observation_date
            , observation_datetime
            , observation_type_concept_id
            , value_as_number
            , value_as_string
            , value_as_concept_id
            , qualifier_concept_id
            , unit_concept_id
            , provider_id
            , site_encounter_id
            , visit_detail_id
            , observation_source_value
            , observation_source_concept_id
            , unit_source_value
            , qualifier_source_value
            , domain_source
            , data_partner_id
            , payload
        FROM unit_concept_id_mapped
        WHERE observation_concept_id IS NOT NULL
        UNION
        SELECT * FROM other_records
        UNION 
        SELECT * FROM long_covid
    )
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    -- In case of collisions, this will be joined on a lookup table in the next step
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_id
    , observation_concept_id
    , observation_date
    , observation_datetime
    , observation_type_concept_id
    , value_as_number
    , value_as_string
    , value_as_concept_id
    , qualifier_concept_id
    , unit_concept_id
    , provider_id
    , site_encounter_id
    , visit_detail_id
    , observation_source_value
    , observation_source_concept_id
    , unit_source_value
    , qualifier_source_value
    , domain_source
    , data_partner_id
    , payload
FROM (
    SELECT
          *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patient_id, ' ')
			, COALESCE(observation_concept_id, ' ')
			, COALESCE(observation_date, ' ')
			, COALESCE(observation_datetime, ' ')
			, COALESCE(observation_type_concept_id, ' ')
			, COALESCE(value_as_number, ' ')
			, COALESCE(value_as_string, ' ')
			, COALESCE(value_as_concept_id, ' ')
			, COALESCE(qualifier_concept_id, ' ')
			, COALESCE(unit_concept_id, ' ')
			, COALESCE(provider_id, ' ')
			, COALESCE(site_encounter_id, ' ')
			, COALESCE(visit_detail_id, ' ')
			, COALESCE(observation_source_value, ' ')
			, COALESCE(observation_source_concept_id, ' ')
			, COALESCE(unit_source_value, ' ')
			, COALESCE(qualifier_source_value, ' ')
        )) as hashed_id
    FROM final_table
)
