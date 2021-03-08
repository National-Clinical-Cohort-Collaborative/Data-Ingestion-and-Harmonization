CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/04 - domain mapping/condition_occurrence` AS

-- Use valueset mapping to look up:
-- (1) condition_type_concept_id from DX_SOURCE field
-- (2) condition_status_concept_id from PRINCIPAL_INDICATOR field
-- condition_type_concept_id is a required OMOP field, so populate with 0 if lookup misses
WITH diagnosis AS (
    SELECT 
          PATIENT_ID as site_patient_id
        , cast(DATE as date) as condition_start_date
        , cast(null as timestamp) as condition_start_datetime
        , cast(null as date) as condition_end_date
        , cast(null as timestamp) as condition_end_datetime
        , cast(COALESCE(dx_source_mapping.TARGET_CONCEPT_ID, 0) as int) as condition_type_concept_id
        , cast(principal_indicator_mapping.TARGET_CONCEPT_ID as int) as condition_status_concept_id
        , cast(null as string) as stop_reason
        , cast(null as int) as provider_id
        , ENCOUNTER_ID as site_encounter_id
        , cast(null as int) as visit_detail_id
        , cast(COALESCE(MAPPED_CODE, DX_CODE) as string) as condition_source_value
        , PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system
        , cast(PRINCIPAL_INDICATOR as string) as condition_status_source_value
        , cast(DATA_PARTNER_ID as int) as data_partner_id
		, payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/diagnosis` d
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` dx_source_mapping
        ON dx_source_mapping.CDM_TBL = 'diagnosis'
        AND dx_source_mapping.CDM_TBL_COLUMN_NAME = 'dx_source'
        AND d.DX_SOURCE = dx_source_mapping.SRC_CODE
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` principal_indicator_mapping
        ON principal_indicator_mapping.CDM_TBL = 'diagnosis'
        AND principal_indicator_mapping.CDM_TBL_COLUMN_NAME = 'principal_indicator'
        AND d.PRINCIPAL_INDICATOR = principal_indicator_mapping.SRC_CODE
), 

diagnosis_procedure AS (
    SELECT * FROM diagnosis

    UNION ALL

    SELECT
          PATIENT_ID as site_patient_id
        , cast(DATE as date) as condition_start_date
        , cast(null as timestamp) as condition_start_datetime
        , cast(null as date) as condition_end_date
        , cast(null as timestamp) as condition_end_datetime
        , 0 as condition_type_concept_id
        , cast(null as int) as condition_status_concept_id
        , cast(null as string) as stop_reason
        , cast(null as int) as provider_id
        , ENCOUNTER_ID as site_encounter_id
        , cast(null as int) as visit_detail_id
        , cast(COALESCE(MAPPED_CODE, PX_CODE) as string) as condition_source_value
        , PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system
        , cast(null as string) as condition_status_source_value
        , cast(DATA_PARTNER_ID as int) as data_partner_id
		, payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/procedure`
), 

crosswalk_lookup AS (
    SELECT
      diagnosis_procedure.*
    , crosswalk.source_concept_id AS condition_source_concept_id
    , crosswalk.target_concept_id AS condition_concept_id
    FROM diagnosis_procedure
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/t2o_code_xwalk_standard` crosswalk
        ON upper(diagnosis_procedure.mapped_code_system) = upper(crosswalk.src_vocab_code)
        AND diagnosis_procedure.condition_source_value = crosswalk.source_code
        AND crosswalk.target_domain_id = 'Condition'
),

final_table AS (
-- Finding only unique measurements, per discussion with Matvey Palchuk + Kristin Kosta 13-08-2020
    SELECT DISTINCT
          *
    FROM crosswalk_lookup
    WHERE condition_concept_id IS NOT NULL
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_id
    , condition_concept_id
    , condition_start_date
    , condition_start_datetime
    , condition_end_date
    , condition_end_datetime
    , condition_type_concept_id
    , condition_status_concept_id
    , stop_reason
    , provider_id
    , site_encounter_id
    , visit_detail_id
    , condition_source_value
    , condition_status_source_value
    , condition_source_concept_id
    , data_partner_id
	, payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patient_id, ' ')
			, COALESCE(condition_concept_id, ' ')
			, COALESCE(condition_start_date, ' ')
			, COALESCE(condition_start_datetime, ' ')
			, COALESCE(condition_end_date, ' ')
			, COALESCE(condition_end_datetime, ' ')
			, COALESCE(condition_type_concept_id, ' ')
			, COALESCE(condition_status_concept_id, ' ')
			, COALESCE(stop_reason, ' ')
			, COALESCE(provider_id, ' ')
			, COALESCE(site_encounter_id, ' ')
			, COALESCE(visit_detail_id, ' ')
			, COALESCE(condition_source_value, ' ')
			, COALESCE(condition_status_source_value, ' ')
			, COALESCE(condition_source_concept_id, ' ')
        )) as hashed_id
        FROM final_table
    )
