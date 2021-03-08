CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/04 - domain mapping/device_exposure` AS

WITH diag_med_proc AS (
    SELECT 
        diagnosis.PATIENT_ID AS site_patient_id,
        CAST(diagnosis.DATE AS date) AS device_exposure_start_date,
        CAST(null AS timestamp) AS device_exposure_start_datetime,
        CAST(null AS date) AS device_exposure_end_date,
        CAST(null AS timestamp) AS device_exposure_end_datetime,
        0 AS device_type_concept_id,
        CAST(null AS string) AS unique_device_id,
        CAST(null AS int) AS quantity,
        CAST(null AS int) AS provider_id,
        diagnosis.ENCOUNTER_ID AS site_encounter_id,
        CAST(null AS int) AS visit_detail_id,
        CAST(COALESCE(MAPPED_CODE, DX_CODE) AS string) AS device_source_value,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(DATA_PARTNER_ID as int) as data_partner_id, 
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/diagnosis` diagnosis

    UNION ALL

    SELECT 
        medication.PATIENT_ID AS site_patient_id,
        CAST(START_DATE AS date) AS device_exposure_start_date,
        CAST(null AS timestamp) AS device_exposure_start_datetime,
        CAST(END_DATE AS date) AS device_exposure_end_date,
        CAST(null AS timestamp) AS device_exposure_end_datetime,
        0 AS device_type_concept_id,
        CAST(null AS string) AS unique_device_id,
        CAST(null AS int) AS quantity,
        CAST(null AS int) AS provider_id,
        medication.ENCOUNTER_ID AS site_encounter_id,
        CAST(null AS int) AS visit_detail_id,
        CAST(COALESCE(MAPPED_CODE, RX_CODE) AS string) AS device_source_value,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(DATA_PARTNER_ID as int) as data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/medication` medication

    UNION ALL

    SELECT
        procedure.PATIENT_ID AS site_patient_id,
        CAST(DATE AS date) AS device_exposure_start_date,
        CAST(null AS timestamp) AS device_exposure_start_datetime,
        CAST(null AS date) AS device_exposure_end_date,
        CAST(null AS timestamp) AS device_exposure_end_datetime,
        0 AS device_type_concept_id,
        CAST(null AS string) AS unique_device_id,
        CAST(null AS int) AS quantity,
        CAST(null AS int) AS provider_id,
        procedure.ENCOUNTER_ID AS site_encounter_id,
        CAST(null AS int) AS visit_detail_id,
        CAST(COALESCE(procedure.MAPPED_CODE, procedure.PX_CODE) AS string) AS device_source_value,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(DATA_PARTNER_ID as int) as data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/procedure` procedure
),

crosswalk_lookup AS (
    SELECT
      diag_med_proc.*
    , CAST(crosswalk.source_concept_id as int) AS device_source_concept_id
    , CAST(crosswalk.target_concept_id as int) AS device_concept_id
    FROM diag_med_proc
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/t2o_code_xwalk_standard` crosswalk
        ON upper(diag_med_proc.mapped_code_system) = upper(crosswalk.src_vocab_code)
        AND diag_med_proc.device_source_value = crosswalk.source_code
        AND crosswalk.target_domain_id = 'Device'
),

final_table AS (
    SELECT
          *
        -- Required for identical rows so that their IDs differ when hashing
        , row_number() OVER (
            PARTITION BY
              site_patient_id
            , device_concept_id
            , device_exposure_start_date
            , device_exposure_start_datetime
            , device_exposure_end_date
            , device_exposure_end_datetime
            , device_type_concept_id
            , unique_device_id
            , quantity
            , provider_id
            , site_encounter_id
            , visit_detail_id
            , device_source_value
            , device_source_concept_id
            ORDER BY site_patient_id
        ) as row_index
    FROM crosswalk_lookup
    WHERE device_concept_id IS NOT NULL
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as device_exposure_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_id
    , device_concept_id
    , device_exposure_start_date
    , device_exposure_start_datetime
    , device_exposure_end_date
    , device_exposure_end_datetime
    , device_type_concept_id
    , unique_device_id
    , quantity
    , provider_id
    , site_encounter_id
    , visit_detail_id
    , device_source_value
    , device_source_concept_id
    , data_partner_id
	, payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patient_id, ' ')
			, COALESCE(device_concept_id, ' ')
			, COALESCE(device_exposure_start_date, ' ')
			, COALESCE(device_exposure_start_datetime, ' ')
			, COALESCE(device_exposure_end_date, ' ')
			, COALESCE(device_exposure_end_datetime, ' ')
			, COALESCE(device_type_concept_id, ' ')
			, COALESCE(unique_device_id, ' ')
			, COALESCE(quantity, ' ')
			, COALESCE(provider_id, ' ')
			, COALESCE(site_encounter_id, ' ')
			, COALESCE(visit_detail_id, ' ')
			, COALESCE(device_source_value, ' ')
			, COALESCE(device_source_concept_id, ' ')
			, COALESCE(row_index, ' ')
        )) as hashed_id
        FROM final_table
    )
