CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/04 - domain mapping/drug_exposure` AS
    
-- Use valueset mapping to look up drug_type_concept_id from RX_SOURCE field
WITH medication AS (
    SELECT 
        PATIENT_ID AS site_patient_id,
        CAST(START_DATE AS date) AS drug_exposure_start_date,
        CAST(null AS timestamp) AS drug_exposure_start_datetime,
        CAST(END_DATE AS date) AS drug_exposure_end_date,
        CAST(null AS timestamp) AS drug_exposure_end_datetime,
        CAST(null AS date) AS verbatim_end_date,
        CAST(COALESCE(rx_source_mapping.TARGET_CONCEPT_ID, 0) AS int) AS drug_type_concept_id,
        CAST(null AS string) AS stop_reason,
        CAST(REFILLS AS int) AS refills,
        CAST(QTY_DISPENSED AS float) AS quantity,
        CAST(DURATION AS int) AS days_supply,
        CAST(null AS string) AS sig,
        CAST(null AS string) AS lot_number,
        CAST(null AS int) AS provider_id,
        ENCOUNTER_ID AS site_encounter_id,
        CAST(null AS int) AS visit_detail_id,
        CAST(COALESCE(MAPPED_CODE, RX_CODE) AS string) AS drug_source_value,
        CAST(ROUTE_OF_ADMINISTRATION AS string) AS route_source_value,
        CAST(null AS string) AS dose_unit_source_value,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(DATA_PARTNER_ID as int) as data_partner_id,
		payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/medication` m
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` rx_source_mapping
        ON rx_source_mapping.CDM_TBL = 'medication'
        AND rx_source_mapping.CDM_TBL_COLUMN_NAME = 'rx_source'
        AND m.RX_SOURCE = rx_source_mapping.SRC_CODE
),

diag_med_proc AS (
    SELECT 
        PATIENT_ID AS site_patient_id,
        CAST(DATE AS date) AS drug_exposure_start_date,
        CAST(null AS timestamp) AS drug_exposure_start_datetime,
        CAST(null AS date) AS drug_exposure_end_date,
        CAST(null AS timestamp) AS drug_exposure_end_datetime,
        CAST(null AS date) AS verbatim_end_date,
        0 AS drug_type_concept_id,
        CAST(null AS string) AS stop_reason,
        CAST(null AS int) AS refills,
        CAST(null AS float) AS quantity,
        CAST(null AS int) AS days_supply,
        CAST(null AS string) AS sig,
        CAST(null AS string) AS lot_number,
        CAST(null AS int) AS provider_id,
        ENCOUNTER_ID AS site_encounter_id,
        CAST(null AS int) AS visit_detail_id,
        CAST(COALESCE(MAPPED_CODE, DX_CODE) AS string) AS drug_source_value,
        CAST(null AS string) AS route_source_value,
        CAST(null AS string) AS dose_unit_source_value,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(DATA_PARTNER_ID as int) as data_partner_id,
		payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/diagnosis`

    UNION ALL

    SELECT * FROM medication

    UNION ALL

    SELECT 
        PATIENT_ID AS site_patient_id,
        CAST(DATE AS date) AS drug_exposure_start_date,
        CAST(null AS timestamp) AS drug_exposure_start_datetime,
        CAST(null AS date) AS drug_exposure_end_date,
        CAST(null AS timestamp) AS drug_exposure_end_datetime,
        CAST(null AS date) AS verbatim_end_date,
        0 AS drug_type_concept_id,
        CAST(null AS string) AS stop_reason,
        CAST(null AS int) AS refills,
        CAST(null AS float) AS quantity,
        CAST(null AS int) AS days_supply,
        CAST(null AS string) AS sig,
        CAST(null AS string) AS lot_number,
        CAST(null AS int) AS provider_id,
        ENCOUNTER_ID AS site_encounter_id,
        CAST(null AS int) AS visit_detail_id,
        CAST(COALESCE(MAPPED_CODE, PX_CODE) AS string) AS drug_source_value,
        CAST(null AS string) AS route_source_value,
        CAST(null AS string) AS dose_unit_source_value,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(DATA_PARTNER_ID as int) as data_partner_id,
		payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/procedure`
), 

crosswalk_lookup AS (
    SELECT
      diag_med_proc.*
    , crosswalk.source_concept_id AS drug_source_concept_id
    , crosswalk.target_concept_id AS drug_concept_id
    FROM diag_med_proc
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/t2o_code_xwalk_standard` crosswalk
        ON upper(diag_med_proc.mapped_code_system) = upper(crosswalk.src_vocab_code)
        AND diag_med_proc.drug_source_value = crosswalk.source_code
        AND crosswalk.target_domain_id = 'Drug'
),

route_lookup AS (
    -- 10/2/20 -- Remove joins on string fields in concept table 
    SELECT
          crosswalk_lookup.*
          , CAST(COALESCE(route_xw.TARGET_CONCEPT_ID, 0) as int) as route_concept_id
    FROM crosswalk_lookup
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` route_xw
        ON route_xw.CDM_TBL = 'medication'
        AND route_xw.CDM_TBL_COLUMN_NAME = 'route_of_administration'
        AND crosswalk_lookup.route_source_value = route_xw.SRC_CODE
),

final_table AS (
    SELECT
          *
        -- Required for identical rows so that their IDs differ when hashing
        , row_number() OVER (
            PARTITION BY
              site_patient_id
            , drug_concept_id
            , drug_exposure_start_date
            , drug_exposure_start_datetime
            , drug_exposure_end_date
            , drug_exposure_end_datetime
            , verbatim_end_date
            , drug_type_concept_id
            , stop_reason
            , refills
            , quantity
            , days_supply
            , sig
            , route_concept_id
            , lot_number
            , provider_id
            , site_encounter_id
            , visit_detail_id
            , drug_source_value
            , drug_source_concept_id
            , route_source_value
            , dose_unit_source_value   
            ORDER BY site_patient_id        
        ) as row_index
    FROM route_lookup
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_id
    , drug_concept_id
    , drug_exposure_start_date
    , drug_exposure_start_datetime
    , drug_exposure_end_date
    , drug_exposure_end_datetime
    , verbatim_end_date
    , drug_type_concept_id
    , stop_reason
    , refills
    , quantity
    , days_supply
    , sig
    , route_concept_id
    , lot_number
    , provider_id
    , site_encounter_id
    , visit_detail_id
    , drug_source_value
    , drug_source_concept_id
    , route_source_value
    , dose_unit_source_value
    , data_partner_id
	, payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patient_id, ' ')
			, COALESCE(drug_concept_id, ' ')
			, COALESCE(drug_exposure_start_date, ' ')
			, COALESCE(drug_exposure_start_datetime, ' ')
			, COALESCE(drug_exposure_end_date, ' ')
			, COALESCE(drug_exposure_end_datetime, ' ')
			, COALESCE(verbatim_end_date, ' ')
			, COALESCE(drug_type_concept_id, ' ')
			, COALESCE(stop_reason, ' ')
			, COALESCE(refills, ' ')
			, COALESCE(quantity, ' ')
			, COALESCE(days_supply, ' ')
			, COALESCE(sig, ' ')
			, COALESCE(route_concept_id, ' ')
			, COALESCE(lot_number, ' ')
			, COALESCE(provider_id, ' ')
			, COALESCE(site_encounter_id, ' ')
			, COALESCE(visit_detail_id, ' ')
			, COALESCE(drug_source_value, ' ')
			, COALESCE(drug_source_concept_id, ' ')
			, COALESCE(route_source_value, ' ')
			, COALESCE(dose_unit_source_value, ' ')
			, COALESCE(row_index, ' ')
        )) as hashed_id
        FROM final_table
    )
