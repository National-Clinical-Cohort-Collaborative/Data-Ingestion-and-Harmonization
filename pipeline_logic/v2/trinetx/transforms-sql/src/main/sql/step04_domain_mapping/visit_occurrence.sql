CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/visit_occurrence` AS

/*
Handle "duplicate" TriNetX records that correspond to the same encounter, but whose source encounter
types mapped to two different mapped types and created two rows. Concatenate the visit types and use 
the valueset mapping table to map these records to a single OMOP concept_id where applicable.
*/
with de_duplication as (
    SELECT 
        patient_id,
        encounter_id,
        encounter_type,
        start_date,
        end_date,
        length_of_stay,
        orphan_flag,
        data_partner_id,
        payload,
        SORT_ARRAY(COLLECT_LIST(encounter.mapped_encounter_type)) as mapped_et_list,
        SORT_ARRAY(COLLECT_LIST(encounter.COALESCED_MAPPED_ENCOUNTER_TYPE)) as coalesced_mapped_et_list
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/encounter` encounter
    GROUP BY 
        patient_id,
        encounter_id,
        encounter_type,
        start_date,
        end_date,
        length_of_stay,
        orphan_flag,
        data_partner_id,
        payload
),

combined as (
    SELECT 
        * 
        , CASE
            WHEN SIZE(mapped_et_list) == 0 THEN CAST(null as string)
            ELSE concat_ws(' ', mapped_et_list)
        END as MAPPED_ENCOUNTER_TYPE
        , CASE
            WHEN SIZE(coalesced_mapped_et_list) == 0 THEN CAST(null as string)
            ELSE concat_ws(' ', coalesced_mapped_et_list)
        END as COALESCED_MAPPED_ENCOUNTER_TYPE
    FROM de_duplication
),

-- Lookup concept_ids for records with values in the valueset mapping 
visit_id_mapped as (
    SELECT 
        encounter_id AS site_encounter_id,
        patient_id AS site_patient_id,
        CAST(COALESCE(enc_type_xw.TARGET_CONCEPT_ID, 0) AS int) AS visit_concept_id,
        CAST(start_date AS date) AS visit_start_date,
        CAST(null AS timestamp) AS visit_start_datetime,
        CAST(end_date AS date) AS visit_end_date,
        CAST(null AS timestamp) AS visit_end_datetime,
        -- "For visit_type_concept _id -- SET default value = 32035 for  'Visit derived from EHR encounter record' "
        CAST(32035 AS int) AS visit_type_concept_id,
        CAST(null AS long) AS provider_id,
        CAST(null AS long) AS care_site_id,
        CAST(COALESCED_MAPPED_ENCOUNTER_TYPE AS string) AS visit_source_value,
        CAST(null AS int) AS visit_source_concept_id,
        CAST(null AS int) AS admitting_source_concept_id,
        CAST(null AS string) AS admitting_source_value,
        CAST(null AS int) AS discharge_to_concept_id,
        CAST(null AS string) AS discharge_to_source_value,
        CAST(null AS long) AS preceding_visit_occurrence_id,
        CAST(data_partner_id as int) as data_partner_id,
        payload
    FROM combined
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` enc_type_xw
        ON enc_type_xw.CDM_TBL = 'encounter'
        AND enc_type_xw.CDM_TBL_COLUMN_NAME = 'mapped_encounter_type'
        AND combined.COALESCED_MAPPED_ENCOUNTER_TYPE = enc_type_xw.SRC_CODE
        -- Based on conversation with SMEs, drop records that have no date information 
        WHERE start_date IS NOT NULL OR end_date IS NOT NULL
)

SELECT
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    , cast(base_10_hash_value as bigint) & 2251799813685247 as visit_occurrence_id_51_bit
    FROM (
        SELECT
          *
        , conv(sub_hash_value, 16, 10) as base_10_hash_value
        FROM (
            SELECT
              *
            , substr(hashed_id, 1, 15) as sub_hash_value
            FROM (
                SELECT
                  *
                -- Create primary key by hashing patient id to 128bit hexademical with md5,
                -- and converting to 51 bit int by first taking first 15 hexademical digits and converting
                --  to base 10 (60 bit) and then bit masking to extract the first 51 bits
                , md5(site_encounter_id) as hashed_id
                FROM visit_id_mapped
            )
        )
    )
