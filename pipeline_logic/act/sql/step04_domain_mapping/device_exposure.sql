CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/device_exposure` AS

with obs_fact as (
    SELECT 
        patient_num as site_patient_num,
        CAST(xw.target_concept_id as int) AS device_concept_id,
        CAST(ob.start_date AS date) AS device_exposure_start_date,
        CAST(ob.start_date AS timestamp) AS device_exposure_start_datetime,
        CAST(ob.end_date AS date) AS device_exposure_end_date,
        CAST(ob.end_date AS timestamp) AS device_exposure_end_datetime,
        32817 AS device_type_concept_id,
        CAST(null AS string) AS unique_device_id,
        CAST(null AS int) AS quantity,
        CAST(null AS int) AS provider_id,
        ob.encounter_num AS site_encounter_num,
        CAST(null AS int) AS visit_detail_id,
        CAST(concept_cd as string) AS device_source_value,
        CAST(xw.source_concept_id as int) AS device_source_concept_id,
        'OBSERVATION_FACT' AS domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` ob
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/a2o_code_xwalk` xw 
            ON xw.src_code_type || ':' || xw.src_code = ob.concept_cd
            AND xw.cdm_tbl = 'OBSERVATION_FACT'
            AND xw.target_domain_id = 'Device'
),

final_table AS (
    SELECT
          *
        -- Required for identical rows so that their IDs differ when hashing
        , row_number() OVER (
            PARTITION BY
              site_patient_num
            , device_concept_id
            , device_exposure_start_date
            , device_exposure_start_datetime
            , device_exposure_end_date
            , device_exposure_end_datetime
            , device_type_concept_id
            , unique_device_id
            , quantity
            , provider_id
            , site_encounter_num
            , visit_detail_id
            , device_source_value
            , device_source_concept_id
            ORDER BY site_patient_num
        ) as row_index
    FROM obs_fact
    WHERE device_concept_id IS NOT NULL
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as device_exposure_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_num
    , device_concept_id
    , device_exposure_start_date
    , device_exposure_start_datetime
    , device_exposure_end_date
    , device_exposure_end_datetime
    , device_type_concept_id
    , unique_device_id
    , quantity
    , provider_id
    , site_encounter_num
    , visit_detail_id
    , device_source_value
    , device_source_concept_id
    , domain_source
    , site_comparison_key
    , data_partner_id
    , payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
            , COALESCE(site_patient_num, ' ')
            , COALESCE(device_concept_id, ' ')
            , COALESCE(device_exposure_start_date, ' ')
            , COALESCE(device_exposure_start_datetime, ' ')
            , COALESCE(device_exposure_end_date, ' ')
            , COALESCE(device_exposure_end_datetime, ' ')
            , COALESCE(device_type_concept_id, ' ')
            , COALESCE(unique_device_id, ' ')
            , COALESCE(quantity, ' ')
            , COALESCE(provider_id, ' ')
            , COALESCE(site_encounter_num, ' ')
            , COALESCE(visit_detail_id, ' ')
            , COALESCE(device_source_value, ' ')
            , COALESCE(device_source_concept_id, ' ')
            , COALESCE(row_index, ' ')
            , COALESCE(site_comparison_key, ' ')
        )) as hashed_id
        FROM final_table
    )
