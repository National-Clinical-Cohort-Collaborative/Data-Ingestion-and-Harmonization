CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/visit_detail` AS

    --- build visit_detail using the ADT event data coming in via observation_fact
    --- ICU maps to OMOP concept ID 581379 "Inpatient Critical Care Facility"
    --- ED maps to OMOP concept ID 8870 "Emergency Room - Hospital"
    --- other to OMOP concept ID 8717 "Inpatient Hospital"
    --- observation_fact to visit_detail

with visit_detail as (
    SELECT DISTINCT
        -- there may be more than one adt event per patient and per encounter, need to include datetime for unique visit_detail_id
        -- in case end_date is null use the blob field
        CAST(site_comparison_key || '|' || vd.start_date || '|' || COALESCE( vd.end_date, observation_blob ) as string) as site_visit_detail_id
        , vd.patient_num as site_person_id
        , CASE WHEN vd.parsed_concept_code = 'ADT_ICU' THEN 581379
            WHEN vd.parsed_concept_code = 'ADT_ED' THEN 8870
            WHEN vd.parsed_concept_code = 'ADT_OTHER' THEN 8717
            ELSE 0   
            END AS visit_detail_concept_id 
        , CAST(vd.start_date as date) AS visit_detail_start_date
        , CAST(vd.start_date as timestamp) AS visit_detail_start_datetime
        , CAST(vd.end_date as date) AS visit_detail_end_date
        , CAST(vd.start_date as timestamp) AS visit_detail_end_datetime
        , 32817 AS visit_detail_type_concept_id
        , CAST(provider_id as int) AS site_provider_id
        , CAST(observation_blob as int) AS site_care_site_id
        , concept_cd || observation_blob AS visit_detail_source_value ---TODO: shong, check about the observation_blob
        , CAST( NULL AS int ) AS visit_detail_source_concept_id
        , CAST( NULL AS string ) AS  admitting_source_value
        , CAST( NULL AS int ) AS admitting_source_concept_id
        , CAST( NULL AS string ) AS discharge_to_source_value
        , CAST( NULL AS int ) AS discharge_to_concept_id
        , CAST( NULL AS long ) AS preceding_visit_detail_id
        , CAST( NULL AS long ) AS visit_detail_parent_id
        , vd.encounter_num as site_visit_occurrence_id
        , 'OBSERVATION_FACT' domain_source
        , site_comparison_key
        , vd.data_partner_id
        , vd.payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` vd
    --encounter num for adt data should be a valid visit in the visit_dimension
    INNER JOIN  `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/visit_dimension` enc
       ON enc.encounter_num = vd.encounter_num and enc.patient_num = vd.patient_num
    WHERE vd.concept_cd like 'N3C:ADT_OTHER%' or  concept_cd like 'N3C:ADT_ED%' or concept_cd like 'N3C:ADT_ICU%'
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as visit_detail_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , *
FROM (
    SELECT md5(site_visit_detail_id) as hashed_id, *
    FROM visit_detail
)
