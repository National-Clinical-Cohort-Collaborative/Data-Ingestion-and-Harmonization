CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/condition_occurrence` AS

with crosswalk_lookup AS (
    SELECT
        patient_num as site_patient_num,
        cast(crosswalk.target_concept_id as int) as condition_concept_id,
        cast(start_date as date) AS condition_start_date,
        cast(start_date as timestamp) AS condition_start_datetime,
        cast(end_date as date) AS condition_end_date,
        cast(end_date as timestamp) AS condition_end_datetime,
        43542353 AS condition_type_concept_id, --already collected from the visit_occurrence_table. / visit_occurrence.visit_source_value 
        cast(null as string) as stop_reason, ---- encounter discharge type e.discharge type
        cast(null as int) as provider_id, ---is provider linked to patient
        encounter_num as site_encounter_num,
        cast(null as int) as visit_detail_id,
        cast(COALESCE(o.mapped_concept_cd, o.concept_cd) as string) AS condition_source_value,
        cast(crosswalk.source_concept_id as int) AS condition_source_concept_id,
        cast(null as string) condition_status_source_value,
        cast(null as int) AS condition_status_concept_id,
        'OBSERVATION_FACT' AS domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` o
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/a2o_code_xwalk` crosswalk
        ON crosswalk.cdm_tbl = 'OBSERVATION_FACT'
        AND crosswalk.target_domain_id = 'Condition'
        AND concat(crosswalk.src_code_type, ':', crosswalk.src_code) = COALESCE(o.mapped_concept_cd, o.concept_cd)
),

final_table AS (
    -- Finding only unique measurements, per discussion with Matvey Palchuk + Kristin Kosta 13-08-2020
    SELECT DISTINCT *
    FROM crosswalk_lookup
    WHERE condition_concept_id IS NOT NULL
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_num
    , condition_concept_id
    , condition_start_date
    , condition_start_datetime
    , condition_end_date
    , condition_end_datetime
    , condition_type_concept_id
    , stop_reason
    , provider_id
    , site_encounter_num
    , visit_detail_id
    , condition_source_value
    , condition_source_concept_id
    , condition_status_source_value
    , condition_status_concept_id
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
        , COALESCE(condition_concept_id, ' ')
        , COALESCE(condition_start_date, ' ')
        , COALESCE(condition_start_datetime, ' ')
        , COALESCE(condition_end_date, ' ')
        , COALESCE(condition_end_datetime, ' ')
        , COALESCE(condition_type_concept_id, ' ')
        , COALESCE(stop_reason, ' ')
        , COALESCE(provider_id, ' ')
        , COALESCE(site_encounter_num, ' ')
        , COALESCE(visit_detail_id, ' ')
        , COALESCE(condition_source_value, ' ')
        , COALESCE(condition_source_concept_id, ' ')
        , COALESCE(condition_status_source_value, ' ')
        , COALESCE(condition_status_concept_id, ' ')
        , COALESCE(site_comparison_key, ' ')
    )) as hashed_id
    FROM final_table
)
