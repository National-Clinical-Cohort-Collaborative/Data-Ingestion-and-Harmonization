CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/procedure_occurrence` AS

-- source domains: OBSERVATION_FACT
with obs_fact as (
    SELECT
        o.patient_num as site_patient_num,
        CAST(xw.target_concept_id as int) AS procedure_concept_id, -- use from the map to avoid multi-map issue
        CAST(o.start_date as date) AS procedure_date,
        CAST(o.start_date as timestamp) AS procedure_datetime,
        38000275 AS procedure_type_concept_id, -- ssh: 7/27/20 use this type concept id for ehr order list for ACT 
        0 modifier_concept_id, -- need to create a cpt_concept_id table based on the source_code_concept id
        CAST(NULL as int) AS quantity,
        CAST(NULL as int) AS provider_id,
        o.encounter_num AS site_encounter_num,
        CAST(NULL as int) AS visit_detail_id,
        CAST(COALESCE(o.mapped_concept_cd, o.concept_cd) as string) AS procedure_source_value,
        CAST(xw.source_concept_id as int) AS procedure_source_concept_id,
        CAST(xw.src_code_type as string) AS modifier_source_value,
        'OBSERVATION_FACT' AS domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` o
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/a2o_code_xwalk` xw 
            ON xw.src_code_type || ':' || xw.src_code = COALESCE(o.mapped_concept_cd, o.concept_cd)
            -- ON xw.source_code = substr(o.concept_cd, instr(o.concept_cd, ':')+1, length(o.concept_cd))
            AND xw.cdm_tbl = 'OBSERVATION_FACT'
            AND xw.target_domain_id = 'Procedure'
),

final_table AS (
    SELECT
          *
        -- Required for identical rows so that their IDs differ when hashing
        , ROW_NUMBER() OVER (
            PARTITION BY
              site_patient_num
            , procedure_concept_id
            , procedure_date
            , procedure_datetime
            , procedure_type_concept_id
            , modifier_concept_id
            , quantity
            , provider_id
            , site_encounter_num
            , visit_detail_id
            , procedure_source_value
            , procedure_source_concept_id
            , modifier_source_value
            ORDER BY site_patient_num
        ) as row_index
    FROM obs_fact
    WHERE procedure_concept_id IS NOT NULL
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id_51_bit
    , hashed_id
    , site_patient_num
    , procedure_concept_id
    , procedure_date
    , procedure_datetime
    , procedure_type_concept_id
    , modifier_concept_id
    , quantity
    , provider_id
    , site_encounter_num
    , visit_detail_id
    , procedure_source_value
    , procedure_source_concept_id
    , modifier_source_value
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
			, COALESCE(procedure_concept_id, ' ')
			, COALESCE(procedure_date, ' ')
			, COALESCE(procedure_datetime, ' ')
			, COALESCE(procedure_type_concept_id, ' ')
			, COALESCE(modifier_concept_id, ' ')
			, COALESCE(quantity, ' ')
			, COALESCE(provider_id, ' ')
			, COALESCE(site_encounter_num, ' ')
			, COALESCE(visit_detail_id, ' ')
			, COALESCE(procedure_source_value, ' ')
			, COALESCE(procedure_source_concept_id, ' ')
			, COALESCE(modifier_source_value, ' ')
			, COALESCE(row_index, ' ')
			, COALESCE(site_comparison_key, ' ')
        )) as hashed_id
    FROM final_table
)
