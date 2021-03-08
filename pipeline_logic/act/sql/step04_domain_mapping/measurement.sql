CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/measurement` AS

with obs_fact as (
    SELECT
        patient_num AS site_patient_num,
        CAST(xw.target_concept_id as int) AS measurement_concept_id,
        CAST(start_date as date) AS measurement_date,
        CAST(start_date as timestamp) AS measurement_datetime,
        CAST(null as string) AS measurement_time,
        32833 AS measurement_type_concept_id,  -----3017575 Reference lab test results------------
        ---- operator_concept is only relevant for numeric value 
        CASE
            WHEN valtype_cd = 'N' THEN
                CAST(tvqual.TARGET_CONCEPT_ID as int)
            ELSE
                0
        END AS operator_concept_id, -- operator is stored in tval_char / sometimes
        -- ssh/ 7/28/20 - if the value is numerical then store the result in the value_as_number
        CASE
            WHEN valtype_cd = 'N' THEN
                CAST(o.nval_num as float)
            ELSE
                CAST(null as float)
        END AS value_as_number, --result_num
        -----SHONG: if the valtype is categorical use the tval_char, but if tval_char is null then use the valueflag_cd value  
        -----ssh UK: data had null in tval_char and categorical values in the valuelflag_cd 8/1/2020
        CASE
            WHEN valtype_cd = 'T' AND tval_char IS NULL 
                THEN CAST(vfqual.TARGET_CONCEPT_ID as int)
            WHEN valtype_cd = 'T' AND tval_char IS NOT NULL 
                THEN CAST(tvqual.TARGET_CONCEPT_ID as int)
            ELSE
                CAST(null as int)
        END AS value_as_concept_id, -------qualitative result
        --mapped unit concept id
        CASE
            WHEN valtype_cd = 'T' 
                THEN CAST(null as int)
            ELSE
                CAST(units.TARGET_CONCEPT_ID as int)
        END AS unit_concept_id,
        --if the valtype_cd = T and concept_cd like 'LOINC:%' tval_char contains 154 variations - updated :
        ---update rules: ssh: 8/3/20
        --- per michele's note set range to 
        CAST(null as float) AS range_low,   --ssh 8/5/20 set to 0 
        CAST(null as float) AS range_high, --ssh 8/5/20 set to 0 
        CAST(null as int) AS provider_id,
        encounter_num as site_encounter_num,
        CAST(null as int) AS visit_detail_id,
        CAST(COALESCE(o.mapped_concept_cd, o.concept_cd) as string) AS measurement_source_value,
        CAST(xw.source_concept_id as int) AS measurement_source_concept_id,
        CAST(o.units_cd as string) AS unit_source_value,
        ---if numerical value then Concat( tval_char(operator code text like E/NE/LE/L/G/GE) + nval_num + valueflag_cd) 
        ---if categorical value then tval_char contains text and valueflag_cd contains qual result values
        'concept_cd:' || COALESCE(concept_cd, '') || '|tval_char:' || COALESCE(tval_char, '') || '|nval_num:' || COALESCE(o.nval_num, '') || '|valueflag_cd:' || COALESCE(valueflag_cd, '') AS value_source_value,
        'OBSERVATION_FACT' AS domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` o
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/a2o_code_xwalk` xw 
            ON xw.src_code_type || ':' || xw.src_code = COALESCE(o.mapped_concept_cd, o.concept_cd)
            AND xw.cdm_tbl = 'OBSERVATION_FACT'
            AND xw.target_domain_id = 'Measurement'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` units 
            ON o.units_cd = units.SRC_CODE
            AND units.CDM_TBL_COLUMN_NAME = 'UNITS_CD'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` vfqual 
            ON lower(TRIM(o.valueflag_cd)) = lower(TRIM(vfqual.SRC_CODE))
            AND vfqual.CDM_TBL = 'OBSERVATION_FACT'
            AND vfqual.CDM_TBL_COLUMN_NAME = 'VALUEFLAG_CD'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` tvqual 
            ON lower(TRIM(o.tval_char)) = lower(TRIM(tvqual.SRC_CODE))
            AND tvqual.CDM_TBL = 'OBSERVATION_FACT'
            AND tvqual.CDM_TBL_COLUMN_NAME = 'TVAL_CHAR'

UNION ALL

    SELECT
        patient_num AS site_patient_num,
        ---- shong 8/29/20 use : SARS-CoV-2 (COVID-19) RNA [Presence] in Unspecified specimen by NAA with probe detection
        706170 AS measurement_concept_id, --concept id for covid 19 generic lab test result
        CAST(start_date as date) AS measurement_date,
        CAST(start_date as timestamp) AS measurement_datetime,
        CAST(null as string) AS measurement_time,
        32833 AS measurement_type_concept_id,  -----3017575 Reference lab test results------------
        ---- operator_concept is only relevant for numeric value 
        CASE
            WHEN valtype_cd = 'N' THEN
                CAST(tvqual.TARGET_CONCEPT_ID as int)
            ELSE
                0
        END AS operator_concept_id, -- operator is stored in tval_char / sometimes
        -- ssh/ 7/28/20 - if the value is numerical then store the result in the value_as_number
        CASE
            WHEN valtype_cd = 'N' THEN
                CAST(o.nval_num as float)
            ELSE
                CAST(null as float)
        END AS value_as_number, --result_num
        -----SHONG: if the valtype is categorical use the tval_char, but if tval_char is null then use the valueflag_cd value  
        -----ssh UK: data had null in tval_char and categorical values in the valuelflag_cd 8/1/2020
        CASE
            WHEN o.concept_cd like 'ACT|LOCAL|LAB%EQUIVOCAL%' or o.concept_cd like '%UMLS:C4303880%' then 45884087  
            WHEN o.concept_cd like 'ACT|LOCAL|LAB%NEGATIVE%' or o.concept_cd like '%UMLS:C1334932%' then 45878583
            WHEN o.concept_cd like 'ACT|LOCAL|LAB%PENDING%' or o.concept_cd like '%UMLS:C1611271%' then 1177297 
            WHEN o.concept_cd like 'ACT|LOCAL|LAB%POSITIVE%' or o.concept_cd like '%UMLS:C1335447%' then 45884084
            ELSE CAST(null as int)
        END AS value_as_concept_id, -------qualitative result
        --mapped unit concept id
        CASE
            WHEN valtype_cd = 'T' 
                THEN CAST(null as int)
            ELSE
                CAST(units.TARGET_CONCEPT_ID as int)
        END AS unit_concept_id,
        --if the valtype_cd = T and concept_cd like 'LOINC:%' tval_char contains 154 variations - updated :
        ---update rules: ssh: 8/3/20
        --- per michele's note set range to 
        CAST(null as float) AS range_low,   --ssh 8/5/20 set to 0 
        CAST(null as float) AS range_high, --ssh 8/5/20 set to 0 
        CAST(null as int) AS provider_id,
        encounter_num as site_encounter_num,
        CAST(null as int) AS visit_detail_id,
        CAST(o.concept_cd as string) AS measurement_source_value,
        706170 AS measurement_source_concept_id,
        CAST(o.units_cd as string) AS unit_source_value,
        ---if numerical value then Concat( tval_char(operator code text like E/NE/LE/L/G/GE) + nval_num + valueflag_cd) 
        ---if categorical value then tval_char contains text and valueflag_cd contains qual result values
        'concept_cd:' || COALESCE(concept_cd, '') || '|tval_char:' || COALESCE(tval_char, '') || '|nval_num:' || COALESCE(o.nval_num, '') || '|valueflag_cd:' || COALESCE(valueflag_cd, '') AS value_source_value,
        'OBSERVATION_FACT' AS domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` o
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` units 
            ON o.units_cd = units.SRC_CODE
            AND units.CDM_TBL_COLUMN_NAME = 'UNITS_CD'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` vfqual 
            ON lower(TRIM(o.valueflag_cd)) = lower(TRIM(vfqual.SRC_CODE))
            AND vfqual.CDM_TBL = 'OBSERVATION_FACT'
            AND vfqual.CDM_TBL_COLUMN_NAME = 'VALUEFLAG_CD'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` tvqual 
            ON lower(TRIM(o.tval_char)) = lower(TRIM(tvqual.SRC_CODE))
            AND tvqual.CDM_TBL = 'OBSERVATION_FACT'
            AND tvqual.CDM_TBL_COLUMN_NAME = 'TVAL_CHAR'
    WHERE 
        o.concept_cd like '%UMLS%' or 
        o.concept_cd like '%ACT|LOCAL|LAB%'

UNION ALL

    SELECT
        patient_num AS site_patient_num,
        CAST(xw.target_concept_id as int) AS measurement_concept_id, --concept id for covid 19 generic lab test result
        CAST(o.start_date as date) AS measurement_date,
        CAST(o.start_date as timestamp) AS measurement_datetime,
        CAST(null as string) AS measurement_time,
        32833 AS measurement_type_concept_id,  -----32833 ehr  Reference lab test results------------
                ---- operator_concept is only relevant for numeric value
        CAST(null as int) AS operator_concept_id, -- operator is stored in tval_char / sometimes
                -- ssh/ 7/28/20 - if the value is numerical then store the result in the value_as_number
                -- value as num is not relevant for qualitative result
        CAST(null as float) as value_as_number, --result_num
            ----concept_cd will contain the overloaded result text
        CAST(tvqual.TARGET_CONCEPT_ID as int) AS value_as_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as float) AS range_low,   --ssh 8/5/20 set to 0 
        CAST(null as float) AS range_high, --ssh 8/5/20 set to 0 
        CAST(null as int) AS provider_id,
        encounter_num as site_encounter_num,
        CAST(null as int) AS visit_detail_id,
        CAST(o.concept_cd as string) AS measurement_source_value,
        CAST(xw.source_concept_id as int) AS measurement_source_concept_id,
        CAST(o.units_cd as string) AS unit_source_value,
        ---if numerical value then Concat( tval_char(operator code text like E/NE/LE/L/G/GE) + nval_num + valueflag_cd)
        ---if categorical value then tval_char contains text and valueflag_cd contains qual result values
        'concept_cd:' || COALESCE(concept_cd, '') || '|tval_char:' || COALESCE(tval_char, '') || '|nval_num:' || COALESCE(o.nval_num, '') || '|valueflag_cd:' || COALESCE(valueflag_cd, '') AS value_source_value,
        'OBSERVATION_FACT' AS domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` o
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/a2o_code_xwalk` xw 
            ON xw.src_code = TRIM(substr(TRIM(concept_cd), instr(concept_cd, ':') + 1, instr(concept_cd, ' ') - instr(concept_cd, ':') - 1))
            AND xw.cdm_tbl = 'OBSERVATION_FACT'
            AND xw.target_domain_id = 'Measurement'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` tvqual 
            ON lower(TRIM(substr(TRIM(concept_cd), instr(concept_cd, ' ') + 1, length(TRIM(concept_cd))))) = lower(TRIM(tvqual.SRC_CODE))
            AND tvqual.CDM_TBL = 'OBSERVATION_FACT'
            AND tvqual.CDM_TBL_COLUMN_NAME = 'TVAL_CHAR'
        WHERE
            o.concept_cd LIKE 'LOINC%NEGATIVE'
            OR o.concept_cd LIKE 'LOINC%POSITIVE'
            OR o.concept_cd LIKE 'LOINC%EQUIVOCAL'
            OR o.concept_cd LIKE 'LOINC%PENDING'
),

final_table as (
-- Finding only unique measurements, per discussion with Matvey Palchuk + Kristin Kosta 13-08-2020
    SELECT DISTINCT
          *
    FROM obs_fact
    WHERE measurement_concept_id IS NOT NULL
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(base_10_hash_value as bigint) & 2251799813685247 as measurement_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_num
    , measurement_concept_id
    , measurement_date
    , measurement_datetime
    , measurement_time
    , measurement_type_concept_id
    , operator_concept_id
    , value_as_number
    , value_as_concept_id
    , unit_concept_id
    , range_low
    , range_high
    , provider_id
    , site_encounter_num
    , visit_detail_id
    , measurement_source_value
    , measurement_source_concept_id
    , unit_source_value
    , value_source_value
    , domain_source
    , site_comparison_key
    , data_partner_id
    , payload
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
                -- Create primary key by concatenating row, hashing to 128bit hexademical with md5,
                -- and converting to 51 bit int by first taking first 15 hexademical digits and converting
                --  to base 10 (60 bit) and then bit masking to extract the first 51 bits
                , md5(concat_ws(
                    ';'
                    , COALESCE(site_patient_num, ' ')
                    , COALESCE(measurement_concept_id, ' ')
                    , COALESCE(measurement_date, ' ')
                    , COALESCE(measurement_datetime, ' ')
                    , COALESCE(measurement_time, ' ')
                    , COALESCE(measurement_type_concept_id, ' ')
                    , COALESCE(operator_concept_id, ' ')
                    , COALESCE(value_as_number, ' ')
                    , COALESCE(value_as_concept_id, ' ')
                    , COALESCE(unit_concept_id, ' ')
                    , COALESCE(range_low, ' ')
                    , COALESCE(range_high, ' ')
                    , COALESCE(provider_id, ' ')
                    , COALESCE(site_encounter_num, ' ')
                    , COALESCE(visit_detail_id, ' ')
                    , COALESCE(measurement_source_value, ' ')
                    , COALESCE(measurement_source_concept_id, ' ')
                    , COALESCE(unit_source_value, ' ')
                    , COALESCE(value_source_value, ' ')
                    , COALESCE(site_comparison_key, ' ')
                )) as hashed_id
            FROM final_table
        )
    )
)
