CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/condition_occurrence` AS

with cond_diagnosis_obsclin AS (
    -- CONDITION
    SELECT
        patid AS site_patid,  
        cast(xw.target_concept_id as int) as condition_concept_id, 
        cast(c.report_date as date) as condition_start_date, 
        cast(null as timestamp) as condition_start_datetime, 
        cast(c.resolve_date as date) as condition_end_date, 
        cast(null as timestamp) as condition_end_datetime,
        -- case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        --     when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
        -- else 0 end AS condition_type_concept_id,  --check again
        32817 as condition_type_concept_id,
        --NULL as condition_type_concept_id, ---ehr/ip/op/AG? setting of care origin ehr system /insclaim / registry / other sources/check the visit/ encounter type for this
        cast(null as string) as stop_reason,
        cast(null as long) as provider_id, 
        encounterid as site_encounterid,
        cast(null as long) as visit_detail_id, 
        cast(c.condition as string) AS condition_source_value,
        cast(xw.source_concept_id as int) as condition_source_concept_id,
        cast(c.condition_status as string) AS condition_status_source_value,    
        cast(cst.TARGET_CONCEPT_ID as int) AS condition_status_concept_id,
        'CONDITION' as domain_source,
        conditionid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/condition` c
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
        ON c.condition = xw.src_code 
        AND xw.CDM_TBL='CONDITION' 
        AND xw.target_domain_id = 'Condition'
        AND xw.src_code_type = c.condition_type
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` csrc 
        ON csrc.CDM_TBL = 'CONDITION' 
        AND csrc.CDM_TBL_COLUMN_NAME = 'CONDITION_SOURCE' 
        AND csrc.SRC_CODE = c.condition_source
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` cst 
        ON cst.CDM_TBL = 'CONDITION' 
        AND cst.CDM_TBL_COLUMN_NAME = 'CONDITION_STATUS' 
        AND cst.SRC_CODE = c.condition_status

UNION ALL

    -- DIAGNOSIS
    SELECT 
        patid AS site_patid,  
        cast(xw.target_concept_id as int) as condition_concept_id, 
        cast(COALESCE(d.dx_date, d.admit_date) as date) as condition_start_date,
        -- COALESCE(d.dx_date, d.admit_date) as condition_start_datetime, --** MB: these are date columns
        cast(null as timestamp) as condition_start_datetime,
        cast(null as date) as condition_end_date, 
        cast(null as timestamp) as condition_end_datetime,
    --    vx.target_concept_id AS condition_type_concept_id, --already collected fro the visit_occurrence_table. / visit_occurrence.visit_source_value 
    --    43542353 condition_type_concept_id,
        cast(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) as condition_type_concept_id,
        cast(null as string) as stop_reason, ---- encounter discharge type e.discharge type
        cast(null as long) as provider_id,
        encounterid as site_encounterid,
        cast(null as long) as visit_detail_id, 
        cast(d.dx as string) AS condition_source_value,
        cast(xw.source_concept_id as int) as condition_source_concept_id,
        cast(null as string) AS condition_status_source_value,    
        cast(null as int) AS condition_status_concept_id,
        'DIAGNOSIS' as domain_source,
        diagnosisid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/diagnosis` d
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
        ON d.dx = xw.src_code
        AND xw.CDM_TBL = 'DIAGNOSIS' 
        AND xw.target_domain_id = 'Condition'
        and xw.src_code_type = d.dx_type
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
        ON d.dx_origin = xw2.SRC_CODE
        AND xw2.CDM_TBL = 'DIAGNOSIS'
        AND xw2.CDM_TBL_COLUMN_NAME = 'dx_origin'

UNION ALL

    -- OBS_CLIN
    SELECT
        patid AS site_patid,  
        cast(xw.target_concept_id as int) as condition_concept_id, 
        cast(obs.obsclin_start_date as date) AS condition_start_date,
        cast(obs.OBSCLIN_START_DATETIME as timestamp) AS condition_start_datetime,
        cast(obs.obsclin_stop_date as date) AS condition_end_date,
        cast(obs.OBSCLIN_STOP_DATETIME as timestamp) AS condition_end_datetime,
--            CASE
--                WHEN obs.obsclin_type = 'LC' THEN
--                    37079395
--                WHEN obs.obsclin_type = 'SM' THEN
--                    37079429
--                ELSE
--                    0
--            END AS condition_type_concept_id,
        32817 AS condition_type_concept_id,
        cast(null as string) as stop_reason, 
        cast(null as long) as provider_id, 
        encounterid as site_encounterid,
        cast(null as long) as visit_detail_id, 
        cast(obs.obsclin_code as string) AS condition_source_value,
        cast(xw.target_concept_id as int) AS condition_source_concept_id,
        cast(null as string) as condition_status_source_value,
        cast(null as int) AS condition_status_concept_id,
        'OBS_CLIN' AS domain_source,
        obsclinid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_clin` obs
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw
            ON obs.obsclin_code = xw.src_code
            AND obs.mapped_obsclin_type = xw.src_vocab_code
            AND xw.CDM_TBL = 'OBS_CLIN'
            AND xw.target_domain_id = 'Condition'
),

final_table AS (
-- Finding only unique measurements, per discussion with Matvey Palchuk + Kristin Kosta 13-08-2020
    SELECT DISTINCT
          *
    FROM cond_diagnosis_obsclin
    WHERE condition_concept_id IS NOT NULL
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patid
    , condition_concept_id
    , condition_start_date
    , condition_start_datetime
    , condition_end_date
    , condition_end_datetime
    , condition_type_concept_id
    , condition_status_concept_id
    , stop_reason
    , provider_id
    , site_encounterid
    , visit_detail_id
    , condition_source_value
    , condition_status_source_value
    , condition_source_concept_id
    , domain_source
    , site_pkey
    , data_partner_id
    , payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
        , COALESCE(site_patid, '')
        , COALESCE(condition_concept_id, '')
        , COALESCE(condition_start_date, '')
        , COALESCE(condition_start_datetime, '')
        , COALESCE(condition_end_date, '')
        , COALESCE(condition_end_datetime, '')
        , COALESCE(condition_type_concept_id, '')
        , COALESCE(stop_reason, '')
        , COALESCE(provider_id, '')
        , COALESCE(site_encounterid, '')
        , COALESCE(visit_detail_id, '')
        , COALESCE(condition_source_value, '')
        , COALESCE(condition_source_concept_id, '')
        , COALESCE(condition_status_source_value, '')
        , COALESCE(condition_status_concept_id, '')
        , site_pkey
        )) as hashed_id
        FROM final_table
    )
