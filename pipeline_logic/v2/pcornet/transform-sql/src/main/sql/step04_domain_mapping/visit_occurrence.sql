CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/visit_occurrence` AS

with enc_obsgen as (
-- encounter
    SELECT 
        encounterid as site_encounterid,
        patid AS site_patid,
        cast(COALESCE(vx.TARGET_CONCEPT_ID, 0) as int) as visit_concept_id,
        cast(enc.admit_date as date) as visit_start_date,
        cast(null as timestamp) as visit_start_datetime, --** MB: admit_date is just a date col, no time info
        cast(enc.discharge_date as date) as visit_end_date,
        cast(null as timestamp) as visit_end_datetime,
        -- confirmed this issue:
        ---Stephanie Hong 6/19/2020 -32035 -default to 32035 "Visit derived from EHR encounter record.
        ---case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        ---when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
        ---else 0 end AS VISIT_TYPE_CONCEPT_ID,  --check with SMEs
        32035 as visit_type_concept_id, ---- where did the record came from / need clarification from SME
        -- MB: the two below fields get filled in during step 06
        CAST(null AS long) as provider_id,
        CAST(null AS long) as care_site_id,
        cast(enc.enc_type as string) as visit_source_value,
        cast(null as int) as visit_source_concept_id,  
        cast(vsrc.TARGET_CONCEPT_ID as int) AS admitting_source_concept_id,
        cast(enc.admitting_source as string) AS admitting_source_value,
        cast(disp.TARGET_CONCEPT_ID as int) AS discharge_to_concept_id,
        cast(enc.discharge_status as string) AS discharge_to_source_value,
        cast(null as long) AS preceding_visit_occurrence_id, 
        'ENCOUNTER' as domain_source,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` enc
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/visit_xwalk_mapping_table` vx
            ON vx.CDM_TBL = 'ENCOUNTER' 
            AND vx.CDM_NAME = 'PCORnet' 
            AND trim(vx.SRC_VISIT_TYPE) = trim(enc.enc_type)
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` vsrc 
            ON vsrc.CDM_TBL = 'ENCOUNTER' 
            AND vsrc.CDM_SOURCE = 'PCORnet' 
            AND vsrc.CDM_TBL_COLUMN_NAME = 'ADMITTING_SOURCE'
            AND vsrc.SRC_CODE = enc.admitting_source 
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` disp 
            ON disp.CDM_TBL = 'ENCOUNTER' 
            AND disp.CDM_SOURCE = 'PCORnet' 
            AND disp.CDM_TBL_COLUMN_NAME ='DISCHARGE_STATUS'
            AND disp.SRC_CODE = enc.discharge_status 

UNION ALL

-- obs_gen
--   MB (10/15) per conversation with Stephanie, create visit_occurrence record from certain obs_gen records even though
--   these obs_gen records already have encounterids. Make sure to hash a unique site id by adding string prefix.
    SELECT
        'obsgenid:' || COALESCE(obsgenid, '') || '|encounterid:' || COALESCE(encounterid, '') as site_encounterid,
        patid AS site_patid,
        581379 AS visit_concept_id,
        CAST(obg.obsgen_start_date as date) AS visit_start_date,
        CAST(obg.OBSGEN_START_DATETIME as timestamp) visit_start_datetime,
        CAST(obg.obsgen_stop_date as date) AS visit_end_date,
        CAST(obg.OBSGEN_STOP_DATETIME as timestamp) AS visit_end_datetime,
        32831 AS visit_type_concept_id,
        CAST(null AS long) as provider_id,
        CAST(null AS long) as care_site_id,
        'obsgen_type:' || COALESCE(obg.obsgen_type, '') || '|obsgen_code:' || COALESCE(obg.obsgen_code, '') || '|obsgen_source: ' || COALESCE(obg.obsgen_source, '') 
            || '|obsgen_result_text:' || COALESCE(obsgen_result_text, '') AS visit_source_value,
        581379 AS visit_source_concept_id,
        32833 AS admitting_source_concept_id,
        CAST(obg.obsgen_source as string) AS admitting_source_value,
        CAST(null AS int) AS discharge_to_concept_id,
        CAST(null AS string) AS discharge_to_source_value,
        CAST(null AS long) AS preceding_visit_occurrence_id,
        'OBS_GEN' AS domain_source,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` obg
    WHERE obg.obsgen_type = 'PC_COVID'
        AND obg.obsgen_code = 2000
        AND obg.obsgen_source = 'DR'
        AND obg.obsgen_result_text = 'Y'
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
                , md5(site_encounterid) as hashed_id
                FROM enc_obsgen
            )
        )
    )