CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/visit_occurrence` AS

with visit_dim as (
    SELECT 
        VISIT_DIMENSION_ID,
        encounter_num AS site_encounter_num,
        enc.patient_num AS site_patient_num,
        COALESCE(vx.TARGET_CONCEPT_ID, 46237210)  as visit_concept_id, -- 46237210 = "No information"
        CAST(enc.start_date as DATE) as visit_start_date,
        CAST(enc.start_date as timestamp) as visit_start_datetime,
        --** Use these values, or leave as null? 
        CAST(enc.end_date as date) as visit_end_date,
        CAST(enc.end_date as timestamp) as visit_end_datetime,
        -- confirmed this issue:
        ---Stephanie Hong 6/19/2020 -32035 -default to 32035 "Visit derived from EHR encounter record.
        ---case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        ---when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
        ---else 0 end AS VISIT_TYPE_CONCEPT_ID,  --check with SMEs
        32035 as visit_type_concept_id, ---- where did the record came from / need clarification from SME
        CAST(null AS long) as provider_id,
        CAST(null AS long) as care_site_id, -- CAN WE ADD LOCATION_CD IN visit_dimension as care_site? ssh 7/27/20
        CAST(enc.inout_cd as string) as visit_source_value,
        CAST(null AS int) AS visit_source_concept_id,
        CAST(null AS int) AS admitting_source_concept_id,
        CAST(null AS string) AS admitting_source_value,
        CAST(null AS int) AS discharge_to_concept_id,
        -- enc.dsch_disp_cd as DISCHARGE_TO_SOURCE_VALUE,
        CAST(null AS string) AS discharge_to_source_value,
        CAST(null AS long) AS preceding_visit_occurrence_id,
        'VISIT_DIMENSION' as domain_source,
        enc.data_partner_id,
        enc.payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/visit_dimension` enc
    JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/patient_dimension` p
    on enc.patient_num = p.patient_num and enc.data_partner_id = p.data_partner_id
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/visit_xwalk_table` vx 
            ON vx.CDM_TBL = 'VISIT_DIMENSION' 
            AND vx.CDM_NAME='I2B2ACT' 
            AND vx.SRC_VISIT_TYPE = enc.inout_cd
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
                , md5(CAST(VISIT_DIMENSION_ID as string)) as hashed_id
                FROM visit_dim
            )
        )
    )
