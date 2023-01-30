CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/visit_detail`
TBLPROPERTIES (foundry_transform_profile = 'high-memory') AS

    --- ADTEvents, Stephanie Hong, 4/21/2022
    --- build visit_detail using the ADT event data coming in via obs_gen
    --- ICU maps to OMOP concept ID 581379 "Inpatient Critical Care Facility"
    --- ED maps to OMOP concept ID 8870 "Emergency Room - Hospital"
    --- other to OMOP concept ID 8717 "Inpatient Hospital"
    --- obsgen_code contains the mapped OMOP concept_ids for visit_detail_concept_id
    --- obs_gen source data to visit_detail domain
 with visit_detail_obsgen as (
        SELECT DISTINCT
        --- shong (4/21/22), since there is no source visit_detail_id and we are generating from the obsgen source, generate a new id using the pk fields in obsgen
        COALESCE(obsgenid, '') || '|' || COALESCE(obg.patid, '') || '|' ||  COALESCE(obg.encounterid, '') || CAST(obsgen_start_date AS string) as site_visit_detail_id
        , obg.patid  AS site_patid
        , CAST( obg.obsgen_code AS int) AS visit_detail_concept_id 
        , CAST(obg.obsgen_start_date as date) AS visit_detail_start_date
        , CAST(obg.OBSGEN_START_DATETIME as timestamp) AS visit_detail_start_datetime
        , CAST(obg.obsgen_stop_date as date) AS visit_detail_end_date
        , CAST(obg.OBSGEN_STOP_DATETIME as timestamp) AS visit_detail_end_datetime
        , 32817 AS visit_detail_type_concept_id
        , CAST(raw_obsgen_code as int) AS site_provider_id
        , CAST(null as long) as provider_id
        , CAST( null as long ) AS care_site_id
        , "raw_obsgen_name:" || raw_obsgen_name || '|' || "raw_obsgen_code:" || raw_obsgen_code || '|' || "raw_obsgen_type:" || raw_obsgen_type AS visit_detail_source_value ---TODO: shong, check about the observation_blob
        , CAST( NULL AS int ) AS visit_detail_source_concept_id
        , CAST( NULL AS string ) AS  admitting_source_value
        , CAST( NULL AS int ) AS admitting_source_concept_id
        , CAST( NULL AS string ) AS discharge_to_source_value
        , CAST( NULL AS int ) AS discharge_to_concept_id
        , CAST( NULL AS long ) AS preceding_visit_detail_id
        , CAST( NULL AS long ) AS visit_detail_parent_id
        , obg.encounterid as site_encounter_id
        , 'OBS_GEN' AS domain_source
        , obsgenid || '|' || obg.patid || '|' || obg.encounterid as site_pk_key
        , obg.data_partner_id as data_partner_id
        , obg.payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` obg
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` enc
    ON obg.patid = enc.patid and obg.encounterid = enc.encounterid
    WHERE obg.obsgen_type = 'UD_ADTEVENT' and obg.obsgen_code is not null 
 )

SELECT
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    , cast(base_10_hash_value as bigint) & 2251799813685247 as visit_detail_id_51_bit
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
                , md5(site_visit_detail_id) as hashed_id
                FROM visit_detail_obsgen
            )
        )
    )
