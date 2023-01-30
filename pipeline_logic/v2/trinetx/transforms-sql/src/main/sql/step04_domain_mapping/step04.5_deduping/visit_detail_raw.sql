CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/04.5 - deduping/visit_detail_raw` AS

 with  visit_detail as (
    SELECT 
        -- it is possible to have null values in  vd.adt_event_end_datetime, adt_event_service, adt_event_location depending on the sites 
        concat_ws(
              ';'
			, COALESCE(vd.patient_id, ' ')
			, COALESCE(vd.encounter_id, ' ')
			, COALESCE(vd.adt_event_start_datetime, ' ')
			, COALESCE(vd.adt_event_end_datetime, ' ')
            , COALESCE(vd.adt_event_location, ' ')
			, COALESCE(vd.adt_event_service, ' ')
        ) as site_visit_detail_id
        , vd.encounter_id AS site_encounter_id
        , vd.patient_id AS site_patient_id
        , CASE WHEN vd.adt_event_is_icu = 't' THEN 581379
            WHEN vd.adt_event_is_ed = 't' THEN 8870
            ELSE 8717   
            END AS visit_detail_concept_id 
        , CAST(vd.adt_event_start_datetime as date) AS visit_detail_start_date
        , CAST(vd.adt_event_start_datetime as timestamp) AS visit_detail_start_datetime
        , CAST(vd.adt_event_end_datetime as date) AS visit_detail_end_date
        , CAST(vd.adt_event_end_datetime as timestamp) AS visit_detail_end_datetime
        , 32817 AS visit_detail_type_concept_id
        , CAST(NULL as long) AS provider_id
        , CAST(NULL as long) AS care_site_id
        , 'adt_event_is_icu:' || adt_event_is_icu || ' adt_event_is_ed:' || adt_event_is_ed || ' ' || 'adt_event_location:' || adt_event_location || 'adt_event_service:' || adt_event_service AS visit_detail_source_value 
        , CAST( NULL AS int ) AS visit_detail_source_concept_id
        , CAST( NULL AS string ) AS  admitting_source_value
        , CAST( NULL AS int ) AS admitting_source_concept_id
        , CAST( NULL AS string ) AS discharge_to_source_value
        , CAST( NULL AS int ) AS discharge_to_concept_id
        , CAST( NULL AS long ) AS preceding_visit_detail_id
        , CAST( NULL AS long ) AS visit_detail_parent_id
        , vd.encounter_id as site_visit_occurrence_id
        , 'ADT' as domain_source
        , vd.data_partner_id
        , vd.payload
     FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/adt` vd
     JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/encounter` e
     ON e.encounter_id = vd.encounter_id and e.patient_id = vd.patient_id
     WHERE adt_event_start_datetime IS NOT NULL 
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
                FROM visit_detail
            )
        )
    )