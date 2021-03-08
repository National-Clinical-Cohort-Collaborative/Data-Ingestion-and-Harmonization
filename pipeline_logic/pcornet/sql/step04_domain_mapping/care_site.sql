CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/04 - domain mapping/care_site` AS

with enc_map as (
  SELECT  
    cast(NULL as string) AS care_site_name,
    cast(fx.TARGET_CONCEPT_ID as int) AS place_of_service_concept_id,
    SUBSTRING(enc.facility_type, 1, 50) AS care_site_source_value,   
    SUBSTRING(enc.facility_type, 1, 50) AS place_of_service_source_value,  -- ehr/encounter
    'ENCOUNTER' AS domain_source, 
    data_partner_id,
    payload
    FROM (
      SELECT DISTINCT facility_type, data_partner_id, payload FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/encounter` encounter 
        WHERE encounter.facility_type is not null 
    ) enc
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` fx 
      ON fx.CDM_TBL = 'ENCOUNTER' 
      AND fx.CDM_SOURCE = 'PCORnet' 
      AND fx.CDM_TBL_COLUMN_NAME = 'FACILITY_TYPE' 
      AND enc.facility_type = fx.SRC_CODE
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as care_site_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , care_site_name
    , place_of_service_concept_id
    , care_site_source_value
    , place_of_service_source_value
    , domain_source
    , data_partner_id
    , payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
            , COALESCE(care_site_name, '')
            , COALESCE(place_of_service_concept_id, '')
            , COALESCE(care_site_source_value, '')
            , COALESCE(place_of_service_source_value, '')
        )) as hashed_id
        FROM enc_map
    )
