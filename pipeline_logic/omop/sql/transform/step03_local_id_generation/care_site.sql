CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/03 - local id generation/care_site` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as care_site_id_51_bit
    FROM (
        SELECT
              care_site_id as site_care_site_id
            , md5(CAST(care_site_id as string)) as hashed_id
            , care_site_name
            , place_of_service_concept_id
            , location_id as site_location_id
            , care_site_source_value
            , place_of_service_source_value
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/02 - clean/care_site`
        WHERE care_site_id IS NOT NULL
    )   