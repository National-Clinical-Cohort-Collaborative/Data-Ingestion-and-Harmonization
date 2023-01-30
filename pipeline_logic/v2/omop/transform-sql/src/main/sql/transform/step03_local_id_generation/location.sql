CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/location` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as location_id_51_bit
    FROM (
        SELECT
              location_id as site_location_id
            , md5(CAST(location_id as string)) as hashed_id
            , address_1	
            , address_2	
            , city	
            , state	
            , zip	
            , county	
            , location_source_value	
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/location`
        WHERE location_id IS NOT NULL
    )   
