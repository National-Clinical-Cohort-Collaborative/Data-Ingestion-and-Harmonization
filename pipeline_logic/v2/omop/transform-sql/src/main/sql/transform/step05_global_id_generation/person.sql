CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/person` AS


WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as id_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/person` d
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/04 - id collision lookup tables/person` lookup
    ON d.person_id_51_bit = lookup.person_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as person_id 
    FROM (
        SELECT
            *
            -- Take collision index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(person_id_51_bit, 2) + id_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , loc.location_id
    , prov.provider_id
    , care.care_site_id
FROM global_id
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/location` loc
ON global_id.site_location_id = loc.site_location_id
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/provider` prov
ON global_id.site_provider_id = prov.site_provider_id
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/care_site` care
ON global_id.site_care_site_id = care.site_care_site_id