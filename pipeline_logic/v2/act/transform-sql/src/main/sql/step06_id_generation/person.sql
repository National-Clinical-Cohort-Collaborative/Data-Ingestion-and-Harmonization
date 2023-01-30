CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/person` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as id_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/04 - domain mapping/person` m
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/05 - pkey collision lookup tables/person` lookup
    ON m.person_id_51_bit = lookup.person_id_51_bit
    AND m.hashed_id = lookup.hashed_id
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
-- Join in the final location id after collision resolutions
    , loc.location_id
FROM global_id
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/location` loc
    ON global_id.location_hashed_id = loc.hashed_id
