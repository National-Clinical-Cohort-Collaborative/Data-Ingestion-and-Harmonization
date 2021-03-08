CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/06 - id generation/location` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/04 - domain mapping/location` m
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/05 - pkey collision lookup tables/location` lookup
    ON m.location_id_51_bit = lookup.location_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as location_id 
    FROM (
        SELECT
            *
            -- Take collision index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(location_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT * FROM global_id
