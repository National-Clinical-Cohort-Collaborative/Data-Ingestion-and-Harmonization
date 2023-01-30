CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 777/transform/06 - global id generation/control_map` AS
    
WITH join_conflict_id AS (
    SELECT 
        cm.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 777/transform/04 - mapping/control_map` cm
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 777/transform/05 - id collision lookup tables/control_map` lookup
    ON cm.control_map_id_51_bit = lookup.control_map_id_51_bit
    AND cm.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as control_map_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(control_map_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id as case_person_id
    , pp.person_id as control_person_id
FROM global_id
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 777/transform/06 - global id generation/person` p
ON global_id.site_case_person_id = p.site_person_id and global_id.data_partner_id = p.data_partner_id 
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 777/transform/06 - global id generation/person` pp
ON global_id.site_control_person_id = p.site_person_id and global_id.data_partner_id = pp.data_partner_id