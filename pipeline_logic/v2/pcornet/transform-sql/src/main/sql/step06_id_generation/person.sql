CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/06 - id generation/person` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as id_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/person` m
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/05 - pkey collision lookup tables/person` lookup
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
), 

cte_addr as (
    -- Get most recent address for each patient
    SELECT * FROM (
        SELECT 
            addressid,
            patid,
            address_city,
            address_state,
            address_zip5,
            address_period_start,
            address_period_end,
            data_partner_id,
            payload,
            Row_Number() Over (Partition By patid Order By COALESCE(address_period_end, CURRENT_DATE()) Desc, address_period_start Desc) as addr_rank
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/lds_address_history` addr_hist
    )
    WHERE addr_rank = 1
),

pat_to_loc_id_map AS (
    SELECT 
        cte_addr.patid as site_patid,
        loc.*
    FROM cte_addr
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/06 - id generation/location` loc
        ON address_city = loc.city
        AND address_state = loc.state
        AND address_zip5 = loc.zip
)

SELECT
      global_id.*
    -- Join in the final location id after collision resolutions
    , pat_to_loc_id_map.location_id
FROM global_id
LEFT JOIN pat_to_loc_id_map
    ON global_id.site_patid = pat_to_loc_id_map.site_patid
