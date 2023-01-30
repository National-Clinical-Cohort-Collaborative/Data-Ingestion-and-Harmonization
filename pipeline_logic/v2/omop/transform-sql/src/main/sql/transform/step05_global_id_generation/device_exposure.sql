CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/device_exposure` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/device_exposure` d
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/04 - id collision lookup tables/device_exposure` lookup
    ON d.device_exposure_id_51_bit = lookup.device_exposure_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as device_exposure_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(device_exposure_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
    , v.visit_occurrence_id
    , prov.provider_id
FROM global_id
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/person` p
ON global_id.site_person_id = p.site_person_id
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/visit_occurrence` v
ON global_id.site_visit_occurrence_id = v.site_visit_occurrence_id
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/provider` prov
ON global_id.site_provider_id = prov.site_provider_id