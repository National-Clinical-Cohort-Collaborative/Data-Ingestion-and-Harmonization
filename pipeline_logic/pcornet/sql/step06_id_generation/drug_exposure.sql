CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/06 - id generation/drug_exposure` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/04 - domain mapping/drug_exposure` m
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/05 - pkey collision lookup tables/drug_exposure` lookup
    ON m.drug_exposure_id_51_bit = lookup.drug_exposure_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as drug_exposure_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(drug_exposure_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
    , v.visit_occurrence_id
FROM global_id
INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/06 - id generation/person` p
    ON global_id.site_patid = p.site_patid
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/06 - id generation/visit_occurrence` v
    ON global_id.site_encounterid = v.site_encounterid
