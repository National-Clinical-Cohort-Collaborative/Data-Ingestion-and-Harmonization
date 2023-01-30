CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/visit_occurrence` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/04 - domain mapping/visit_occurrence` m
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/05 - pkey collision lookup tables/visit_occurrence` lookup
    ON m.visit_occurrence_id_51_bit = lookup.visit_occurrence_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as visit_occurrence_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(visit_occurrence_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM global_id
-- Inner join to remove patients who've been dropped in step04 due to not having an encounter
INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/person` p
    ON global_id.site_patient_num = p.site_patient_num
