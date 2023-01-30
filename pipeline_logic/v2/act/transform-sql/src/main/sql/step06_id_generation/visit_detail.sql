CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/visit_detail` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/04 - domain mapping/visit_detail` d
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/05 - pkey collision lookup tables/visit_detail` lookup
    ON d.visit_detail_id_51_bit = lookup.visit_detail_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as visit_detail_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(visit_detail_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

    SELECT
        global_id.*
        -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
        , p.person_id
        , CAST( global_id.site_care_site_id AS long) as care_site_id
        , CAST( global_id.site_provider_id as long) as provider_id
        , visit.visit_occurrence_id
    FROM global_id
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/person` p
    ON global_id.site_person_id = p.site_patient_num
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/visit_occurrence` visit
    ON global_id.site_visit_occurrence_id = visit.site_encounter_num
