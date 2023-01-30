CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/visit_detail` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/visit_detail` d
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/04 - id collision lookup tables/visit_detail` lookup
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
),

global_visit_detail as ( 

    SELECT
        global_id.*
        -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
        , p.person_id
        , care.care_site_id
        , prov.provider_id
        , visit.visit_occurrence_id
    FROM global_id
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/person` p
    ON global_id.site_person_id = p.site_person_id
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/care_site` care
    ON global_id.site_care_site_id = care.site_care_site_id
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/provider` prov
    ON global_id.site_provider_id = prov.site_provider_id
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/visit_occurrence` visit
    ON global_id.site_visit_occurrence_id = visit.site_visit_occurrence_id
)

SELECT gvd.*
    , enc.visit_occurrence_id as preceding_visit_detail_id
    FROM global_visit_detail gvd
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/05 - global id generation/visit_occurrence` enc
    ON gvd.site_preceding_visit_detail_id = enc.site_visit_occurrence_id