CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/06 - id generation/person` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as id_index
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/person` m
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/05 - pkey collision lookup tables/person` lookup
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

pat_with_loc_id AS (
    SELECT 
        pat.site_patient_id,
        loc.*
    -- FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/patient` pat
    -- use person from 04 - domain mapping? include postal code in step4 to person to prepare for location join in step6
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/person` pat
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/06 - id generation/location` loc
    ON pat.postal_code = loc.zip 
)

SELECT
      global_id.site_patient_id,
      global_id.gender_concept_id,
      global_id.year_of_birth,
      global_id.month_of_birth,
      global_id.day_of_birth,
      global_id.birth_datetime,
      global_id.race_concept_id,
      global_id.ethnicity_concept_id,
      global_id.provider_id,
      global_id.care_site_id,
      global_id.person_source_value,
      global_id.gender_source_value,
      global_id.gender_source_concept_id,
      global_id.race_source_value,
      global_id.race_source_concept_id,
      global_id.ethnicity_source_value,
      global_id.ethnicity_source_concept_id,
      global_id.data_partner_id,
      global_id.payload,
      global_id.hashed_id,
      global_id.sub_hash_value,
      global_id.base_10_hash_value,
      global_id.person_id_51_bit,
      global_id.id_index,
      global_id.local_id,
      global_id.person_id,
-- Join in the final location id after collision resolutions
      pat_with_loc_id.location_id
FROM global_id
LEFT JOIN pat_with_loc_id
ON global_id.site_patient_id = pat_with_loc_id.site_patient_id
