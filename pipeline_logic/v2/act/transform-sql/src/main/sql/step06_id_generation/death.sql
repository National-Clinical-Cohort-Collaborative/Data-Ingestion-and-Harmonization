CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/death` AS

SELECT
      d.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/04 - domain mapping/death` d
-- Inner join to remove patients who've been dropped in step04 due to not having an encounter
INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 777/transform/06 - id generation/person` p
  ON d.site_patient_num = p.site_patient_num
