CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/06 - global id generation/death` AS

SELECT 
    d.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/04 - mapping/death` d
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/06 - global id generation/person` p
ON d.site_person_id = p.site_person_id 
