CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/04 - mapping/observation_period` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_period_id_51_bit
    FROM (
        SELECT
              observation_period_id as site_observation_period_id
            , md5(CAST(observation_period_id as string)) as hashed_id
            , person_id as site_person_id
            , observation_period_start_date
            , observation_period_end_date
            , period_type_concept_id
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/observation_period`
        WHERE observation_period_id IS NOT NULL
    )   
