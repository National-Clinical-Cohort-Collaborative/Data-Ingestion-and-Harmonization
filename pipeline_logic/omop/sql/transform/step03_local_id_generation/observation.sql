CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/03 - local id generation/observation` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id_51_bit
    FROM (
        SELECT
              observation_id as site_observation_id
            , md5(CAST(observation_id as string)) as hashed_id
            , person_id as site_person_id
            , observation_concept_id
            , observation_date
            , observation_datetime
            , observation_type_concept_id
            , value_as_number
            , value_as_string
            , value_as_concept_id
            , qualifier_concept_id
            , unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , observation_source_value
            , observation_source_concept_id
            , unit_source_value
            , qualifier_source_value
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/02 - clean/observation`
        WHERE observation_id IS NOT NULL
    )   