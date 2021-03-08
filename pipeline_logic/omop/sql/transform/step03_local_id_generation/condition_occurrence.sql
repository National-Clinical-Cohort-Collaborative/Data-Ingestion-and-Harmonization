CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/03 - local id generation/condition_occurrence` AS
    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id_51_bit
    FROM (
        SELECT
              condition_occurrence_id as site_condition_occurrence_id
            , md5(CAST(condition_occurrence_id as string)) as hashed_id
            , person_id as site_person_id
            , condition_concept_id
            , condition_start_date
            , condition_start_datetime
            , condition_end_date
            , condition_end_datetime
            , condition_type_concept_id
            , condition_status_concept_id
            , stop_reason
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value
            , condition_source_concept_id
            , condition_status_source_value
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/02 - clean/condition_occurrence` 
        WHERE condition_occurrence_id IS NOT NULL
    )   
