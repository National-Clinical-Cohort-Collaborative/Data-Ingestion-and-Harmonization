CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/03 - local id generation/measurement` AS
    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id_51_bit
    FROM (
        SELECT
              measurement_id as site_measurement_id
            , md5(CAST(measurement_id as string)) as hashed_id
            , person_id as site_person_id
            , measurement_concept_id
            , measurement_date
            , measurement_datetime
            , measurement_time
            , measurement_type_concept_id
            , operator_concept_id
            , value_as_number
            , value_as_concept_id
            , unit_concept_id
            , range_low
            , range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value
            , measurement_source_concept_id
            , unit_source_value
            , value_source_value
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/02 - clean/measurement`
        WHERE measurement_id IS NOT NULL
    )   