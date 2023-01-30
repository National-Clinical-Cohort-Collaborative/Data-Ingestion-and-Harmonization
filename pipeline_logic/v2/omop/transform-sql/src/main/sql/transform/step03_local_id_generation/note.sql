CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/note` AS

   SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as note_id_51_bit
    FROM (
        SELECT
           n.note_id as site_note_id
            , md5(CAST(note_id as string)) as hashed_id
            , n.person_id as site_person_id
            , n.note_date
            , n.note_datetime
            , n.note_type_concept_id
            , n.note_class_concept_id
            , n.note_title 
            , n.note_text
            , n.encoding_concept_id
            , n.language_concept_id
            , n.provider_id as site_provider_id
            , n.visit_occurrence_id as site_visit_occurrence_id
            , n.visit_detail_id as site_visit_detail_id
            , n.note_source_value
            , n.data_partner_id
            , n.payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/note` n
        WHERE note_id IS NOT NULL
    )