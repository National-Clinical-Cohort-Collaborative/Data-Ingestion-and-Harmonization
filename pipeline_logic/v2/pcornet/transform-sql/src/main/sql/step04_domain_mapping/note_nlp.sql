CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/note_nlp` AS

   SELECT
          *
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as note_nlp_id_51_bit
    FROM (
        SELECT
           n.note_nlp_id as site_note_nlp_id
            , md5(CAST(note_nlp_id as string)) as hashed_id
            , note_id as site_note_id
            , n.section_concept_id
            , n.snippet
            , n.offset
            , n.lexical_variant
            , note_nlp_concept_id
            , note_nlp_source_concept_id
            , nlp_system
            , nlp_date
            , nlp_datetime
            , CAST(null as boolean) as term_exists
            , term_temporal 
            , term_modifiers
            , CAST(n.data_partner_id as int) as data_partner_id
            , n.payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/note_nlp` n
        WHERE note_id IS NOT NULL
    )  