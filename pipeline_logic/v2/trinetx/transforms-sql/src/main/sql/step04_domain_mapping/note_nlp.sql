CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/note_nlp` AS	

   SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as note_nlp_id_51_bit
    FROM (
        SELECT DISTINCT
           n.note_nlp_id as site_note_nlp_id
            , n.hashed_id
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
            , CAST(term_temporal as string ) as term_temporal
            , CAST(term_modifiers as string ) as term_modifiers
            , CAST(n.data_partner_id as int) as data_partner_id
            , n.payload
            FROM 
            ( 
            SELECT DISTINCT 
                * 
                , md5(concat_ws( ';'
                    ,coalesce(note_nlp_id, '')
                    ,coalesce(note_id, '')
                    ,coalesce(section_concept_id, '')
                    ,coalesce(snippet, '')
                    ,coalesce(offset, '')
                    ,coalesce(lexical_variant, '')
                    ,coalesce(note_nlp_concept_id, '')
                    ,coalesce(note_nlp_source_concept_id, '')
                    ,coalesce(nlp_system, '')
                    ,coalesce(nlp_date, '')
                    ,coalesce(nlp_datetime, '')
                    ,coalesce(term_temporal, '')
                    ,coalesce(term_modifiers, '')
                    )) as hashed_id   
                    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/note_nlp` nlp
                    WHERE note_id IS NOT NULL and note_nlp_id is not null 
            ) n
    
        )  