CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/visit_detail` AS
    
    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as visit_detail_id_51_bit
    FROM (
        SELECT
              vd.visit_detail_id as site_visit_detail_id
            , md5(CAST(visit_detail_id as string)) as hashed_id
            , vd.person_id as site_person_id
            , vd.visit_detail_concept_id
            , vd.visit_detail_start_date
            , vd.visit_detail_start_datetime
            , vd.visit_detail_end_date
            , vd.visit_detail_end_datetime
            , vd.visit_detail_type_concept_id
            , vd.provider_id as site_provider_id
            , vd.care_site_id as site_care_site_id
            , vd.visit_detail_source_value
            , vd.visit_detail_source_concept_id
            , vd.admitting_source_value
            , vd.admitting_source_concept_id
            , vd.discharge_to_source_value
            , vd.discharge_to_concept_id
            , vd.preceding_visit_detail_id as site_preceding_visit_detail_id
            , vd.visit_detail_parent_id
            , vd.visit_occurrence_id as site_visit_occurrence_id
            , vd.data_partner_id
            , vd.payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/visit_detail` vd
        join `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/visit_occurrence` v
         on vd.visit_occurrence_id = v.visit_occurrence_id
        join  `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/person` p
        on vd.person_id = p.person_id
        WHERE visit_detail_id IS NOT NULL
    )   
