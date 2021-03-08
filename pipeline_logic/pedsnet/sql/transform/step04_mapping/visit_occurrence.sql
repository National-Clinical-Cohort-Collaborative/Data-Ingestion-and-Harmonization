CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/04 - mapping/visit_occurrence` AS
    
    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as visit_occurrence_id_51_bit
    FROM (
        SELECT
              visit_occurrence_id as site_visit_occurrence_id
            , md5(CAST(visit_occurrence_id as string)) as hashed_id
            , person_id as site_person_id
            , CAST(COALESCE(xwalk.TARGET_CONCEPT_ID, visit_concept_id) as int) as visit_concept_id
            , visit_start_date
            , visit_start_datetime
            , visit_end_date
            , visit_end_datetime
            , visit_type_concept_id
            , provider_id as site_provider_id
            , care_site_id as site_care_site_id
            , visit_source_value 
            , visit_source_concept_id
            , admitting_source_concept_id
            , admitting_source_value
            , discharge_to_concept_id
            , discharge_to_source_value
            , preceding_visit_occurrence_id
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.af1cd1ad-8826-4127-ae9e-03510727e971` visit
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Reference and Mapping Materials/pedsnet_visit_xwalk_table` xwalk
        ON visit.visit_concept_id = xwalk.SOURCE_ID
        WHERE visit_occurrence_id IS NOT NULL
    )   
