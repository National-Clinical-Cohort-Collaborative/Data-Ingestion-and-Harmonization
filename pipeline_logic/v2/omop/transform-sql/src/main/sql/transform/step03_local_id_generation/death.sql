CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/death` AS

    SELECT 
            * 
    FROM (
        SELECT
            person_id as site_person_id
            , death_date	
            , death_datetime	
            , death_type_concept_id	
            , cause_concept_id	
            , cause_source_value	
            , cause_source_concept_id	
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/death`
    ) 
