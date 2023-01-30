CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/death` AS
    SELECT 
          patient_id AS site_patient_id
        , CAST(death_date as date) as death_date
        , CAST(null as timestamp) as death_datetime
        , 0 as death_type_concept_id
        , 0 as cause_concept_id
        , CAST(null as string) as cause_source_value
        , 0 as cause_source_concept_id
        , CAST(data_partner_id as int) as data_partner_id
		, payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/patient`
    WHERE death_date IS NOT NULL

