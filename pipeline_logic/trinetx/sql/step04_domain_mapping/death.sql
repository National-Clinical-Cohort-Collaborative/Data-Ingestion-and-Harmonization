CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/04 - domain mapping/death` AS
    SELECT 
          PATIENT_ID AS site_patient_id
        , CAST(`DEATH_DATE` as date) as death_date
        , CAST(null as timestamp) as death_datetime
        , 0 as death_type_concept_id
        , 0 as cause_concept_id
        , CAST(null as string) as cause_source_value
        , 0 as cause_source_concept_id
        , CAST(DATA_PARTNER_ID as int) as data_partner_id
		, payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/patient`
    WHERE `DEATH_DATE` IS NOT NULL

