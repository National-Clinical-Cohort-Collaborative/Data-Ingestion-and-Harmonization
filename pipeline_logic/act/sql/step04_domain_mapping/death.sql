CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/death` AS

SELECT 
    patient_num as site_patient_num,
    CAST(death_date as date) as death_date,
    CAST(null as timestamp) as death_datetime,
    32510 as death_type_concept_id,
    0 as cause_concept_id,  -- there is no death cause as concept id , no death cause in ACT
    CAST(null as string) as cause_source_value, --put raw ICD10 codes here, no death cause in ACT 
    0 as cause_source_concept_id,  -- this field is number, ICD codes don't fit
    'PATIENT_DIMENSION' as domain_source, 
    data_partner_id,
    payload
FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/patient_dimension` d
WHERE 
    (d.death_date is not null) OR (d.vital_status_cd in ('D', 'DEM|VITAL STATUS:D'))
