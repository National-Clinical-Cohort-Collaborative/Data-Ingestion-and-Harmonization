CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/visit_dimension_excluded` AS
    SELECT 
        VISIT_DIMENSION_ID
        ,enc.patient_num
        ,encounter_num
        ,active_status_cd
        ,start_date
        ,end_date
        ,inout_cd
        ,location_cd
        ,location_path
        ,length_of_stay
        ,enc.update_date
        ,enc.download_date
        ,enc.import_date
        ,enc.sourcesystem_cd
        ,enc.upload_id
        ,'VISIT_DIMENSION' as domain_source
        ,enc.data_partner_id
        ,enc.payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/visit_dimension` enc
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/patient_dimension` p
    on enc.patient_num = p.patient_num and enc.data_partner_id = p.data_partner_id
    where p.patient_num is null and enc.encounter_num is not null 
