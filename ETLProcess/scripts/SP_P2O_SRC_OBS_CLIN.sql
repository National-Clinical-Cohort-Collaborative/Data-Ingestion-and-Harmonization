/***********************************************************************************************************
project : N3C DI&H
Date: 5/16/2020
Author: Richard Zhu, Stephanie Hong
FILE:           SP_P2O_SRC_OBS_CLIN.sql
Description :   Loading The NATIVE_PCORNET51_CDM.OBS_CLIN Table into
                1. CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE
                2. CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE
                3. CDMH_STAGING.ST_OMOP53_OBSERVATION
                4. CDMH_STAGING.ST_OMOP53_MEASUREMENT
Procedure:      SP_P2O_SRC_OBS_CLIN

Edit History:
     Ver       Date         Author          Description
     0.1       5/16/2020    Shong           Initial version
     0.2       6/25/2020    RZHU            Added logic to insert into CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE

*************************************************************************************************************/

CREATE PROCEDURE CDMH_STAGING.SP_P2O_SRC_OBS_CLIN 
(
    DATAPARTNERID IN NUMBER
,   MANIFESTID IN NUMBER
, RECORDCOUNT OUT NUMBER
) AS 

obs_recordCount number;
measure_recordCount number;
condition_recordCount number;
device_recordCount number;
BEGIN

    -- PCORnet OBS_CLIN to OMOP531 Observation
    
    INSERT INTO CDMH_STAGING.ST_OMOP53_OBSERVATION (
    DATA_PARTNER_ID,
    MANIFEST_ID,
    OBSERVATION_ID,
    PERSON_ID,
    OBSERVATION_CONCEPT_ID ,
    OBSERVATION_DATE ,
    OBSERVATION_DATETIME ,
    OBSERVATION_TYPE_CONCEPT_ID ,
    VALUE_AS_NUMBER ,
    VALUE_AS_STRING ,
    VALUE_AS_CONCEPT_ID ,
    QUALIFIER_CONCEPT_ID ,
    UNIT_CONCEPT_ID ,
    PROVIDER_ID ,
    VISIT_OCCURRENCE_ID ,
    VISIT_DETAIL_ID ,
    OBSERVATION_SOURCE_VALUE ,
    OBSERVATION_SOURCE_CONCEPT_ID ,
    UNIT_SOURCE_VALUE ,
    QUALIFIER_SOURCE_VALUE ,
    DOMAIN_SOURCE
    )
    SELECT 
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS OBSERVATION_ID,
    p.N3cds_Domain_Map_Id AS PERSON_ID,
    mp.target_concept_id as OBSERVATION_CONCEPT_ID,
    obs.obsclin_date as OBSERVATION_DATE,
    obs.obsclin_date as OBSERVATION_DATETIME, --set same with obsclin_date 
    case when obs.obsclin_type = 'LC' then 37079395
        when obs.obsclin_type = 'SM' then 37079429
        else 0 end as OBSERVATION_TYPE_CONCEPT_ID,
    obs.obsclin_result_num as VALUE_AS_NUMBER,
    obs.obsclin_result_text as VALUE_AS_STRING,
    obs.obsclin_result_snomed as VALUE_AS_CONCEPT_ID,
    0 as QUALIFIER_CONCEPT_ID, -- all 0 in data_store schema
    0 as UNIT_CONCEPT_ID, -- all 0 in data_store schema
    null as PROVIDER_ID, --for now, still TBD, Governance group question, Davera will follow up
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    null as VISIT_DETAIL_ID,
    obs.obsclin_code as OBSERVATION_SOURCE_VALUE,
    xw.target_concept_id as OBSERVATION_SOURCE_CONCEPT_ID,
    obs.obsclin_result_unit as UNIT_SOURCE_VALUE, -- lookup unit_concept_code
    obs.obsclin_result_qual as QUALIFIER_SOURCE_VALUE,
    'PCORNET_OBS_CLIN' as DOMAIN_SOURCE 
    FROM NATIVE_PCORNET51_CDM.obs_clin obs
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= obs.OBSCLINID
                AND mp.Domain_Name='OBS_CLIN' AND mp.Target_Domain_Id = 'Observation' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=obs.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=obs.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on obs.obsclin_code = xw.src_code and xw.CDM_TBL = 'OBS_CLIN' AND xw.target_concept_ID=mp.target_concept_id
    ;
    obs_recordCount:=SQL%rowcount;
    COMMIT;
    

    
    -- PCORnet OBS_CLIN to OMOP531 Measurement
     
    INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (
    DATA_PARTNER_ID,
    MANIFEST_ID,
    MEASUREMENT_ID,
    PERSON_ID,
    MEASUREMENT_CONCEPT_ID,
    MEASUREMENT_DATE,
    MEASUREMENT_DATETIME,
    MEASUREMENT_TIME,
    MEASUREMENT_TYPE_CONCEPT_ID,
    OPERATOR_CONCEPT_ID,
    VALUE_AS_NUMBER,
    VALUE_AS_CONCEPT_ID,
    UNIT_CONCEPT_ID,
    RANGE_LOW,
    RANGE_HIGH,
    PROVIDER_ID,
    VISIT_OCCURRENCE_ID,
    VISIT_DETAIL_ID,
    MEASUREMENT_SOURCE_VALUE,
    MEASUREMENT_SOURCE_CONCEPT_ID,
    UNIT_SOURCE_VALUE,
    VALUE_SOURCE_VALUE,
    DOMAIN_SOURCE
    )
    SELECT
    DATAPARTNERID as DATA_PARTNER_ID,
    MANIFESTID as MANIFEST_ID,
    mp.N3cds_Domain_Map_Id AS MEASUREMENT_ID,
    p.N3cds_Domain_Map_Id AS PERSON_ID,
    mp.target_concept_id as MEASUREMENT_CONCEPT_ID,
    obs.obsclin_date as MEASUREMENT_DATE,
    obs.obsclin_date as MEASUREMENT_DATETIME,
    obs.obsclin_time as MEASUREMENT_TIME,
    case when obs.obsclin_type = 'LC' then 37079395
        when obs.obsclin_type = 'SM' then 37079429
        else 0 end as MEASUREMENT_TYPE_CONCEPT_ID,
    null as OPERATOR_CONCEPT_ID,
    obs.obsclin_result_num as VALUE_AS_NUMBER,
    null as VALUE_AS_CONCEPT_ID,
    u.target_concept_id as UNIT_CONCEPT_ID,
    null as RANGE_LOW,
    null as RANGE_HIGH,
    null as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    null as VISIT_DETAIL_ID,
    obs.obsclin_code as MEASUREMENT_SOURCE_VALUE,
    xw.target_concept_id as MEASUREMENT_SOURCE_CONCEPT_ID,
    obs.obsclin_result_unit as UNIT_SOURCE_VALUE,
    obs.obsclin_result_qual as VALUE_SOURCE_VALUE, 
    'PCORNET_OBS_CLIN' as DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.obs_clin obs
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= obs.OBSCLINID
                AND mp.Domain_Name='OBS_CLIN' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=obs.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=obs.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on obs.obsclin_code = xw.src_code and xw.CDM_TBL = 'OBS_CLIN' and xw.target_concept_id=mp.target_concept_id
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard lab on obs.obsclin_code = lab.src_code  and lab.CDM_TBL = 'LAB_RESULT_CM' AND lab.target_domain_id = 'Measurement'
    LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on obs.obsclin_result_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'
    ;
    measure_recordCount:=SQL%rowcount;
    COMMIT;
    


    -- PCORnet OBS_CLIN to OMOP531 Condition_Occurrence
     
    INSERT INTO CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE (  
    data_partner_id,
    manifest_id,
    condition_occurrence_id, 
    person_id,
    condition_concept_id,
    condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime,
    condition_type_concept_id,
    stop_reason, provider_id, 
    visit_occurrence_id, visit_detail_id,
    condition_source_value, condition_source_concept_id, condition_status_source_value, condition_status_concept_id,
    DOMAIN_SOURCE) 
    SELECT
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS condition_occurrence_id, 
    p.N3cds_Domain_Map_Id AS person_id,
    mp.target_concept_id as condition_concept_id,
    obs.obsclin_date as condition_start_date, 
    obs.obsclin_date as condition_start_datetime, 
    obs.obsclin_date as condition_end_date, 
    obs.obsclin_date as condition_end_datetime,
    case when obs.obsclin_type = 'LC' then 37079395
        when obs.obsclin_type = 'SM' then 37079429
        else 0 end as condition_type_concept_id,
    NULL as stop_reason, 
    null as provider_id, 
    e.N3cds_Domain_Map_Id as visit_occurrence_id, 
    null as visit_detail_id,
    obs.obsclin_code as condition_source_value, 
    xw.target_concept_id as condition_source_concept_id, 
    null as condition_status_source_value, 
    null as condition_status_concept_id,
    'PCORNET_OBS_CLIN' as DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.obs_clin obs
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= obs.OBSCLINID
                AND mp.Domain_Name='OBS_CLIN' AND mp.Target_Domain_Id = 'Condition' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=obs.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=obs.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on obs.obsclin_code = xw.src_code and xw.CDM_TBL = 'OBS_CLIN' and Xw.Target_Concept_Id=Mp.Target_Concept_Id;
    condition_recordCount:=SQL%rowcount;
    COMMIT;
    
        INSERT INTO CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE (
        data_partner_id,
        manifest_id,
        device_exposure_id,
        person_id,
        device_concept_id,
        device_exposure_start_date,
        device_exposure_start_datetime,
        device_exposure_end_date,
        device_exposure_end_datetime,
        device_type_concept_id,
        unique_device_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        device_source_value,
        device_source_concept_id,
        domain_source
    )
    SELECT 
        DATAPARTNERID as data_partner_id,
        MANIFESTID as manifest_id,
        mp.N3cds_Domain_Map_Id AS device_exposure_id,
        p.N3cds_Domain_Map_Id AS person_id,
        mp.target_concept_id as device_concept_id,
        obs.obsclin_date as device_exposure_start_date,
        obs.obsclin_date as device_exposure_start_datetime,
        obs.obsclin_date as device_exposure_end_date,
        obs.obsclin_date as device_exposure_end_datetime,
        44818707 as device_type_concept_id, -- default values from draft mappings spreadsheet
        null as unique_device_id, --??
        null as quantity, --??
        null as provider_id,
        e.N3cds_Domain_Map_Id as visit_occurrence_id,
        null as visit_detail_id,
        obs.OBSCLIN_CODE as device_source_value, --??
        null as device_source_concept_id, --??
        'PCORNET_OBS_CLIN' as domain_source
    FROM NATIVE_PCORNET51_CDM.obs_clin obs
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= obs.OBSCLINID
                AND mp.Domain_Name='OBS_CLIN' AND mp.Target_Domain_Id = 'Device' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=obs.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=obs.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on obs.obsclin_code = xw.src_code and xw.CDM_TBL = 'OBS_CLIN' and Xw.Target_Concept_Id=Mp.Target_Concept_Id
    ;
    device_recordCount:=SQL%rowcount;
    COMMIT;
    recordcount:=obs_recordCount+measure_recordCount+condition_recordCount+device_recordCount;
    DBMS_OUTPUT.put_line(Recordcount || ' PCORnet OBS_CLIN source data inserted to ST_OMOP53_CONDITION_OCCURRENCE/ST_OMOP53_MEASUREMENT/ST_OMOP53_OBSERVATION/ST_OMOP53_DEVICE_EXPOSURE, successfully.'); 

END SP_P2O_SRC_OBS_CLIN;
