/********************************************************************************************************
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong 
File: SP_P2O_SRC_PROCEDURES.sql
Description : Stored Procedure to insert PCORnet PROCEDURES into staging table
CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE / CDMH_STAGING.ST_OMOP53_MEASUREMENT/ CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE /CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE

Procedure: SP_P2O_SRC_PROCEDURES:
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER 
 
Edit History:
6/18/2020 Stephanie Hong
WHEN pr.px_source ='UN' THEN 0 --UN This is not a type concept and it really has no value,  so set to 0 / do not use 45877986 for UN - 6/18/20 SSH
      Date        Author               Description
     0.1       6/16/2020     SHONG        Initial version
               6/18/2020     SHONG        WHEN pr.px_source ='UN' THEN 0 --UN This is not a type concept and it really has no value,  so set to 0 / do not use 45877986 for UN - 6/18/20 SSH
     0.2       6/25/2020     TZHANG       Added logic to insert into CDMH_STAGING.ST_OMOP53_MEASUREMENT
     0.3       6/25/2020     TZHANG       Added logic to insert into CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
     0.4       6/26/2020     TZHANG       Added logic to insert into CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE/ CDMH_STAGING.ST_OMOP53_OBSERVATION/ CDMH_STAGING.ST_OMOP53_OBSERVATION

*********************************************************************************************************/

CREATE PROCEDURE                CDMH_STAGING.SP_P2O_SRC_PROCEDURES 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 

--obs_recordCount number;
proc_recordCount number;
measure_recordCount number;
drug_recordCount number;
device_recordCount number;
obs_recordCount number;

BEGIN
  --PROCEDURES	Observation	118
  --PROCEDURES	Device	158
  --PROCEDURES	Procedure	4295
  --PROCEDURES	Measurement	371
  --PROCEDURES	Drug	123
  --truncate before loading -
  -- execute immediate 'truncate table CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE';
  -- commit ;

   INSERT INTO CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE ( 
    DATA_PARTNER_ID,
    MANIFEST_ID,
    PROCEDURE_OCCURRENCE_ID,
    PERSON_ID,
    PROCEDURE_CONCEPT_ID,
    PROCEDURE_DATE,
    PROCEDURE_DATETIME,
    PROCEDURE_TYPE_CONCEPT_ID,
    MODIFIER_CONCEPT_ID,
    QUANTITY,
    PROVIDER_ID,
    VISIT_OCCURRENCE_ID,
    VISIT_DETAIL_ID,
    PROCEDURE_SOURCE_VALUE,
    PROCEDURE_SOURCE_CONCEPT_ID,
    MODIFIER_SOURCE_VALUE,
    DOMAIN_SOURCE)
    SELECT     
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id, 
    mp.N3cds_Domain_Map_Id AS PROCEDURE_OCCURRENCE_ID,
    p.N3cds_Domain_Map_Id AS person_id,   
    mp.target_concept_id as PROCEDURE_CONCEPT_ID, 
    pr.px_date as PROCEDURE_DATE, 
    null as PROCEDURE_DATETIME,
    CASE WHEN pr.px_source ='BI' THEN 257 
    WHEN pr.px_source ='CL' THEN 32468
    WHEN pr.px_source ='OD' THEN 38000275 --ORDER /EHR 
    WHEN pr.px_source ='UN' THEN 0 --UN This is not a type concept and it really has no value,  so set to 0 / do not use 45877986 for UN - 6/18/20 SSH
    WHEN pr.px_source ='NI' THEN 46237210
    WHEN pr.px_source ='OT' THEN 45878142
    ELSE 0 END AS PROCEDURE_TYPE_CONCEPT_ID, -- use this type concept id for ehr order list
    0 MODIFIER_CONCEPT_ID, -- need to create a cpt_concept_id table based on the source_code_concept id
    null as QUANTITY,
    null as PROVIDER_ID,
    e.n3cds_domain_map_id as VISIT_OCCURRENCE_ID,
    null as VISIT_DETAIL_ID,
    xw.src_code as PROCEDURE_SOURCE_VALUE,
    xw.source_code_concept_id as PROCEDURE_SOURCE_CONCEPT_ID,
    xw.src_code_type as MODIFIER_SOURCE_VALUE,
    'PCORNET_PROCEDURES' AS DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.PROCEDURES pr
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= pr.PROCEDURESID AND Mp.Domain_Name='PROCEDURES' AND mp.Target_Domain_Id = 'Procedure' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pr.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=pr.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    --LEFT JOIN CDMH_STAGING.visit_xwalk vx ON vx.cdm_tbl='ENCOUNTER' AND vx.CDM_NAME='PCORnet' AND vx.src_visit_type=d.ENC_TYPE
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on pr.px = xw.src_code  
                                                  and xw.CDM_TBL = 'PROCEDURES' 
                                                  AND xw.target_domain_id = 'Procedure' 
                                                  and xw.target_concept_id=mp.target_concept_id
                                                  and Xw.Src_Code_Type=pr.px_type
  ;
  proc_recordCount:=Sql%Rowcount;
  COMMIT;
  --for procedures
      INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (
        DATA_PARTNER_ID
        ,MANIFEST_ID
        ,MEASUREMENT_ID
        ,PERSON_ID
        ,MEASUREMENT_CONCEPT_ID
        ,MEASUREMENT_DATE
        ,MEASUREMENT_DATETIME
        ,MEASUREMENT_TIME
        ,MEASUREMENT_TYPE_CONCEPT_ID
        ,OPERATOR_CONCEPT_ID
        ,VALUE_AS_NUMBER
        ,VALUE_AS_CONCEPT_ID
        ,UNIT_CONCEPT_ID
        ,RANGE_LOW
        ,RANGE_HIGH
        ,PROVIDER_ID
        ,VISIT_OCCURRENCE_ID
        ,VISIT_DETAIL_ID
        ,MEASUREMENT_SOURCE_VALUE
        ,MEASUREMENT_SOURCE_CONCEPT_ID
        ,UNIT_SOURCE_VALUE
        ,VALUE_SOURCE_VALUE
        ,DOMAIN_SOURCE ) 
        SELECT
            DATAPARTNERID as data_partner_id,
            MANIFESTID as manifest_id,
            mp.N3cds_Domain_Map_Id AS measurement_id,
            p.N3cds_Domain_Map_Id AS person_id,
            mp.TARGET_CONCEPT_ID  as measurement_concept_id, 
            pr.PX_DATE as measurement_date, 
            pr.PX_DATE as measurement_datetime,
            null as measurement_time, 
--            null as measurement_type_concept_id, --TBD, do we have a concept id to indicate 'procedure' in measurement
            case when pr.px_source ='OD' then 38000179
                when pr.px_source ='BI' then 38000177
                when pr.px_source ='CL' then 38000177
                else 45769798 end AS 
            measurement_type_concept_id,
            NULL as OPERATOR_CONCEPT_ID,
            null as VALUE_AS_NUMBER, --result_num
            NULL as VALUE_AS_CONCEPT_ID,
            NULL as UNIT_CONCEPT_ID,
            NULL as RANGE_LOW,
            NULL as RANGE_HIGH,
            NULL AS PROVIDER_ID,
            e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
            NULL as VISIT_DETAIL_ID,
            pr.PX as MEASUREMENT_SOURCE_VALUE,
            xw.SOURCE_CODE_CONCEPT_ID as MEASUREMENT_SOURCE_CONCEPT_ID,
            NULL as UNIT_SOURCE_VALUE,
            NULL as VALUE_SOURCE_VALUE,
            'PCORNET_PROCEDURES' as DOMAIN_SOURCE
        FROM NATIVE_PCORNET51_CDM.PROCEDURES pr
        JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= pr.PROCEDURESID AND Mp.Domain_Name='PROCEDURES' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pr.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=pr.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
--        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map prv on prv.Source_Id=e.source_id.providerid AND prv.Domain_Name='PROVIDER' AND prv.DATA_PARTNER_ID=DATAPARTNERID 
        LEFT JOIN CDMH_STAGING.P2O_CODE_XWALK_STANDARD xw on pr.px = xw.src_code  
                                                  and xw.CDM_TBL = 'PROCEDURES' 
                                                  AND xw.target_domain_id = 'Measurement' 
                                                  and xw.target_concept_id=mp.target_concept_id
                                                  and Xw.Src_Code_Type=pr.px_type
;
  
  Measure_Recordcount:=sql%rowcount;
  COMMIT;
  
  INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE ( 
    data_partner_id,
    manifest_id,
    drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,drug_exposure_start_datetime,
    drug_exposure_end_date,drug_exposure_end_datetime,
    verbatim_end_date,
    drug_type_concept_id,
    stop_reason,refills,quantity,days_supply,sig,
    route_concept_id,
    lot_number,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value,
    DOMAIN_SOURCE)
    Select
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id, 
    mp.N3cds_Domain_Map_Id AS drug_exposure_id,
    p.N3cds_Domain_Map_Id AS person_id, 
    xw.target_concept_id as drug_concept_id,
    pr.PX_DATE as drug_exposure_start_date,pr.PX_DATE as drug_exposure_start_datetime,
    pr.PX_DATE as drug_exposure_end_date,null as drug_exposure_end_datetime,
    null as verbatim_end_date,
    --xw2.TARGET_CONCEPT_ID as drug_type_concept_id,
    38000179 as drug_type_concept_id,
    null as stop_reason,null as refills,null as quantity,null as days_supply,null as sig,
    null as route_concept_id,
    null as lot_number,
    null as provider_id,
    e.N3cds_Domain_Map_Id as visit_occurrence_id,
    null as visit_detail_id,
    pr. PX as drug_source_value,
    xw.SOURCE_CODE_CONCEPT_ID as drug_source_concept_id,
    null as route_source_value,
    null as dose_unit_source_value,
    'PCORNET_PROCEDURES' as DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.PROCEDURES pr
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= pr.PROCEDURESID
                AND mp.Domain_Name='PROCEDURES' AND mp.Target_Domain_Id = 'Drug' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pr.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=pr.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
--this is to look for drug_concept_id line 27 can use mp.target_concept_id, makes no difference
    LEFT JOIN CDMH_STAGING.P2O_CODE_XWALK_STANDARD xw on pr.px = xw.src_code  
                                                  and xw.CDM_TBL = 'PROCEDURES' 
                                                  AND xw.target_domain_id = 'Drug' 
                                                  and xw.target_concept_id=mp.target_concept_id
                                                  and Xw.Src_Code_Type=pr.px_type
                                                  ;
    drug_recordCount:=sql%rowcount;
    commit;
    
    INSERT INTO CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE( 
    data_partner_id,
    manifest_id,
    DEVICE_EXPOSURE_ID, 
    PERSON_ID, 
    DEVICE_CONCEPT_ID, 
    DEVICE_EXPOSURE_START_DATE, 
    DEVICE_EXPOSURE_START_DATETIME, 
    DEVICE_EXPOSURE_END_DATE, 
    DEVICE_EXPOSURE_END_DATETIME, 
    DEVICE_TYPE_CONCEPT_ID, 
    UNIQUE_DEVICE_ID, 
    QUANTITY, 
    PROVIDER_ID, 
    VISIT_OCCURRENCE_ID, 
    VISIT_DETAIL_ID, 
    DEVICE_SOURCE_VALUE, 
    DEVICE_SOURCE_CONCEPT_ID 
)
    Select
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id as DEVICE_EXPOSURE_ID, 
    p.N3cds_Domain_Map_Id as PERSON_ID, 
    xw.TARGET_CONCEPT_ID as DEVICE_CONCEPT_ID, 
    pr.PX_DATE as DEVICE_EXPOSURE_START_DATE, 
    pr.PX_DATE as DEVICE_EXPOSURE_START_DATETIME, 
    null as DEVICE_EXPOSURE_END_DATE, 
    null as DEVICE_EXPOSURE_END_DATETIME, 
    44818707 as DEVICE_TYPE_CONCEPT_ID, 
    null as UNIQUE_DEVICE_ID, 
    null as QUANTITY, 
    null as PROVIDER_ID, 
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID, 
    null as VISIT_DETAIL_ID, 
    pr.PX as DEVICE_SOURCE_VALUE, 
    xw.SOURCE_CODE_CONCEPT_ID as DEVICE_SOURCE_CONCEPT_ID
    FROM NATIVE_PCORNET51_CDM.PROCEDURES pr
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= pr.PROCEDURESID
                AND mp.Domain_Name='PROCEDURES' AND mp.Target_Domain_Id = 'Device' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pr.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=pr.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.P2O_CODE_XWALK_STANDARD xw on pr.px = xw.src_code  
                                                  and xw.CDM_TBL = 'PROCEDURES' 
                                                  AND xw.target_domain_id = 'Device' 
                                                  and xw.target_concept_id=mp.target_concept_id
                                                  and Xw.Src_Code_Type=pr.px_type
                                                  ;
    device_recordCount:=sql%rowcount;
    COMMIT;
    INSERT INTO CDMH_STAGING.ST_OMOP53_OBSERVATION
   (
  DATA_PARTNER_ID 
,  MANIFEST_ID 
, OBSERVATION_ID 
, PERSON_ID  
, OBSERVATION_CONCEPT_ID  
, OBSERVATION_DATE  
, OBSERVATION_DATETIME  
, OBSERVATION_TYPE_CONCEPT_ID 
, VALUE_AS_NUMBER
, VALUE_AS_STRING
, VALUE_AS_CONCEPT_ID
, QUALIFIER_CONCEPT_ID
, UNIT_CONCEPT_ID 
, PROVIDER_ID 
, VISIT_OCCURRENCE_ID 
, VISIT_DETAIL_ID 
, OBSERVATION_SOURCE_VALUE 
, OBSERVATION_SOURCE_CONCEPT_ID 
, UNIT_SOURCE_VALUE 
, QUALIFIER_SOURCE_VALUE 
, DOMAIN_SOURCE
)

    SELECT
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS OBSERVATION_ID,
    p.N3cds_Domain_Map_Id AS person_id,  
    xw.TARGET_CONCEPT_ID as OBSERVATION_CONCEPT_ID, 
    pr.PX_DATE as OBSERVATION_DATE, 
    pr.PX_DATE as OBSERVATION_DATETIME, 
    xw2.TARGET_CONCEPT_ID AS OBSERVATION_TYPE_CONCEPT_ID,  
    NULL as VALUE_AS_NUMBER,
    NULL as VALUE_AS_STRING,
    NULL as VALUE_AS_CONCEPT_ID,
    NULL as QUALIFIER_CONCEPT_ID,
    NULL as UNIT_CONCEPT_ID,
    NULL as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    NULL as visit_detail_id,
    pr.PX as OBSERVATION_SOURCE_VALUE,
    xw.SOURCE_CODE_CONCEPT_ID as OBSERVATION_SOURCE_CONCEPT_ID,
    NULL as UNIT_SOURCE_VALUE,
    NULL as QUALIFIER_SOURCE_VALUE,
    'PCORNET_PROCEDURES' as DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.PROCEDURES pr
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= pr.PROCEDURESID
                AND mp.Domain_Name='PROCEDURES' AND mp.Target_Domain_Id = 'Observation' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pr.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=pr.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.P2O_CODE_XWALK_STANDARD xw on pr.px = xw.src_code  
                                                  and xw.CDM_TBL = 'PROCEDURES' 
                                                  AND xw.target_domain_id = 'Observation' 
                                                  and xw.target_concept_id=mp.target_concept_id
                                                  and Xw.Src_Code_Type=pr.px_type
    LEFT JOIN CDMH_STAGING.P2O_TERM_XWALK xw2 on pr.px_source = xw2.src_code  
                                                  and xw2.CDM_TBL = 'PROCEDURES' 
                                                  AND xw2.CDM_TBL_COLUMN_NAME = 'PX_SOURCE'
                                                  ;    
    obs_recordCount:=sql%rowcount;
    COMMIT;
    
  Recordcount:=Proc_Recordcount+Measure_Recordcount+drug_recordCount+device_recordCount+obs_recordCount;
  DBMS_OUTPUT.put_line(Recordcount || ' PCORnet PROCEDURE source data inserted to PROCEDURE staging table-ST_OMOP53_PROCEDURE_OCCURRENCE, 
                                         Measurement staging table-ST_OMOP53_MEASUREMENT, 
                                         DRUG staging table-ST_OMOP53_DRUG_EXPOSURE,
                                         Observation staging table-ST_OMOP53_OBSERVATION,
                                         Device staging table-ST_OMOP53_DEVICE_EXPOSURE,
                                         ,successfully.'); 

END SP_P2O_SRC_PROCEDURES;
