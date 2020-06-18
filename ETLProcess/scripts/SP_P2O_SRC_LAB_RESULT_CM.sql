/**
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong 
Description : Stored Procedure to insert PCORnet LAB_RESULT_CM into staging table
Stored Procedure: SP_P2O_SRC_LAB_RESULT_CM
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER 
**/

  CREATE OR REPLACE EDITIONABLE PROCEDURE "CDMH_STAGING"."SP_P2O_SRC_LAB_RESULT_CM" 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
) AS 
BEGIN
    INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (--for lab_result_cm 
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
            mp.target_concept_id  as measurement_concept_id, --concept id for lab - measurement
            lab.result_date as measurement_date, 
            lab.result_date as measurement_datetime,
            lab.result_time as measurement_time, 
            case when lab.lab_result_source ='OD' then 5001
                when lab.lab_result_source ='BI' then 32466
                when lab.lab_result_source ='CL' then 32466
                when lab.lab_result_source ='DR' then 45754907
                when lab.lab_result_source ='NI' then 46237210
                when lab.lab_result_source ='UN' then 45877986
                when lab.lab_result_source ='OT' then 45878142
                else 0 end AS measurement_type_concept_id,  -----lab_result_source determines the ------------
            NULL as OPERATOR_CONCEPT_ID,
            lab.result_num as VALUE_AS_NUMBER, --result_num
            NULL as VALUE_AS_CONCEPT_ID,
            u.target_concept_id as UNIT_CONCEPT_ID,
            lab.NORM_RANGE_LOW as RANGE_LOW,
            lab.NORM_RANGE_HIGH as RANGE_HIGH,
            NULL as PROVIDER_ID,
        e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
        NULL as visit_detail_id,
        lab.LAB_LOINC as MEASUREMENT_SOURCE_VALUE,
        xw.source_code_concept_id as MEASUREMENT_SOURCE_CONCEPT_ID,
        lab.result_unit as UNIT_SOURCE_VALUE,
        lab.raw_result  as VALUE_SOURCE_VALUE,
        'PCORNET_LAB_RESULT_CM' AS DOMAIN_SOURCE
        FROM NATIVE_PCORNET51_CDM.LAB_RESULT_CM lab
        JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on Mp.Source_Id= lab.LAB_RESULT_CM_ID AND Mp.Domain_Name='LAB_RESULT_CM' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id= lab.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id= lab.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
        ---LEFT JOIN CDMH_STAGING.N3cds_Domain_Map prv on prv.Source_Id=e.source_id.providerid AND prv.Domain_Name='PROVIDER' AND prv.DATA_PARTNER_ID=DATAPARTNERID 
        LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on lab.lab_loinc = xw.src_code  and xw.CDM_TBL = 'LAB_RESULT_CM' AND xw.target_domain_id = 'Measurement'
        LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on lab.raw_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'
        ;
        -----LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on lab.result_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'; 
        ----SSH: change to use the result_unit/UNC contained error in their original data sent
        ----lab.result_unit use the lab.result_unit when joining to retrieve the target concept id associated with the unit


  DBMS_OUTPUT.put_line('PCORnet LAB_RESULT_CM source data inserted to MEASUREMENT staging table, ST_OMOP53_MEASUREMENT, successfully.'); 

END SP_P2O_SRC_LAB_RESULT_CM;

/
