/***********************************************************************************************************
project : N3C DI&H
Date: 5/16/2020
Author: Tanner Zhang/ Stephanie Hong / Sandeep Naredla 
FILE:           SP_P2O_SRC_PROVIDER.sql
Description :   Loading NATIVE_PCORNET51_CDM.PROVIDER table into stging table SP_P2O_SRC_PROVIDER
Procedure:      SP_P2O_SRC_PROVIDER
Edit History:
     Ver       Date         Author          Description
     0.1       6/1/2020    TZhang          Initial version
 
*************************************************************************************************************/

CREATE PROCEDURE                CDMH_STAGING.SP_P2O_SRC_VITAL 
(
    DATAPARTNERID IN NUMBER
    , MANIFESTID IN NUMBER 
    , RECORDCOUNT OUT NUMBER
) AS

ht_recordCount number;
wt_recordCount number;
bmi_recordCount number;
diastolic_recordCount number;
systolic_recordCount number;
smoking_recordCount number;
tobacco_recordCount number;

BEGIN
   --execute immediate 'truncate table CDMH_STAGING.ST_OMOP53_MEASUREMENT';
   --commit ;

   INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (--for Height
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
)--22 items

    SELECT
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS measurement_id,
    p.N3cds_Domain_Map_Id AS person_id,  
    mp.target_concept_id  as measurement_concept_id, --concept id for Height, from notes
    v.measure_date as measurement_date, 
    v.measure_date as measurement_datetime,
    v.measure_time as measurement_time, 
    vt.TARGET_CONCEPT_ID AS measurement_type_concept_id,  
    NULL as OPERATOR_CONCEPT_ID,
    v.HT as VALUE_AS_NUMBER, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
    NULL as VALUE_AS_CONCEPT_ID,
    9327 as UNIT_CONCEPT_ID,
    NULL as RANGE_LOW,
    NULL as RANGE_HIGH,
    NULL as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    NULL as visit_detail_id,
    'Height in inches' as MEASUREMENT_SOURCE_VALUE,
    NULL as MEASUREMENT_SOURCE_CONCEPT_ID,
    'Inches' as UNIT_SOURCE_VALUE,
    v.HT as VALUE_SOURCE_VALUE,
    'PCORNET_VITAL' as DOMAIN_SOURCE
FROM NATIVE_PCORNET51_CDM.Vital v
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= v.VITALID AND Mp.Domain_Name='VITAL' AND mp.Target_Domain_Id = 'Measurement' 
            AND mp.DATA_PARTNER_ID=DATAPARTNERID AND mp.target_concept_id = 4177340 --ht
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=v.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=v.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Vital' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.p2o_vital_term_xwalk vt on vt.src_cdm_tbl='VITAL' AND vt.src_cdm_column='VITAL_SOURCE' AND vt.src_code=v.VITAL_SOURCE
;
ht_recordCount := sql%rowcount;
COMMIT;

   INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (-- For weight
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
)--22 items
    SELECT
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS measurement_id,
    p.N3cds_Domain_Map_Id AS person_id,  
    mp.target_concept_id as measurement_concept_id, --concept id for Weight, from notes
    v.measure_date as measurement_date, 
    v.measure_date as measurement_datetime,
    v.measure_time as measurement_time, 
    vt.TARGET_CONCEPT_ID AS measurement_type_concept_id,  
    NULL as OPERATOR_CONCEPT_ID,
    v.WT as VALUE_AS_NUMBER, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
    NULL as VALUE_AS_CONCEPT_ID,
    8739 as UNIT_CONCEPT_ID,
    NULL as RANGE_LOW,
    NULL as RANGE_HIGH,
    NULL as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    NULL as visit_detail_id,
    'Weight in pounds' as MEASUREMENT_SOURCE_VALUE,
    NULL as MEASUREMENT_SOURCE_CONCEPT_ID,
    'Pounds' as UNIT_SOURCE_VALUE,
    v.WT as VALUE_SOURCE_VALUE,
    'PCORNET_VITAL' as DOMAIN_SOURCE
FROM NATIVE_PCORNET51_CDM.Vital v
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= v.VITALID AND Mp.Domain_Name='VITAL' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
                    AND mp.target_concept_id = 4099154 --htv.WT!=0
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=v.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=v.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' AND e.target_domain_id ='Vital' and e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.p2o_vital_term_xwalk vt on vt.src_cdm_tbl='VITAL' AND vt.src_cdm_column='VITAL_SOURCE' AND vt.src_code=v.VITAL_SOURCE
;
wt_recordCount := sql%rowcount;
COMMIT;

   INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (-- For Diastolic BP
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
)--22 items

    SELECT
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS measurement_id,
    p.N3cds_Domain_Map_Id AS person_id,  
    bp.TARGET_CONCEPT_ID as measurement_concept_id, --concept id for DBPs, from notes
    v.measure_date as measurement_date, 
    v.measure_date as measurement_datetime,
    v.measure_time as measurement_time, 
    vt.TARGET_CONCEPT_ID AS measurement_type_concept_id,  
    NULL as OPERATOR_CONCEPT_ID,
    v.DIASTOLIC as VALUE_AS_NUMBER, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
    NULL as VALUE_AS_CONCEPT_ID,
    8876 as UNIT_CONCEPT_ID,
    NULL as RANGE_LOW,
    NULL as RANGE_HIGH,
    NULL as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    NULL as visit_detail_id,
    bp.TARGET_CONCEPT_NAME as MEASUREMENT_SOURCE_VALUE,
    null as MEASUREMENT_SOURCE_CONCEPT_ID,
    'millimeter mercury column' as UNIT_SOURCE_VALUE,
    v.DIASTOLIC as VALUE_SOURCE_VALUE,
    'PCORNET_VITAL' as DOMAIN_SOURCE
FROM NATIVE_PCORNET51_CDM.Vital v
JOIN CDMH_STAGING.p2o_vital_term_xwalk bp on bp.src_cdm_tbl='VITAL' AND bp.src_cdm_column='DIASTOLIC_BP_POSITION' AND bp.src_code=v.bp_position 
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= v.VITALID 
                    AND Mp.Domain_Name='VITAL' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
                    AND bp.target_concept_id = mp.target_concept_id --v.DIASTOLIC!=0

LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=v.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=v.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Vital' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.p2o_vital_term_xwalk vt on vt.src_cdm_tbl='VITAL' AND vt.src_cdm_column='VITAL_SOURCE' AND vt.src_code=v.VITAL_SOURCE
;
Diastolic_Recordcount := sql%rowcount;
commit ;

   INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (-- For Systolic BP
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
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS measurement_id,
    p.N3cds_Domain_Map_Id AS person_id,  
    bp.TARGET_CONCEPT_ID as measurement_concept_id, --concept id for SBPs, from notes
    v.measure_date as measurement_date, 
    null as measurement_datetime,
    v.measure_time as measurement_time, 
    vt.TARGET_CONCEPT_ID AS measurement_type_concept_id,  
    NULL as OPERATOR_CONCEPT_ID,
    v.SYSTOLIC as VALUE_AS_NUMBER, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
    NULL as VALUE_AS_CONCEPT_ID,
    8876 as UNIT_CONCEPT_ID,
    NULL as RANGE_LOW,
    NULL as RANGE_HIGH,
    NULL as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    NULL as visit_detail_id,
    bp.TARGET_CONCEPT_NAME as MEASUREMENT_SOURCE_VALUE,
    null as MEASUREMENT_SOURCE_CONCEPT_ID,
    'millimeter mercury column' as UNIT_SOURCE_VALUE,
    v.SYSTOLIC as VALUE_SOURCE_VALUE,
    'PCORNET_VITAL' as DOMAIN_SOURCE
FROM NATIVE_PCORNET51_CDM.Vital v
JOIN CDMH_STAGING.p2o_vital_term_xwalk bp on bp.src_cdm_tbl='VITAL' AND bp.src_cdm_column='SYSTOLIC_BP_POSITION' AND bp.src_code=v.bp_position 
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= v.VITALID 
                    AND Mp.Domain_Name='VITAL' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
                    AND mp.target_concept_id = bp.target_concept_id -- v.SYSTOLIC!=0

LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=v.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=v.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id= 'Vital' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.p2o_vital_term_xwalk vt on vt.src_cdm_tbl='VITAL' AND vt.src_cdm_column='VITAL_SOURCE' AND vt.src_code=v.VITAL_SOURCE
;
Systolic_Recordcount := sql%rowcount;
commit ;

   INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (-- For Original BMI
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
)--22 items

    SELECT
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS measurement_id,
    p.N3cds_Domain_Map_Id AS person_id,  
    mp.target_concept_id as measurement_concept_id, --concept id for BMI, from notes
    v.measure_date as measurement_date, 
    null as measurement_datetime,
    v.measure_time as measurement_time, 
    vt.TARGET_CONCEPT_ID AS measurement_type_concept_id,  
    NULL as OPERATOR_CONCEPT_ID,
    v.ORIGINAL_BMI as VALUE_AS_NUMBER, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
    NULL as VALUE_AS_CONCEPT_ID,
    NULL as UNIT_CONCEPT_ID,
    NULL as RANGE_LOW,
    NULL as RANGE_HIGH,
    NULL as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    NULL as visit_detail_id,
    'Original BMI' as MEASUREMENT_SOURCE_VALUE,
    NULL as MEASUREMENT_SOURCE_CONCEPT_ID,
    null as UNIT_SOURCE_VALUE,
    v.ORIGINAL_BMI as VALUE_SOURCE_VALUE,
    'PCORNET_VITAL' as DOMAIN_SOURCE
FROM NATIVE_PCORNET51_CDM.Vital v
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= v.VITALID 
                    AND Mp.Domain_Name='VITAL' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
                    AND mp.target_concept_id = 4245997 --v.ORIGINAL_BMI!=0
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=v.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=v.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id = 'Vital' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.p2o_vital_term_xwalk vt on vt.src_cdm_tbl='VITAL' AND vt.src_cdm_column='VITAL_SOURCE' AND vt.src_code=v.VITAL_SOURCE
;
Bmi_Recordcount := sql%rowcount;
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
    mp.target_concept_id as OBSERVATION_CONCEPT_ID, --concept id for Smoking, from notes
    v.measure_date as OBSERVATION_DATE, 
    v.measure_date as OBSERVATION_DATETIME, 
    vt.TARGET_CONCEPT_ID AS OBSERVATION_TYPE_CONCEPT_ID,  
    NULL as VALUE_AS_NUMBER,
    s.TARGET_CONCEPT_NAME as VALUE_AS_STRING,
    s.TARGET_CONCEPT_ID as VALUE_AS_CONCEPT_ID,
    NULL as QUALIFIER_CONCEPT_ID,
    NULL as UNIT_CONCEPT_ID,
    NULL as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    NULL as visit_detail_id,
    CONCAT('VITAL.SMOKING= ',v.SMOKING) as OBSERVATION_SOURCE_VALUE,
    v.SMOKING as OBSERVATION_SOURCE_CONCEPT_ID,
    NULL as UNIT_SOURCE_VALUE,
    NULL as QUALIFIER_SOURCE_VALUE,
    'PCORNET_VITAL' as DOMAIN_SOURCE

FROM NATIVE_PCORNET51_CDM.Vital v
JOIN CDMH_STAGING.p2o_vital_term_xwalk s on s.src_cdm_tbl='VITAL' AND s.src_cdm_column='SMOKING' AND s.src_code = v.SMOKING
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= v.VITALID 
                    AND Mp.Domain_Name='VITAL' AND mp.Target_Domain_Id = 'Observation' AND mp.DATA_PARTNER_ID=DATAPARTNERID
                    AND s.target_concept_id = mp.target_concept_id
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=v.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=v.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.p2o_vital_term_xwalk vt on vt.src_cdm_tbl='VITAL' AND vt.src_cdm_column='VITAL_SOURCE' AND vt.src_code=v.VITAL_SOURCE
;
smoking_recordCount:=SQL%ROWCOUNT;
commit;

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
    mp.target_concept_id as OBSERVATION_CONCEPT_ID, --concept id for TOBACCO, from notes
    v.measure_date as OBSERVATION_DATE, 
    v.measure_date as OBSERVATION_DATETIME, 
    vt.TARGET_CONCEPT_ID AS OBSERVATION_TYPE_CONCEPT_ID,  
    NULL as VALUE_AS_NUMBER,
    s.TARGET_CONCEPT_NAME as VALUE_AS_STRING,
    s.TARGET_CONCEPT_ID as VALUE_AS_CONCEPT_ID,
    NULL as QUALIFIER_CONCEPT_ID,
    NULL as UNIT_CONCEPT_ID,
    NULL as PROVIDER_ID,
    e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
    NULL as visit_detail_id,
    CONCAT('VITAL.TOBACCO= ',v.TOBACCO) as OBSERVATION_SOURCE_VALUE,
    --NULL as OBSERVATION_SOURCE_VALUE,
    v.TOBACCO as OBSERVATION_SOURCE_CONCEPT_ID,
    NULL as UNIT_SOURCE_VALUE,
    NULL as QUALIFIER_SOURCE_VALUE,
    'PCORNET_VITAL' as DOMAIN_SOURCE

FROM NATIVE_PCORNET51_CDM.Vital v
JOIN CDMH_STAGING.p2o_vital_term_xwalk s on s.src_cdm_tbl='VITAL' AND s.src_cdm_column='TOBACCO' AND s.src_code = v.TOBACCO
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= v.VITALID 
                    AND Mp.Domain_Name='VITAL' AND mp.Target_Domain_Id = 'Observation' AND mp.DATA_PARTNER_ID=DATAPARTNERID
                    AND s.target_concept_id = mp.target_concept_id
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=v.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=v.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN JHU_SHONG.p2o_vital_term_xwalk vt on vt.src_cdm_tbl='VITAL' AND vt.src_cdm_column='VITAL_SOURCE' AND vt.src_code=v.VITAL_SOURCE
;
tobacco_recordCount:=SQL%ROWCOUNT;
commit;


RECORDCOUNT  := Bmi_Recordcount+ht_recordCount+wt_recordCount+Systolic_Recordcount+diastolic_recordcount+smoking_recordCount+tobacco_recordCount;
DBMS_OUTPUT.put_line(RECORDCOUNT ||' PCORnet VITAL source data inserted to following staging tables, ST_OMOP53_MEASUREMENT/ST_OMOP53_OBSERVATION, successfully.'); 

END SP_P2O_SRC_VITAL;
