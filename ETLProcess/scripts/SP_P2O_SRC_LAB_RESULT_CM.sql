
CREATE PROCEDURE                  CDMH_STAGING.SP_P2O_SRC_LAB_RESULT_CM 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_LAB_RESULT_CM
     Purpose:    Loading The NATIVE_PCORNET51_CDM.LAB_RESULT_CM Table into 
                1. CDMH_STAGING.ST_OMOP53_MEASUREMENT
                2. CDMH_STAGING.ST_OMOP53_OBSERVATION

     Edit History :
     Ver          Date        Author               Description
    0.1       5/16/2020     SHONG Initial Version
    0.2. DI&S update_type_concept_id
*********************************************************************************************************/
measure_recordCount number;
observation_recordCount number;
BEGIN
      DELETE FROM CDMH_STAGING.ST_OMOP53_MEASUREMENT WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_LAB_RESULT_CM';
      COMMIT;
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
            xw.target_concept_id  as measurement_concept_id, --concept id for lab - measurement
            lab.result_date as measurement_date, 
            lab.result_date as measurement_datetime,
            lab.result_time as measurement_time, 
--            case when lab.lab_result_source ='OD' then 5001
--                when lab.lab_result_source ='BI' then 32466
--                when lab.lab_result_source ='CL' then 32466
--                when lab.lab_result_source ='DR' then 45754907
--                when lab.lab_result_source ='NI' then 46237210
--                when lab.lab_result_source ='UN' then 45877986
--                when lab.lab_result_source ='OT' then 45878142
--                else 0 end AS measurement_type_concept_id,  -----lab_result_source determines the ------------
            NVL(xw2.target_concept_id,0) AS measurement_type_concept_id,
            NULL as OPERATOR_CONCEPT_ID,
            lab.result_num as VALUE_AS_NUMBER, --result_num
            case 
                when lower(trim(result_qual)) = 'positive' then 45884084
                when lower(trim(result_qual)) = 'negative' then 45878583
                when lower(trim(result_qual)) = 'pos' then 45884084
                when lower(trim(result_qual)) = 'neg' then 45878583
                when lower(trim(result_qual)) = 'presumptive positive' then 45884084
                when lower(trim(result_qual)) = 'presumptive negative' then 45878583
                when lower(trim(result_qual)) = 'detected' then 45884084
                when lower(trim(result_qual)) = 'not detected' then 45878583
                when lower(trim(result_qual)) = 'inconclusive' then 45877990
                when lower(trim(result_qual)) = 'normal' then 45884153
                when lower(trim(result_qual)) = 'abnormal' then 45878745
                when lower(trim(result_qual)) = 'low' then 45881666
                when lower(trim(result_qual)) = 'high' then 45876384
                when lower(trim(result_qual)) = 'borderline' then 45880922
                when lower(trim(result_qual)) = 'elevated' then 4328749  --ssh add issue number 55 - 6/26/2020 
                when lower(trim(result_qual)) = 'undetermined' then 45880649
                when lower(trim(result_qual)) = 'undetectable' then 45878583 
                when lower(trim(result_qual)) = 'un' then 0
                when lower(trim(result_qual)) = 'unknown' then 0
                when lower(trim(result_qual)) = 'no information' then 46237210
                else 45877393 -- all other cases INVALID, Not Detected, See Comments, TNP, QNS, 
            end as VALUE_AS_CONCEPT_ID,
            u.target_concept_id as UNIT_CONCEPT_ID,
            ---lab.NORM_RANGE_LOW as RANGE_LOW, -- non-numeric fields are found
            ---lab.NORM_RANGE_HIGH as RANGE_HIGH, -- non-numeric data will error on a insert, omop define this column as float
--            case when LENGTH(TRIM(TRANSLATE(NORM_RANGE_LOW, ' +-.0123456789', ' '))) is null Then cast(NORM_RANGE_LOW as float) else 0 end as range_low,
--            case when LENGTH(TRIM(TRANSLATE(NORM_RANGE_HIGH, ' +-.0123456789', ' '))) is null Then cast(NORM_RANGE_HIGH as integer ) else 0 end as range_high,
            case when fn_is_number(NORM_RANGE_LOW)=1 Then cast(NORM_RANGE_LOW as float) else 0 end as range_low,
            case when fn_is_number(NORM_RANGE_HIGH)=1 Then cast(NORM_RANGE_HIGH as integer ) else 0 end as range_high,
            NULL as PROVIDER_ID,
        e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
        NULL as visit_detail_id,
        lab.LAB_LOINC as MEASUREMENT_SOURCE_VALUE,
        --cast(xw.source_code_concept_id as int ) as MEASUREMENT_SOURCE_CONCEPT_ID,
        0 as MEASUREMENT_SOURCE_CONCEPT_ID,
        lab.result_unit as UNIT_SOURCE_VALUE,
        nvl(lab.raw_result, DECODE(lab.result_num, 0, lab.result_qual)) as VALUE_SOURCE_VALUE,
        'PCORNET_LAB_RESULT_CM' AS DOMAIN_SOURCE
        FROM NATIVE_PCORNET51_CDM.LAB_RESULT_CM lab
        JOIN CDMH_STAGING.PERSON_CLEAN pc on lab.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
        JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on Mp.Source_Id= lab.LAB_RESULT_CM_ID AND Mp.Domain_Name='LAB_RESULT_CM' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id= lab.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id= lab.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
        ---LEFT JOIN CDMH_STAGING.N3cds_Domain_Map prv on prv.Source_Id=e.source_id.providerid AND prv.Domain_Name='PROVIDER' AND prv.DATA_PARTNER_ID=DATAPARTNERID 
        JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on lab.lab_loinc = xw.src_code  and xw.CDM_TBL = 'LAB_RESULT_CM' AND xw.target_domain_id = 'Measurement' and xw.target_concept_id=mp.target_concept_id
        LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on lab.result_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'
        LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON lab.LAB_RESULT_SOURCE = xw2.src_code
                                                         AND xw2.cdm_tbl = 'LAB_RESULT_CM'
                                                         AND xw2.cdm_tbl_column_name = 'LAB_RESULT_SOURCE'
        ;
        -- raw_unit contains the correct unit, but the correct column we should use here is result_unit
        -----LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on lab.result_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'; 
        ----SSH: change to use the result_unit/UNC contained error in their original data sent
        ----lab.result_unit use the lab.result_unit when joining to retrieve the target concept id associated with the unit
  measure_recordCount:= sql%rowcount;
  COMMIT;
      DELETE FROM CDMH_STAGING.ST_OMOP53_OBSERVATION WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_LAB_RESULT_CM';
      COMMIT;
      insert into CDMH_STAGING.ST_OMOP53_OBSERVATION (
        DATA_PARTNER_ID
        ,MANIFEST_ID
        ,OBSERVATION_ID
        ,PERSON_ID
        ,OBSERVATION_CONCEPT_ID
        ,OBSERVATION_DATE
        ,OBSERVATION_DATETIME
        ,OBSERVATION_TYPE_CONCEPT_ID
        ,VALUE_AS_NUMBER
        ,VALUE_AS_STRING
        ,VALUE_AS_CONCEPT_ID
        ,QUALIFIER_CONCEPT_ID
        ,UNIT_CONCEPT_ID
        ,PROVIDER_ID
        ,VISIT_OCCURRENCE_ID
        ,VISIT_DETAIL_ID
        ,OBSERVATION_SOURCE_VALUE
        ,OBSERVATION_SOURCE_CONCEPT_ID
        ,UNIT_SOURCE_VALUE
        ,QUALIFIER_SOURCE_VALUE
        ,DOMAIN_SOURCE ) --1870
        Select
        DATAPARTNERID as DATA_PARTNER_ID
        ,MANIFESTID as MANIFEST_ID
        ,mp.n3cds_domain_map_id as OBSERVATION_ID 
        ,p.n3cds_domain_map_id as PERSON_ID
        ,xw.target_concept_id as OBSERVATION_CONCEPT_ID
        ,lab.result_date as OBSERVATION_DATE
        ,lab.result_date as OBSERVATION_DATETIME
--        ,case when lab_result_source = 'OD' then 5001 
--        else 44818702 end as OBSERVATION_TYPE_CONCEPT_ID
        , NVL(xw2.target_concept_id,0) as OBSERVATION_TYPE_CONCEPT_ID 
        ,lab.result_num as VALUE_AS_NUMBER
        ,lab.lab_loinc as VALUE_AS_STRING
        ,4188539 as VALUE_AS_CONCEPT_ID --ssh observation of the measurement -- recorded as yes observation 6/26/2020
        ,case 
            when lower(trim(result_qual)) = 'positive' then 45884084
            when lower(trim(result_qual)) = 'negative' then 45878583
            when lower(trim(result_qual)) = 'pos' then 45884084
            when lower(trim(result_qual)) = 'neg' then 45878583
            when lower(trim(result_qual)) = 'presumptive positive' then 45884084
            when lower(trim(result_qual)) = 'presumptive negative' then 45878583
            when lower(trim(result_qual)) = 'detected' then 45884084
            when lower(trim(result_qual)) = 'not detected' then 45878583
            when lower(trim(result_qual)) = 'inconclusive' then 45877990
            when lower(trim(result_qual)) = 'normal' then 45884153
            when lower(trim(result_qual)) = 'abnormal' then 45878745
            when lower(trim(result_qual)) = 'low' then 45881666
            when lower(trim(result_qual)) = 'high' then 45876384
            when lower(trim(result_qual)) = 'borderline' then 45880922
            when lower(trim(result_qual)) = 'elevated' then 4328749  --ssh add issue number 55 - 6/26/2020
            when lower(trim(result_qual)) = 'undetermined' then 45880649
            when lower(trim(result_qual)) = 'undetectable' then 45878583
            when lower(trim(result_qual)) = 'un' then 0
            when lower(trim(result_qual)) = 'unknown' then 0
            when lower(trim(result_qual)) = 'no information' then 46237210
            else 45877393
         end as QUALIFIER_CONCEPT_ID --null/un/elevated/boderline/ot/low/high/normal/negative/positive/abnormal
        ,u.target_concept_id as UNIT_CONCEPT_ID
        ,p.n3cds_domain_map_id as PROVIDER_ID
        ,e.n3cds_domain_map_id as VISIT_OCCURRENCE_ID
        ,null as VISIT_DETAIL_ID
        ,lab.lab_loinc as OBSERVATION_SOURCE_VALUE
        ,xw.source_code_concept_id as OBSERVATION_SOURCE_CONCEPT_ID
        ,lab.result_unit as UNIT_SOURCE_VALUE
        ,lab.result_modifier as QUALIFIER_SOURCE_VALUE
        , 'PCORNET_LAB_RESULT_CM' as DOMAIN_SOURCE
        --select lab.*, xw.* --7000/22
        FROM NATIVE_PCORNET51_CDM.LAB_RESULT_CM lab
        JOIN CDMH_STAGING.PERSON_CLEAN pc on lab.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
        JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on Mp.Source_Id= lab.LAB_RESULT_CM_ID AND Mp.Domain_Name='LAB_RESULT_CM' AND mp.Target_Domain_Id = 'Observation' AND mp.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id= lab.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id= lab.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
        JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on lab.lab_loinc = xw.src_code  and xw.CDM_TBL = 'LAB_RESULT_CM' AND xw.target_domain_id = 'Observation' 
                    and xw.target_concept_id=mp.target_concept_id
        LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on lab.result_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'
        LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON lab.LAB_RESULT_SOURCE = xw2.src_code
                                                         AND xw2.cdm_tbl = 'LAB_RESULT_CM'
                                                         AND xw2.cdm_tbl_column_name = 'LAB_RESULT_SOURCE'
        ;

        observation_recordCount :=sql%rowcount;
        commit ;
  
  Recordcount:=measure_recordCount+observation_recordCount;
  DBMS_OUTPUT.put_line(Recordcount || ' PCORnet LAB_RESULT_CM source data inserted to MEASUREMENT staging table, ST_OMOP53_MEASUREMENT, successfully.'); 

END SP_P2O_SRC_LAB_RESULT_CM;
