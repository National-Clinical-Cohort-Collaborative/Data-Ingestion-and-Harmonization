
CREATE PROCEDURE                                                                                 CDMH_STAGING.SP_P2O_SRC_DIAGNOSIS 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_DIAGNOSIS
     Purpose:    Loading The NATIVE_PCORNET51_CDM.DIAGNOSIS Table into 
                1. CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE
                2. CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE
                3. CDMH_STAGING.ST_OMOP53_OBSERVATION
                4. CDMH_STAGING.ST_OMOP53_MEASUREMENT
                5. CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
     Source:
     Revisions:
     Ver          Date        Author               Description
    0.1       5/16/2020     SHONG Initial Version
	 0.2       6/25/2020      RZHU          Added logic to insert into CDMH_STAGING.ST_OMOP53_MEASUREMENT,
                                            ST_OMOP53_PROCEDURE_OCCURRENCE,ST_OMOP53_DRUG_EXPOSURE
     0.3       6/26/2020      RZHU          Added logic for observation_type_concept_id/drug_type_concept_id
                                            /measurement_type_concept_id/procedure_type_concept_id
     0.4      8/20/2020      DIH            Updated condition_type_concept_id to 43542353
     0.5       9/9/2020      DIH            Updated the *_type_concept_id logic
*********************************************************************************************************/
condition_recordCount number;
observation_recordCount number;
measurement_recordCount number;
proc_recordCount number;
drug_recordCount number;

BEGIN
      DELETE FROM CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DIAGNOSIS';
      COMMIT;  
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
    Mp.N3cds_Domain_Map_Id AS condition_occurrence_id,
    p.N3cds_Domain_Map_Id AS person_id,   
    xw.target_concept_id as condition_concept_id, 
    nvl(d.dx_date,d.admit_date) as condition_start_date,
     nvl(d.dx_date,d.admit_date) as condition_start_datetime,
    null as condition_end_date, 
    null as condition_end_datetime,
--    vx.target_concept_id AS condition_type_concept_id, --already collected fro the visit_occurrence_table. / visit_occurrence.visit_source_value 
--    43542353 condition_type_concept_id,
    NVL(xw2.target_concept_id,0) as condition_type_concept_id,
    NULL as stop_reason, ---- encounter discharge type e.discharge type
    null as provider_id, ---is provider linked to patient
    e.N3cds_Domain_Map_Id as visit_occurrence_id, 
    null as visit_detail_id, 
    d.dx AS condition_source_value,
    xw.source_code_concept_id as condition_source_concept_id,
   null AS condition_status_source_value,    
   null AS CONDITION_STATUS_CONCEPT_ID,
   'PCORNET_DIAGNOSIS' as DOMAIN_SOURCE
FROM NATIVE_PCORNET51_CDM.diagnosis d
JOIN CDMH_STAGING.PERSON_CLEAN pc on d.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= d.DIAGNOSISID AND Mp.Domain_Name='DIAGNOSIS' AND mp.Target_Domain_Id = 'Condition' AND mp.DATA_PARTNER_ID=DATAPARTNERID
JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=d.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=d.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.visit_xwalk vx ON vx.cdm_tbl='ENCOUNTER' AND vx.CDM_NAME='PCORnet' AND vx.src_visit_type=d.ENC_TYPE
JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on d.dx = xw.src_code  and xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Condition'
                                                          and xw.target_concept_id=mp.target_concept_id
                                                          and Xw.Src_Code_Type=d.DX_TYPE
LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON d.dx_origin = xw2.src_code
                                                         AND xw2.cdm_tbl = 'DIAGNOSIS'
                                                         AND xw2.cdm_tbl_column_name = 'dx_origin'
                                                         ;
condition_recordCount := sql%rowcount;
COMMIT;
      DELETE FROM CDMH_STAGING.st_omop53_observation WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DIAGNOSIS';
      COMMIT; 
INSERT INTO CDMH_STAGING.st_omop53_observation (
      data_partner_id,
      manifest_id,
      observation_id,
      person_id,
      observation_concept_id,
      observation_date,
      observation_datetime,
      observation_type_concept_id,
      value_as_number,
      value_as_string,
      value_as_concept_id,
      qualifier_concept_id,
      unit_concept_id,
      provider_id,
      visit_occurrence_id,
      visit_detail_id,
      observation_source_value,
      observation_source_concept_id,
      unit_source_value,
      qualifier_source_value,
      DOMAIN_SOURCE )
  SELECT
      DATAPARTNERID as data_partner_id,
      MANIFESTID as manifest_id,
      mp.N3cds_Domain_Map_Id as observation_id,
      p.N3cds_Domain_Map_Id as person_id,
      xw.target_concept_id as observation_concept_id,
      NVL(d.DX_DATE,d.ADMIT_DATE) as observation_date,
      NVL(d.DX_DATE,d.ADMIT_DATE) as observation_datetime, -- same as observation_date
--      0  as observation_type_concept_id, --d.DX_SOURCE: need to find concept_id or from P2O_Term_xwalk
--      case when d.PDX = 'P' then 4307107
--        when d.PDX = 'S' then 4309641
--      else 38000280 end as observation_type_concept_id, --default values from draft mappings spreadsheet --added 6/26
      NVL(xw2.target_concept_id,0) as observation_type_concept_id,
      null as value_as_number,
      null as value_as_string,
      xw.target_concept_id as value_as_concept_id,
      null as qualifier_concept_id,
      null as unit_concept_id,
      null as provider_id,
      e.N3cds_Domain_Map_Id as visit_occurrence_id,
      null as visit_detail_id,
      d.DX as observation_source_value,
      xw.source_code_concept_id as observation_source_concept_id,
      null as unit_source_value,
      null as qualifier_source_value,
      'PCORNET_DIAGNOSIS' as DOMAIN_SOURCE 
  FROM NATIVE_PCORNET51_CDM.diagnosis d
  JOIN CDMH_STAGING.PERSON_CLEAN pc on d.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
  JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= d.DIAGNOSISID AND mp.Domain_Name='DIAGNOSIS' AND mp.Target_Domain_Id = 'Observation' AND mp.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=d.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=d.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
  JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on d.dx = xw.src_code and xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Observation'
                                                          AND xw.target_concept_id = mp.target_concept_id
                                                          and Xw.Src_Code_Type=d.DX_TYPE
  LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON d.dx_origin = xw2.src_code
                                                         AND xw2.cdm_tbl = 'DIAGNOSIS'
                                                         AND xw2.cdm_tbl_column_name = 'dx_origin'
  ;
  observation_recordCount:= sql%rowcount;
  COMMIT;
      DELETE FROM CDMH_STAGING.ST_OMOP53_MEASUREMENT WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DIAGNOSIS';
      COMMIT; 
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
      DOMAIN_SOURCE)
  SELECT
      DATAPARTNERID as data_partner_id,
      MANIFESTID as manifest_id,
      mp.N3cds_Domain_Map_Id AS measurement_id,
      p.N3cds_Domain_Map_Id AS person_id,  
      xw.target_concept_id  as measurement_concept_id,
       nvl(d.dx_date,d.admit_date) as MEASUREMENT_DATE,
       nvl(d.dx_date,d.admit_date) as MEASUREMENT_DATETIME,
      null as measurement_time,
--      null AS measurement_type_concept_id, 
--      5001 AS measurement_type_concept_id, --default values from draft mappings spreadsheet --added on 6/26
      NVL(xw2.target_concept_id,0) as measurement_type_concept_id,
      NULL as OPERATOR_CONCEPT_ID,
      null as VALUE_AS_NUMBER,
      NULL as VALUE_AS_CONCEPT_ID,
      null as UNIT_CONCEPT_ID,
      null as RANGE_LOW,
      null as RANGE_HIGH,
      NULL as PROVIDER_ID,
      e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
      NULL as visit_detail_id,
      d.DX as MEASUREMENT_SOURCE_VALUE,
      xw.source_code_concept_id as MEASUREMENT_SOURCE_CONCEPT_ID,
      null as UNIT_SOURCE_VALUE,
      null  as VALUE_SOURCE_VALUE,
      'PCORNET_DIAGNOSIS' as DOMAIN_SOURCE 
  FROM NATIVE_PCORNET51_CDM.diagnosis d
  JOIN CDMH_STAGING.PERSON_CLEAN pc on d.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
  JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= d.DIAGNOSISID AND mp.Domain_Name='DIAGNOSIS' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=d.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=d.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
  JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on d.dx = xw.src_code and xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Measurement' 
                                                AND xw.target_concept_id = mp.target_concept_id
                                                and Xw.Src_Code_Type=d.DX_TYPE
    LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON d.dx_origin = xw2.src_code
                                                         AND xw2.cdm_tbl = 'DIAGNOSIS'
                                                         AND xw2.cdm_tbl_column_name = 'dx_origin'
  ;
  measurement_recordCount := sql%rowcount;
  COMMIT;
     
      DELETE FROM CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DIAGNOSIS';
      COMMIT;

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
    xw.target_concept_id as PROCEDURE_CONCEPT_ID, 
    nvl(d.dx_date,d.admit_date) as PROCEDURE_DATE, 
    nvl(d.dx_date,d.admit_date) as PROCEDURE_DATETIME,
--    0 AS PROCEDURE_TYPE_CONCEPT_ID, -- use this type concept id for ehr order list
--    38000275 AS PROCEDURE_TYPE_CONCEPT_ID, --default values from draft mappings spreadsheet --added 6/26
    NVL(xw2.target_concept_id,0) AS PROCEDURE_TYPE_CONCEPT_ID,
    0 MODIFIER_CONCEPT_ID, -- need to create a cpt_concept_id table based on the source_code_concept id
    null as QUANTITY,
    null as PROVIDER_ID,
    e.n3cds_domain_map_id as VISIT_OCCURRENCE_ID,
    null as VISIT_DETAIL_ID,
    xw.src_code as PROCEDURE_SOURCE_VALUE,
    xw.source_code_concept_id as PROCEDURE_SOURCE_CONCEPT_ID,
    xw.src_code_type as MODIFIER_SOURCE_VALUE,
    'PCORNET_DIAGNOSIS' AS DOMAIN_SOURCE
  FROM NATIVE_PCORNET51_CDM.diagnosis d 
  JOIN CDMH_STAGING.PERSON_CLEAN pc on d.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
  JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= d.DIAGNOSISID AND Mp.Domain_Name='DIAGNOSIS' AND mp.Target_Domain_Id = 'Procedure' AND mp.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=d.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=d.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
  JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on d.dx = xw.src_code  and xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Procedure' 
                  and xw.target_concept_id=mp.target_concept_id and Xw.Src_Code_Type=d.DX_TYPE
    LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON d.dx_origin = xw2.src_code
                                                         AND xw2.cdm_tbl = 'DIAGNOSIS'
                                                         AND xw2.cdm_tbl_column_name = 'dx_origin'
  ;
  proc_recordCount:=Sql%Rowcount;
  COMMIT;

      DELETE FROM CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DIAGNOSIS';
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
  SELECT 
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id, 
    mp.N3cds_Domain_Map_Id AS drug_exposure_id,
    p.N3cds_Domain_Map_Id AS person_id, 
    xw.target_concept_id as drug_concept_id,
     nvl(d.dx_date,d.admit_date) as drug_exposure_start_date, 
     nvl(d.dx_date,d.admit_date) as drug_exposure_start_datetime,
    nvl(d.dx_date,d.admit_date) as drug_exposure_end_date, --need to be revisited
    nvl(d.dx_date,d.admit_date) as drug_exposure_end_datetime, --need to be revisited
    null as verbatim_end_date,
--    581373 as drug_type_concept_id, -- medication administered to patient, from DX_ORIGIN: code 'OD','BI','CL','DR','NI','UN,'OT'
--    case when d.DX_ORIGIN = 'OD' then 38000179
--      when d.DX_ORIGIN = 'BI' then 38000177
--      when d.DX_ORIGIN = 'CL' then 38000177
--    else 45769798 end as drug_type_concept_id, --added on 6/26
    NVL(xw2.target_concept_id,0) AS drug_type_concept_id,
    null as stop_reason,
    null as refills,
    null as quantity,
    null as days_supply,
    null as sig, 
    null as route_concept_id,
    null as lot_number,
    null as provider_id, --m.medadmin_providerid as provider_id,
    e.n3cds_domain_map_id as visit_occurrence_id,
    null as visit_detail_id,
    d.DX as drug_source_value,
    null as drug_source_concept_id, 
    null as route_source_value, 
    null as dose_unit_source_value, 
    'PCORNET_DIAGNOSIS' as DOMAIN_SOURCE
  FROM NATIVE_PCORNET51_CDM.diagnosis d 
  JOIN CDMH_STAGING.PERSON_CLEAN pc on d.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
  JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= d.DIAGNOSISID AND mp.Domain_Name='DIAGNOSIS' AND mp.Target_Domain_Id = 'Drug' AND mp.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=d.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=d.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
  JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on d.dx = xw.src_code  and xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Drug' 
                  and xw.target_concept_id=mp.target_concept_id and Xw.Src_Code_Type=d.DX_TYPE
  LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON d.dx_origin = xw2.src_code
                                                         AND xw2.cdm_tbl = 'DIAGNOSIS'
                                                         AND xw2.cdm_tbl_column_name = 'dx_origin'
  ;
  drug_recordCount:=Sql%Rowcount;
  COMMIT;



RECORDCOUNT := condition_recordCount+observation_recordCount+measurement_recordCount+proc_recordCount+drug_recordCount;
DBMS_OUTPUT.put_line(RECORDCOUNT || ' PCORnet DIAGNOSIS source data inserted to condition staging table, ST_OMOP53_CONDITION_OCCURRENCE, and observation staging table, ST_OMOP53_OBSERVATION, 
and measurement staging table, ST_OMOP53_MEASUREMENT and procedure_occurrence staging table, ST_OMOP53_PROCEDURE_OCCURRENCE, and drug_exposure staging table, ST_OMOP53_DRUG_EXPOSURE successfully.'); 

END SP_P2O_SRC_DIAGNOSIS;
