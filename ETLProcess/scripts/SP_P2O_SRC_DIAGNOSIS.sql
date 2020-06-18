/**
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong 
Description : Stored Procedure to insert PCORnet DIAGNOSIS into staging table
Stored Procedure: SP_P2O_SRC_DIAGNOSIS:
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER 
**/
  CREATE OR REPLACE EDITIONABLE PROCEDURE "CDMH_STAGING"."SP_P2O_SRC_DIAGNOSIS" 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
) AS 
BEGIN
--DIAGNOSIS	Observation	1898
--DIAGNOSIS	Procedure	3
--DIAGNOSIS	Measurement	40
--DIAGNOSIS	Drug	1
--DIAGNOSIS	Condition	23670
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
    mp.target_concept_id as condition_concept_id, 
    d.dx_date as condition_start_date,
    null as condition_start_datetime,
    null as condition_end_date, null as condition_end_datetime,
    vx.target_concept_id AS condition_type_concept_id, --already collected fro the visit_occurrence_table. / visit_occurrence.visit_source_value 
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
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= d.DIAGNOSISID AND Mp.Domain_Name='DIAGNOSIS' AND mp.Target_Domain_Id = 'Condition' AND mp.DATA_PARTNER_ID=DATAPARTNERID
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=d.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=d.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN CDMH_STAGING.visit_xwalk vx ON vx.cdm_tbl='ENCOUNTER' AND vx.CDM_NAME='PCORnet' AND vx.src_visit_type=d.ENC_TYPE
LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on d.dx = xw.src_code  and xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Condition'
;

DBMS_OUTPUT.put_line('PCORnet DIAGNOSIS source data inserted to condition staging table, ST_OMOP53_CONDITION_OCCURRENCE, successfully.'); 

END SP_P2O_SRC_DIAGNOSIS;

/
