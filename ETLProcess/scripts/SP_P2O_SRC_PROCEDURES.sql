
 /**
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong 
Description : Stored Procedure to insert PCORnet PROCEDURES into staging table
Stored Procedure: SP_P2O_SRC_PROCEDURES:
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER 
**/
  CREATE OR REPLACE EDITIONABLE PROCEDURE "CDMH_STAGING"."SP_P2O_SRC_PROCEDURES" 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
) AS 
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
    WHEN pr.px_source ='UN' THEN 45877986
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
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on pr.px = xw.src_code  and xw.CDM_TBL = 'PROCEDURES' AND xw.target_domain_id = 'Procedure'
  ;
  
DBMS_OUTPUT.put_line('PCORnet PROCEDURE source data inserted to PROCEDURE staging table, ST_OMOP53_PROCEDURE_OCCURRENCE, successfully.'); 

END SP_P2O_SRC_PROCEDURES;

/
