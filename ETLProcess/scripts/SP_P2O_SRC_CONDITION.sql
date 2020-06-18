/**
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong / Sandeep Naredla 
Description : Stored Procedure to insert PCORnet Condition into staging table
Stored Procedure: SP_P2O_SRC_CONDITION:
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER 
**/

  CREATE OR REPLACE EDITIONABLE PROCEDURE "CDMH_STAGING"."SP_P2O_SRC_CONDITION" 
(
  DATAPARTNERID IN NUMBER,
  MANIFESTID IN NUMBER
) AS 
BEGIN
   --update the following two lines before submitting the sp for CDMH_STAGING 
   --execute immediate 'truncate table CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE';
   --INSERT INTO CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE (
   --DATAPARTNERID := 1000;
    --execute immediate 'truncate table CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE';
    --commit ;
    
   INSERT INTO CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE (  --INSERT INTO CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE
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
    c.report_date as condition_start_date, null as condition_start_datetime, 
    c.resolve_date as condition_end_date, null as condition_end_datetime,
    case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
    else 0 end AS condition_type_concept_id,  --check again
    --NULL as condition_type_concept_id, ---ehr/ip/op/AG? setting of care origin ehr system /insclaim / registry / other sources/check the visit/ encounter type for this
    NULL as stop_reason, ---- encounter discharge type e.discharge type
    null as provider_id, ---is provider linked to patient
    e.N3cds_Domain_Map_Id as visit_occurrence_id, 
    null as visit_detail_id, 
    c.CONDITION AS condition_source_value,
    xw.source_code_concept_id as condition_source_concept_id,
   c.CONDITION_STATUS AS condition_status_source_value,    
    cst.TARGET_CONCEPT_ID AS CONDITION_STATUS_CONCEPT_ID,
    'PCORNET_CONDITION' as DOMAIN_SOURCE
FROM NATIVE_PCORNET51_CDM.CONDITION c
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= c.CONDITIONID 
                AND Mp.Domain_Name='CONDITION' AND mp.Target_Domain_Id = 'Condition' AND mp.DATA_PARTNER_ID=DATAPARTNERID
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=c.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=c.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN cdmh_staging.p2o_term_xwalk csrc on csrc.CDM_TBL='CONDITION' AND csrc.cdm_tbl_column_name='CONDITION_SOURCE' AND csrc.src_code=c.CONDITION_SOURCE
LEFT JOIN cdmh_staging.p2o_term_xwalk cst on cst.CDM_TBL='CONDITION' AND cst.cdm_tbl_column_name='CONDITION_STATUS' AND cst.src_code=c.CONDITION_STATUS
left join NATIVE_PCORNET51_CDM.encounter enc on enc.encounterid = e.source_id and e.domain_name='ENCOUNTER' 
left join cdmh_staging.visit_xwalk enctype on enc.enc_type = enctype.src_visit_type and enctype.CDM_NAME = 'PCORnet' and enctype.CDM_TBL = 'ENCOUNTER' 
LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on c.condition = xw.src_code and xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Condition'
;


DBMS_OUTPUT.put_line('PCORnet CONDITION source data inserted to condition staging table, ST_OMOP53_CONDITION_OCCURRENCE, successfully.'); 
--


commit ;

END SP_P2O_SRC_CONDITION;

/
