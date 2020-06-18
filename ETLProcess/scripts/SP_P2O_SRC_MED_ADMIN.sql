/**
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong / Sandeep Naredla / Richard Zhu / Tanner Zhang
Description : Stored Procedure to insert PCORnet med_admin into staging table
Stored Procedure: SP_P2O_SRC_MED_ADMIN:
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER 
**/



  CREATE OR REPLACE EDITIONABLE PROCEDURE "CDMH_STAGING"."SP_P2O_SRC_MED_ADMIN" 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
) AS 
BEGIN
    --update the following two lines before submitting the sp for CDMH_STAGING 
    --execute immediate 'truncate table CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE';
    --INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE (
    --DATAPARTNERID := 1000;
    --MANIFESTID = 9
    --execute immediate 'truncate table CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE';
    
    INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE (  --INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
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
    mp.target_concept_id as drug_concept_id,
    m.medadmin_start_date as drug_exposure_start_date, 
    m.medadmin_start_date as drug_exposure_start_datetime,
    -- NVL2(m.medadmin_start_date,TO_DATE(TO_CHAR(m.medadmin_start_date, 'DD-MON-YYYY') ||' '|| m.medadmin_start_time, 'DD-MON-YYYY HH24MISS'),null) as drug_exposure_start_datetime, 
    NVL(m.medadmin_stop_date, m.medadmin_start_date ) as drug_exposure_end_date,
    null as drug_exposure_end_datetime, -- NVL2((isNull(m.medadmin_stop_date) or isNull(m.medadmin_stop_date)),TO_DATE(TO_CHAR(m.medadmin_stop_date, 'DD-MON-YYYY') ||' '|| m.medadmin_stop_time, 'DD-MON-YYYY HH24MISS'),null) as drug_exposure_end_datetime,
    null as verbatim_end_date,
    581373 as drug_type_concept_id, --m medication administered to patient 
    null as stop_reason,
    null as refills,
    null as quantity,
    null as days_supply,
    xw.source_code_description as sig,
    r.target_concept_id as route_concept_id,
    null as lot_number,
    null as provider_id, --m.medadmin_providerid as provider_id,
    e.n3cds_domain_map_id as visit_occurrence_id,
    null as visit_detail_id,
    m.medadmin_code as drug_source_value,
    xw.source_code_concept_id as drug_source_concept_id,
    m.medadmin_route as route_source_value,
    m.medadmin_dose_admin_unit as dose_unit_source_value,
    'PCORENT_MED_ADMIN' as DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.MED_ADMIN m
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= m.medadminid 
                AND mp.Domain_Name='MED_ADMIN' AND mp.Target_Domain_Id = 'Drug' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=m.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on m.medadmin_code = xw.src_code  and xw.CDM_TBL = 'MED_ADMIN' AND xw.target_domain_id = 'Drug'
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=m.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk r on m.medadmin_route = r.src_code and r.src_cdm_column = 'RX_ROUTE'
;

  
DBMS_OUTPUT.put_line('PCORnet MED_ADMIN source data inserted to DRUG_EXPOSURE staging table, ST_OMOP53_DRUG_EXPOSURE, successfully.'); 


END SP_P2O_SRC_MED_ADMIN;

/
