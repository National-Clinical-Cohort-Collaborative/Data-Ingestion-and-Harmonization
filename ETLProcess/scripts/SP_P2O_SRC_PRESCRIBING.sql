/***********************************************************************************************************
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
FILE:           SP_P2O_SRC_PRESCRIBING.sql
Description :   Loading NATIVE_PCORNET51_CDM.PRESCRIBING table into stging table CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
Procedure:      SP_P2O_SRC_PRESCRIBING
Edit History:
     Ver       Date         Author          Description
     0.1       5/16/2020    SHONG           Initial version
 
*************************************************************************************************************/
CREATE PROCEDURE                CDMH_STAGING.SP_P2O_SRC_PRESCRIBING 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 
BEGIN
    --insert into 
    INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE (  --INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
    data_partner_id, manifest_id,
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
    DOMAIN_SOURCE) --26
  --PRESCRIBING
    SELECT 
    DATAPARTNERID AS DATAPARTNERID, MANIFESTID AS MANIFESTID, 
    mp.N3cds_domain_map_id as drug_exposure_id, 
    p.N3cds_domain_map_id as person_id, 
    mp.target_concept_id drug_concept_id,
    nvl(rx_start_date, rx_order_date) as drug_exposure_start_date, nvl(rx_start_date,rx_order_date) as drug_exposure_start_datetime, 
    nvl(rx_end_date, rx_order_date) as drug_exposure_end_date, nvl(rx_end_date,rx_order_date) as drug_exposure_end_datetime,
    pr.rx_end_date as verbatim_end_date,
    38000177 as drug_type_concept_id,
    null as stop_reason, null as refills, null as quantity, 
    RX_DAYS_SUPPLY as days_supply, RX_FREQUENCY as sig,  
    mx.TARGET_CONCEPT_ID as route_concept_id,
    null as lot_number,
    prv.N3cds_domain_map_id as provider_id, 
    e.N3cds_domain_map_id as visit_occurrence_id, 
    null as visit_detail_id,
    RXNORM_CUI as drug_source_value, 
    xw.source_code_concept_id as drug_source_concept_id, --- drug source concept id if it is prescribing
    RX_ROUTE as route_source_value,
    rx_dose_ordered_unit as dose_unit_source_value,
    'PCORNET51_PRESCRIBING' as DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.PRESCRIBING pr
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on pr.PRESCRIBINGID=Mp.Source_Id AND mp.DOMAIN_NAME='PRESCRIBING' AND Mp.target_domain_id='Drug' AND mp.DATA_PARTNER_ID=DATAPARTNERID   
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pr.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=pr.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' AND e.Target_Domain_Id = 'Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map prv on prv.Source_Id=pr.RX_PROVIDERID AND prv.Domain_Name='PROVIDER' AND prv.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on rxnorm_cui= xw.src_code  and xw.CDM_TBL = 'PRESCRIBING' AND xw.target_domain_id = 'Drug' and xw.target_concept_id=mp.target_concept_id
    LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk mx on src_cdm_column='RX_ROUTE' AND mx.src_code=pr.RX_ROUTE
    LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on pr.rx_dose_ordered_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT';
    RECORDCOUNT :=Sql%Rowcount;
    COMMIT;

    DBMS_OUTPUT.put_line(RECORDCOUNT || ' PCORnet PRESCRIBING source data inserted to DRUG_EXPOSURE staging table, ST_OMOP53_DRUG_EXPOSURE, successfully.'); 



END SP_P2O_SRC_PRESCRIBING;
