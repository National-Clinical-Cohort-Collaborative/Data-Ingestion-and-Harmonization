
CREATE PROCEDURE                                                       CDMH_STAGING.SP_P2O_SRC_MED_ADMIN 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_MED_ADMIN
     Purpose:    Loading The NATIVE_PCORNET51_CDM.MED_ADMIN Table into 
                1. CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
                2. CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1       5/16/2020     SHONG Initial Version
     0.2       6/25/2020       RZHU             Added logic to insert into CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE
     0.3       9/9/2020        DIH              Updated the *_type_concept_id logic
*********************************************************************************************************/

drug_recordCount number;
device_recordCount number;
BEGIN
      DELETE FROM CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORENT_MED_ADMIN';
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
    m.medadmin_start_date as drug_exposure_start_date, 
    m.medadmin_start_date as drug_exposure_start_datetime,
    -- NVL2(m.medadmin_start_date,TO_DATE(TO_CHAR(m.medadmin_start_date, 'DD-MON-YYYY') ||' '|| m.medadmin_start_time, 'DD-MON-YYYY HH24MISS'),null) as drug_exposure_start_datetime, 
    NVL(m.medadmin_stop_date, m.medadmin_start_date ) as drug_exposure_end_date,
    null as drug_exposure_end_datetime, -- NVL2((isNull(m.medadmin_stop_date) or isNull(m.medadmin_stop_date)),TO_DATE(TO_CHAR(m.medadmin_stop_date, 'DD-MON-YYYY') ||' '|| m.medadmin_stop_time, 'DD-MON-YYYY HH24MISS'),null) as drug_exposure_end_datetime,
    null as verbatim_end_date,
--    581373 as drug_type_concept_id, --m medication administered to patient 
    32817 as drug_type_concept_id,
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
    JOIN CDMH_STAGING.PERSON_CLEAN pc on m.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= m.medadminid 
                AND mp.Domain_Name='MED_ADMIN' AND mp.Target_Domain_Id = 'Drug' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=m.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on m.medadmin_code = xw.src_code  and xw.CDM_TBL = 'MED_ADMIN' AND xw.target_domain_id = 'Drug'  
                                                        AND xw.target_concept_id=mp.target_concept_id
                                                        and xw.Src_Code_Type=m.MEDADMIN_TYPE--added on 6/28
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=m.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk r on m.medadmin_route = r.src_code and r.src_cdm_column = 'RX_ROUTE'
;
Drug_Recordcount:=Sql%Rowcount;
COMMIT;
      DELETE FROM CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORENT_MED_ADMIN';
      COMMIT;
    INSERT INTO CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE (
        data_partner_id,
        manifest_id,
        device_exposure_id,
        person_id,
        device_concept_id,
        device_exposure_start_date,
        device_exposure_start_datetime,
        device_exposure_end_date,
        device_exposure_end_datetime,
        device_type_concept_id,
        unique_device_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        device_source_value,
        device_source_concept_id,
        domain_source
    )
    SELECT 
        DATAPARTNERID as data_partner_id,
        MANIFESTID as manifest_id,
        mp.N3cds_Domain_Map_Id AS device_exposure_id,
        p.N3cds_Domain_Map_Id AS person_id,
        xw.target_concept_id as device_concept_id,
        m.MEDADMIN_START_DATE as device_exposure_start_date,
        m.MEDADMIN_START_DATE as device_exposure_start_datetime,
        null as device_exposure_end_date,
        null as device_exposure_end_datetime,
--        44818707 as device_type_concept_id, -- default values from draft mappings spreadsheet
        32817 as device_type_concept_id,
        null as unique_device_id, 
        null as quantity, 
        null as provider_id,
        e.N3cds_Domain_Map_Id as visit_occurrence_id,
        null as visit_detail_id,
        m.MEDADMIN_CODE as device_source_value, 
        xw.SOURCE_CODE_CONCEPT_ID as device_source_concept_id, 
        'PCORENT_MED_ADMIN' as domain_source
    FROM NATIVE_PCORNET51_CDM.MED_ADMIN m
    JOIN CDMH_STAGING.PERSON_CLEAN pc on m.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= m.medadminid
                AND mp.Domain_Name='MED_ADMIN' AND mp.Target_Domain_Id = 'Device' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=m.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=m.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
    JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on m.medadmin_code = xw.src_code and xw.CDM_TBL = 'MED_ADMIN' 
                        and xw.Target_Concept_Id=Mp.Target_Concept_Id and xw.Src_Code_Type=m.MEDADMIN_TYPE
    ;
    device_recordCount:=Sql%Rowcount;
    COMMIT;

Recordcount:=Drug_Recordcount+device_recordCount;

DBMS_OUTPUT.put_line(Recordcount || ' PCORnet MED_ADMIN source data inserted to DRUG_EXPOSURE staging table, ST_OMOP53_DRUG_EXPOSURE,
                                                                                Device_Expsoure staging table, ST_OMOP53_DEVICE_EXPOSURE, successfully.'); 


END SP_P2O_SRC_MED_ADMIN;
