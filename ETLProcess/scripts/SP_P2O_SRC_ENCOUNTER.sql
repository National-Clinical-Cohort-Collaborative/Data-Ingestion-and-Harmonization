
/*******************************************************************************************************************************
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong / Sandeep Naredla
Description : Stored Procedure to insert PCORnet Encounter into staging table ST_OMOP53_VISIT_OCCURRENCE, and ST_OMOP53_CARE_SITE 
Stored Procedure: SP_P2O_SRC_ENCOUNTER:
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER, RECORDCOUNT OUT NUMBER
Edit History::
6/16/2020   SHONG Initial Version

********************************************************************************************************************************/
CREATE PROCEDURE                CDMH_STAGING.SP_P2O_SRC_ENCOUNTER 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 
encCnt number ;
careSiteCnt number;
BEGIN

INSERT INTO ST_OMOP53_VISIT_OCCURRENCE (
    DATA_PARTNER_ID
    ,MANIFEST_ID
    ,VISIT_OCCURRENCE_ID
    ,PERSON_ID
    ,VISIT_CONCEPT_ID
    ,VISIT_START_DATE
    ,VISIT_START_DATETIME
    ,VISIT_END_DATE
    ,VISIT_END_DATETIME
    ,VISIT_TYPE_CONCEPT_ID
    ,PROVIDER_ID
    ,CARE_SITE_ID
    ,VISIT_SOURCE_VALUE
    ,VISIT_SOURCE_CONCEPT_ID
    ,ADMITTING_SOURCE_CONCEPT_ID
    ,ADMITTING_SOURCE_VALUE
    ,DISCHARGE_TO_CONCEPT_ID
    ,DISCHARGE_TO_SOURCE_VALUE
    ,PRECEDING_VISIT_OCCURRENCE_ID
    ,DOMAIN_SOURCE
    )
    SELECT 
    DATAPARTNERID as DATA_PARTNER_ID,
    MANIFESTID as MANIFEST_ID,
        mp.n3cds_domain_map_id as VISIT_OCCURRENCE_ID,
        p.n3cds_domain_map_id as PERSON_ID,
        vx.target_concept_id as VISIT_CONCEPT_ID,
        enc.admit_date as VISIT_START_DATE,
        enc.admit_date as VISIT_START_DATETIME,
        NVL(enc.DISCHARGE_DATE, enc.admit_date) as VISIT_END_DATE,
        NVL(enc.DISCHARGE_DATE, enc.admit_date) as VISIT_END_DATETIME,
        -- confirmed this issue:
        ---Stephanie Hong 6/19/2020 -32035 -default to 32035 "Visit derived from EHR encounter record.
        ---case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        ---when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
        ---else 0 end AS VISIT_TYPE_CONCEPT_ID,  --check with SMEs
        32035 as VISIT_TYPE_CONCEPT_ID, ---- where did the record came from / need clarification from SME
        prv.n3cds_domain_map_id as PROVIDER_ID,
        cs.n3cds_domain_map_id as CARE_SITE_ID,
        enc.enc_type as VISIT_SOURCE_VALUE,
        null as VISIT_SOURCE_CONCEPT_ID,  
        vsrc.target_concept_id as ADMITTING_SOURCE_CONCEPT_ID, 
        enc.admitting_source as ADMITTING_SOURCE_VALUE,
        disp.target_concept_id as DISCHARGE_TO_CONCEPT_ID,
        enc.discharge_status as DISCHARGE_TO_SOURCE_VALUE,
        null as PRECEDING_VISIT_OCCURRENCE_ID, ---
        'PCORNET_ENCOUNTER' as DOMAIN_SOURCE
        FROM NATIVE_PCORNET51_CDM.ENCOUNTER enc
        JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= enc.encounterid AND Mp.Domain_Name='ENCOUNTER' AND mp.Target_Domain_Id = 'Visit' AND mp.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=enc.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map prv ON prv.source_id=enc.providerid and prv.domain_name='PROVIDER' AND prv.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map cs on cs.Source_Id=enc.encounterid AND cs.Domain_Name='ENCOUNTER' AND cs.Target_Domain_Id = 'Care_Site' 
        LEFT JOIN CDMH_STAGING.visit_xwalk vx ON vx.cdm_tbl='ENCOUNTER' AND vx.CDM_NAME='PCORnet' AND vx.src_visit_type=enc.ENC_TYPE
        LEFT JOIN CDMH_STAGING.p2o_admitting_source_xwalk vsrc ON vx.cdm_tbl='ENCOUNTER' AND vx.CDM_NAME='PCORnet' AND vsrc.src_admitting_source_type=enc.admitting_source 
        LEFT JOIN CDMH_STAGING.p2o_discharge_status_xwalk disp on disp.cdm_tbl='ENCOUNTER' AND disp.CDM_SOURCE='PCORnet' AND disp.src_discharge_status =enc.discharge_status 
        ;

    encCnt := sql%rowcount;

    ---encounter to care_site
    INSERT INTO CDMH_STAGING.ST_OMOP53_CARE_SITE 
        (
          DATA_PARTNER_ID
        , MANIFEST_ID 
        , CARE_SITE_NAME 
        , PLACE_OF_SERVICE_CONCEPT_ID  
        , LOCATION_ID 
        , CARE_SITE_SOURCE_VALUE 
        , PLACE_OF_SERVICE_SOURCE_VALUE  
        , DOMAIN_SOURCE
        ) --8
        SELECT 
        DATAPARTNERID AS DATA_PARTNER_ID, 
        MANIFESTID AS MANIFEST_ID,  
        NULL AS care_site_name, 
        mp.target_concept_id AS place_of_service_concept_id,
        NULL as location_id, 
        enc.FACILITY_TYPE AS care_site_source_value,   
        enc.enc_type AS PLACE_OF_SERVICE_SOURCE_VALUE,  -- ehr/encounter
        'PCORNET_ENCOUNTER' AS DOMAIN_SOURCE
        from NATIVE_PCORNET51_CDM.ENCOUNTER enc
        JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id=enc.ENCOUNTERID AND Mp.Domain_Name='ENCOUNTER' AND Mp.Target_Domain_Id='Care_Site' AND mp.DATA_PARTNER_ID=1000
        LEFT JOIN CDMH_STAGING.p2o_faciity_type_xwalk fx ON fx.cdm_tbl='ENCOUNTER' AND fx.CDM_SOURCE='PCORnet' AND fx.src_facility_type=enc.facility_type 
        ;


    careSiteCnt  := sql%rowcount;
    RECORDCOUNT  := encCnt + careSiteCnt;
    DBMS_OUTPUT.put_line(RECORDCOUNT || '  PCORnet ENCOUNTER source data inserted to ENCOUNTER staging table, ST_OMOP53_VISIT_OCCURRENCE, and ST_OMOP53_CARE_SITE if facility type is not null,  successfully.');

 COMMIT;

END SP_P2O_SRC_ENCOUNTER;
