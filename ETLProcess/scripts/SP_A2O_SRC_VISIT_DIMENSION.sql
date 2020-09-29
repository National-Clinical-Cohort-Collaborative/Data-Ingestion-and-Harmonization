
CREATE PROCEDURE                           CDMH_STAGING.SP_A2O_SRC_VISIT_DIMENSION  
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 
/*************************************************************************************************************************************
    FileName:   SP_A2O_SRC_VISIT_DIMENSION
    Purpose:     BUILD CODES FOUND IN THE observation_fact table and build the a2o_code_xwalk_standard table with OMOP target concept ids
    Author: Stephanie Hong
    Edit History:
     Ver      Date      Author    Description
     0.1      7/17/20   SHONG       Initial version
     0.2      9/22/20   SNAREDLA    USE visit_dimension_id AS SOURCE ID which is a concatenation of encounter_num and patient_num
**************************************************************************************************************************************/
encCnt number ;

BEGIN
DELETE FROM cdmh_staging.ST_OMOP53_VISIT_OCCURRENCE WHERE data_partner_id=datapartnerid AND  domain_source='I2B2ACT_VISIT_DIMENSION' ;
COMMIT;
INSERT INTO CDMH_STAGING.ST_OMOP53_VISIT_OCCURRENCE (
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
        nvl(vx.target_concept_id, 46237210)  as VISIT_CONCEPT_ID,
        enc.start_date as VISIT_START_DATE,
        enc.start_date as VISIT_START_DATETIME,
        NVL(enc.end_date, enc.start_date) as VISIT_END_DATE,
        NVL(enc.end_DATE, enc.start_date) as VISIT_END_DATETIME,
        -- confirmed this issue:
        ---Stephanie Hong 6/19/2020 -32035 -default to 32035 "Visit derived from EHR encounter record.
        ---case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        ---when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
        ---else 0 end AS VISIT_TYPE_CONCEPT_ID,  --check with SMEs
        32035 as VISIT_TYPE_CONCEPT_ID, ---- where did the record came from / need clarification from SME
        null as PROVIDER_ID,
        null as CARE_SITE_ID, -- CAN WE ADD LOCATION_CD IN visit_dimension as care_site? ssh 7/27/20
        enc.inout_cd as VISIT_SOURCE_VALUE,
        null as VISIT_SOURCE_CONCEPT_ID,  
        null as ADMITTING_SOURCE_CONCEPT_ID, 
        NULL as ADMITTING_SOURCE_VALUE,
        null as DISCHARGE_TO_CONCEPT_ID,
        enc.dsch_disp_cd as DISCHARGE_TO_SOURCE_VALUE,
        null as PRECEDING_VISIT_OCCURRENCE_ID, ---
        'I2B2ACT_VISIT_DIMENSION' as DOMAIN_SOURCE
        FROM NATIVE_I2B2ACT_CDM.visit_dimension enc
        JOIN CDMH_STAGING.PERSON_CLEAN pc on enc.patient_num=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
        JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= enc.visit_dimension_id AND Mp.Domain_Name='VISIT_DIMENSION' AND mp.Target_Domain_Id = 'Visit' AND mp.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=enc.PATIENT_NUM AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.visit_xwalk vx ON vx.cdm_tbl='VISIT_DIMENSION' AND vx.CDM_NAME='I2B2ACT' AND vx.src_visit_type = enc.inout_cd
        ;

    encCnt := sql%rowcount;

    RECORDCOUNT  := encCnt ;
    DBMS_OUTPUT.put_line(RECORDCOUNT || '  I2B2ACT VISIT_DIMENSION source data inserted to VISIT_OCCURRENCE staging table, ST_OMOP53_VISIT_OCCURRENCE successfully.');

 COMMIT;


END SP_A2O_SRC_VISIT_DIMENSION;
