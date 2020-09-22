
CREATE PROCEDURE                                                                    CDMH_STAGING.SP_P2O_SRC_CONDITION 
(
  DATAPARTNERID IN NUMBER,
  MANIFESTID IN NUMBER,
  RECORDCOUNT OUT NUMBER
) AS 
/********************************************************************************************************
project : N3C DI&H
     Name:      SP_P2O_SRC_CONDITION
     Purpose:    Loading The NATIVE_PCORNET51_CDM.CONDITION Table into 
                1. CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE
                2. CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE
                3. CDMH_STAGING.ST_OMOP53_OBSERVATION
                4. CDMH_STAGING.ST_OMOP53_MEASUREMENT
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1         5/16/2020 SHONG Initial Version
	   0.2       6/25/2020     SHONG              Added logic to insert into CDMH_STAGING.ST_OMOP53_MEASUREMENT
     0.3       6/26/2020     SHONG              Added logic to insert into CDMH_STAGING.ST_OMOP53_DRUG_EXPSOURE
     0.4       6/28/2020     SNAREDLA           Updated the condition for CDMH_STAGING.ST_OMOP53_DRUG_EXPSOURE.DRUG_EXPOSURE_END_DATE
     0.5       9/8/2020      DIH                Updated the condition_type_concept_id logic
     0.6       9/9/2020      DIH                Updated the *_type_concept_id logic
*********************************************************************************************************/
condition_recordCount number;
observation_recordCount number;
procedure_recordCount number;
measure_recordCount number;
drug_recordCount number ;
BEGIN
   DELETE FROM CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_CONDITION';
   COMMIT;
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
    xw.target_concept_id as condition_concept_id, 
    c.report_date as condition_start_date, null as condition_start_datetime, 
    c.resolve_date as condition_end_date, null as condition_end_datetime,
--    case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
--        when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
--    else 0 end AS condition_type_concept_id,  --check again
    32817 as condition_type_concept_id,
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
JOIN CDMH_STAGING.PERSON_CLEAN pc on c.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= c.CONDITIONID 
                AND Mp.Domain_Name='CONDITION' AND mp.Target_Domain_Id = 'Condition' AND mp.DATA_PARTNER_ID=DATAPARTNERID
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=c.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=c.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN cdmh_staging.p2o_term_xwalk csrc on csrc.CDM_TBL='CONDITION' AND csrc.cdm_tbl_column_name='CONDITION_SOURCE' AND csrc.src_code=c.CONDITION_SOURCE
LEFT JOIN cdmh_staging.p2o_term_xwalk cst on cst.CDM_TBL='CONDITION' AND cst.cdm_tbl_column_name='CONDITION_STATUS' AND cst.src_code=c.CONDITION_STATUS
left join NATIVE_PCORNET51_CDM.encounter enc on enc.encounterid = e.source_id and e.domain_name='ENCOUNTER' 
--left join cdmh_staging.visit_xwalk enctype on enc.enc_type = enctype.src_visit_type and enctype.CDM_NAME = 'PCORnet' and enctype.CDM_TBL = 'ENCOUNTER' 
JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on c.condition = xw.src_code and xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Condition'
                                                                              and xw.target_concept_id=mp.target_concept_id
                                                                              and Xw.Src_Code_Type=c.condition_type
;
condition_recordCount:=SQL%ROWCOUNT;
COMMIT;
   DELETE FROM CDMH_STAGING.ST_OMOP53_OBSERVATION WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_CONDITION';
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
        ,DOMAIN_SOURCE )

        --condition to observation

        select distinct
            DATAPARTNERID as DATA_PARTNER_ID,
            MANIFESTID as MANIFEST_ID,
            mp.n3cds_domain_map_id as OBSERVATION_ID ,
            p.n3cds_domain_map_id as PERSON_ID,
            xw.target_concept_id as OBSERVATION_CONCEPT_ID,
            c.report_date as OBSERVATION_DATE,
            c.report_date as OBSERVATION_DATETIME,
            --HC/NI/OT/PC/PR/RG/UN/DR

--           case when c.condition_source = 'HC' then 38000245
--                when c.condition_source ='PR' then 45905770
--                when c.condition_source = 'NI' then 46237210
--                when c.condition_source = 'OT' then 45878142
--                when c.condition_source = 'UN' then 45877986  --Unknown (concept_id = 45877986)
--                when c.condition_source in ('RG', 'DR', 'PC') then 0 ---PC    PC=PCORnet-defined  condition  algorithm       See mapping comments
--                else 0
--                end as OBSERVATION_TYPE_CONCEPT_ID,
            32817 as OBSERVATION_TYPE_CONCEPT_ID,
            0 VALUE_AS_NUMBER,
            0 as VALUE_AS_STRING,
            xw.source_code_concept_id as VALUE_AS_CONCEPT_ID ,
            0 as QUALIFIER_CONCEPT_ID,
            0 as UNIT_CONCEPT_ID,
            Null as PROVIDER_ID,
            e.n3cds_domain_map_id as VISIT_OCCURRENCE_ID,
            Null  as VISIT_DETAIL_ID,
            c.condition AS OBSERVATION_SOURCE_VALUE,
            xw.source_code_concept_id as OBSERVATION_SOURCE_CONCEPT_ID,
            Null as UNIT_SOURCE_VALUE,
            null as QUALIFIER_SOURCE_VALUE,
            'PCORNET_CONDITION' DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.CONDITION c
    JOIN CDMH_STAGING.PERSON_CLEAN pc on c.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
    JOIN cdmh_staging.N3cds_Domain_Map mp on mp.Source_Id= c.CONDITIONID AND mp.Domain_Name='CONDITION' AND mp.Target_Domain_Id = 'Observation' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN cdmh_staging.N3cds_Domain_Map p on p.Source_Id=c.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN cdmh_staging.N3cds_Domain_Map e on e.Source_Id=c.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
    JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on c.condition = xw.src_code and xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Observation'  
                                        and mp.target_concept_id=xw.target_concept_id
                                        and Xw.Src_Code_Type=c.condition_type
 ;
observation_recordCount:= sql%rowcount;
COMMIT;
   DELETE FROM CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_CONDITION';
   COMMIT;
   --insert condition to procedure
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
    DOMAIN_SOURCE
    )
    SELECT 
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS PROCEDURE_OCCURRENCE_ID,
    p.N3cds_Domain_Map_Id AS person_id,  
    xw.target_concept_id as PROCEDURE_CONCEPT_ID,
    c.report_date as PROCEDURE_DATE,
    c.report_date as PROCEDURE_DATETIME,
--    case when c.condition_source = 'HC' then 38000245 --HC=Healthcare  problem  list/EHR problem list entry
--                when c.condition_source ='PR' then 45905770 --PR : patient reported
--                when c.condition_source = 'NI' then 46237210
--                when c.condition_source = 'OT' then 45878142
--                when c.condition_source = 'UN' then 0 --charles mapping doc ref. Unknown (concept_id = 45877986/charles mapping doc ref. Unknown (concept_id = 45877986)
--                when c.condition_source in ('RG', 'DR', 'PC') then 0 ---PC    PC=PCORnet-defined  condition  algorithm       See mapping comments,
--                ELSE 0
--                END AS PROCEDURE_TYPE_CONCEPT_ID, -- use this type concept id for ehr order list
    32817 as PROCEDURE_TYPE_CONCEPT_ID,
    0 MODIFIER_CONCEPT_ID, -- need to create a cpt_concept_id table based on the source_code_concept id
    null as QUANTITY,
    null as PROVIDER_ID,
    e.n3cds_domain_map_id as VISIT_OCCURRENCE_ID,
    null as VISIT_DETAIL_ID,
    c.condition as PROCEDURE_SOURCE_VALUE,
    xw.source_code_concept_id as PROCEDURE_SOURCE_CONCEPT_ID,
    xw.src_code_type as MODIFIER_SOURCE_VALUE,
    'PCORNET_CONDITION' AS DOMAIN_SOURCE
    FROM NATIVE_PCORNET51_CDM.CONDITION c
    JOIN CDMH_STAGING.PERSON_CLEAN pc on c.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
    JOIN cdmh_staging.N3cds_Domain_Map mp on mp.Source_Id= c.CONDITIONID AND mp.Domain_Name='CONDITION' AND mp.Target_Domain_Id = 'Procedure' AND mp.DATA_PARTNER_ID=DATAPARTNERID
    LEFT JOIN cdmh_staging.N3cds_Domain_Map p on p.Source_Id=c.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID= DATAPARTNERID
    LEFT JOIN cdmh_staging.N3cds_Domain_Map e on e.Source_Id=c.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID= DATAPARTNERID
    JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on c.condition = xw.src_code and xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Procedure'
                                                       and xw.target_concept_id=mp.target_concept_id
                                                       and Xw.Src_Code_Type=c.condition_type
                                                       ;
    procedure_recordCount:=sql%rowcount;
    COMMIT;
    DELETE FROM CDMH_STAGING.ST_OMOP53_MEASUREMENT WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_CONDITION';
    COMMIT;   
    INSERT INTO CDMH_STAGING.ST_OMOP53_MEASUREMENT (
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
            xw.TARGET_CONCEPT_ID  as measurement_concept_id,
            c.report_date as measurement_date,
            c.report_date as measurement_datetime,
            null as measurement_time,
--            case when c.condition_source = 'HC' then 38000245 --HC=Healthcare  problem  list/EHR problem list entry
--                 when c.condition_source ='PR' then 45905770 --PR : patient reported
--                 when c.condition_source = 'NI' then 46237210
--                 when c.condition_source = 'OT' then 45878142
--                 when c.condition_source = 'UN' then 0 --charles mapping doc ref. Unknown (concept_id = 45877986/charles mapping doc ref. Unknown (concept_id = 45877986)
--                 when c.condition_source in ('RG', 'DR', 'PC') then 0 ---PC    PC=PCORnet-defined  condition  algorithm       See mapping comments,
--                 ELSE 0
--                 END AS measurement_type_concept_id,
            32817 AS measurement_type_concept_id,
            NULL as OPERATOR_CONCEPT_ID,
            null as VALUE_AS_NUMBER, --result_num
            NULL as VALUE_AS_CONCEPT_ID,
            NULL as UNIT_CONCEPT_ID,
            NULL as RANGE_LOW,
            NULL as RANGE_HIGH,
            NULL AS PROVIDER_ID,
            e.N3cds_Domain_Map_Id as VISIT_OCCURRENCE_ID,
            NULL as VISIT_DETAIL_ID,
            c.condition as MEASUREMENT_SOURCE_VALUE,
            xw.source_code_concept_id as MEASUREMENT_SOURCE_CONCEPT_ID,
            NULL as UNIT_SOURCE_VALUE,
            NULL as VALUE_SOURCE_VALUE,
            'PCORNET_CONDITION' as DOMAIN_SOURCE
        FROM NATIVE_PCORNET51_CDM.CONDITION c
        JOIN CDMH_STAGING.PERSON_CLEAN pc on c.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
        JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id= c.CONDITIONID AND Mp.Domain_Name='CONDITION' AND mp.Target_Domain_Id = 'Measurement' AND mp.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=c.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=c.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
--        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map prv on prv.Source_Id=e.source_id.providerid AND prv.Domain_Name='PROVIDER' AND prv.DATA_PARTNER_ID=2000
        JOIN CDMH_STAGING.P2O_CODE_XWALK_STANDARD xw on c.condition = xw.src_code and xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Measurement'
                                                            and xw.target_concept_id=mp.target_concept_id 
                                                            and Xw.Src_Code_Type=c.condition_type

;
measure_recordCount:=SQL%ROWCOUNT;
COMMIT;

    DELETE FROM CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_CONDITION';
    COMMIT;
--Condition to Drug target - map drug_type_concept_id  use CONDITION_SOURCE.

INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE (
    data_partner_id,
    manifest_id,
    drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
	drug_exposure_start_datetime,
    drug_exposure_end_date,
	drug_exposure_end_datetime,
    verbatim_end_date,
	drug_type_concept_id,
    stop_reason,refills,
	quantity,
	days_supply,
	sig,
    route_concept_id,
    lot_number,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value,
    DOMAIN_SOURCE
	)

SELECT
    DATAPARTNERID as data_partner_id,
    MANIFESTID as manifest_id,
    mp.N3cds_Domain_Map_Id AS drug_exposure_id,
    p.N3cds_Domain_Map_Id AS person_id,
    xw.target_concept_id as drug_concept_id,
    c.REPORT_DATE as drug_exposure_start_date,
    c.REPORT_DATE as drug_exposure_start_datetime,
    NVL(c.RESOLVE_DATE,c.REPORT_DATE ) as drug_exposure_end_date,--sn 6/28/2020
    null as drug_exposure_end_datetime,
    null as verbatim_end_date,
--    45769798 as drug_type_concept_id, --ssh 6/25/2020 --issue number 54
    32817 as drug_type_concept_id, 
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
    c.CONDITION as drug_source_value,
    xw.source_code_concept_id as drug_source_concept_id,
    null as route_source_value,
    null as dose_unit_source_value,
    'PCORNET_CONDITION' as DOMAIN_SOURCE
  FROM NATIVE_PCORNET51_CDM.CONDITION c
  JOIN CDMH_STAGING.PERSON_CLEAN pc on c.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
  JOIN CDMH_STAGING.N3cds_Domain_Map mp on mp.Source_Id= c.CONDITIONID AND mp.Domain_Name='CONDITION' AND mp.Target_Domain_Id = 'Drug' AND mp.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=c.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.N3cds_Domain_Map e on e.Source_Id=c.ENCOUNTERID AND e.Domain_Name='ENCOUNTER' and e.target_domain_id ='Visit' AND e.DATA_PARTNER_ID=DATAPARTNERID
  JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on c.condition = xw.src_code and xw.CDM_TBL='CONDITION' 
														AND xw.target_domain_id = 'Drug'
														and xw.target_concept_id=mp.target_concept_id 
														and xw.Src_Code_Type=c.condition_type

  ;

drug_recordCount :=SQL%ROWCOUNT;

Commit ;

RECORDCOUNT  := observation_recordCount + condition_recordCount+procedure_recordCount+measure_recordCount+drug_recordCount;
DBMS_OUTPUT.put_line(RECORDCOUNT ||' PCORnet CONDITION source data inserted to condition staging table - ST_OMOP53_CONDITION_OCCURRENCE 
                                                                               observation staging table - ST_OMOP53_OBSERVATION 
                                                                               procedure staging table - ST_OMOP53_PROCEDURE_OCCURRENCE
                                                                               drug staging table - ST_OMOP53_DRUG_EXPOSURE
                                                                               , successfully.'); 
--


END SP_P2O_SRC_CONDITION;
