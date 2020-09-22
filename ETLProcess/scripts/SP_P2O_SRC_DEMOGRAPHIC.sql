
CREATE PROCEDURE                             CDMH_STAGING.SP_P2O_SRC_DEMOGRAPHIC 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER 
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_CONDITION
     Purpose:    Loading The NATIVE_PCORNET51_CDM.SP_P2O_SRC_DEMOGRAPHIC Table into 
                1. CDMH_STAGING.ST_OMOP53_PERSON
                2. CDMH_STAGING.ST_OMOP53_OBSERVATION
                3. CDMH_STAGING.ST_OMOP53_OBSERVATION_PERIOD
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1       5/16/2020    SHONG Initial Version
     0.2       6/26/2020     SNAREDLA           Removed UNION ALL and added commit after each block
     0.3       7/8/2020     SNAREDLA           Updated race_concept_id logic when race=06
     0.4       7/24/2020     DI&H               Added logic to create OBSERVATION_PERIOD records

*********************************************************************************************************/
    personCnt number;
    observationCnt1 number;
    observationCnt2 number;
    observationCnt3 number;
    observationPdCnt4 number;

BEGIN
   DELETE FROM CDMH_STAGING.ST_OMOP53_PERSON WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DEMOGRAPHIC';
   COMMIT;
    Insert into CDMH_STAGING.ST_OMOP53_PERSON (
        DATA_PARTNER_ID
        ,MANIFEST_ID
        ,PERSON_ID
        ,GENDER_CONCEPT_ID
        ,YEAR_OF_BIRTH
        ,MONTH_OF_BIRTH
        ,DAY_OF_BIRTH
        ,BIRTH_DATETIME
        ,RACE_CONCEPT_ID
        ,ETHNICITY_CONCEPT_ID
        ,LOCATION_ID
        ,PROVIDER_ID
        ,CARE_SITE_ID
        ,PERSON_SOURCE_VALUE
        ,GENDER_SOURCE_VALUE
        ,GENDER_SOURCE_CONCEPT_ID
        ,RACE_SOURCE_VALUE
        ,RACE_SOURCE_CONCEPT_ID
        ,ETHNICITY_SOURCE_VALUE
        ,ETHNICITY_SOURCE_CONCEPT_ID
        ,DOMAIN_SOURCE ) 
        SELECT 
            DATAPARTNERID AS DATA_PARTNER_ID,
            MANIFESTID AS MANIFEST_ID,
            mp.N3cds_Domain_Map_Id AS PERSON_ID, 
            gx.TARGET_CONCEPT_ID AS gender_concept_id,
            EXTRACT(YEAR FROM BIRTH_DATE) AS YEAR_OF_BIRTH, 
            EXTRACT(MONTH FROM BIRTH_DATE) AS MONTH_OF_BIRTH,
            1 AS DAY_OF_BIRTH,
            null as BIRTH_DATETIME,
--            rx.TARGET_CONCEPT_ID AS race_concept_id, 
            CASE WHEN demo.RACE != '06' OR (demo.RACE='06' AND demo.raw_race is null) then rx.TARGET_CONCEPT_ID
                ELSE null
                END AS race_concept_id,
            ex.TARGET_CONCEPT_ID AS ethnicity_concept_id, 
            lds.N3cds_Domain_Map_Id AS LOCATIONID,
            NULL AS PROVIDER_ID, 
            NULL AS CARE_SITE_ID, 
            demo.PATID AS person_source_value, 
            demo.SEX AS gender_source_value,  
            0 as gender_source_concept_id, 
            demo.RACE AS race_source_value, 
            0 AS race_source_concept_id,  
            demo.HISPANIC AS ethnicity_source_value, 
            0 AS ethnicity_source_concept_id, 
            'PCORNET_DEMOGRAPHIC' AS DOMAIN_SOURCE
            FROM NATIVE_PCORNET51_CDM.DEMOGRAPHIC demo
            JOIN CDMH_STAGING.PERSON_CLEAN pc on demo.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
            JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id=demo.PATID AND Mp.Domain_Name='PERSON' AND mp.DATA_PARTNER_ID=DATAPARTNERID 
            LEFT JOIN CDMH_STAGING.N3cds_Domain_Map lds on lds.Source_Id=demo.PATID AND lds.Domain_Name='LDS_ADDRESS_HISTORY' AND lds.DATA_PARTNER_ID=DATAPARTNERID  
            LEFT JOIN CDMH_STAGING.Gender_Xwalk gx on gx.CDM_TBL='DEMOGRAPHIC'AND Gx.Src_Gender=demo.Sex 
            LEFT JOIN CDMH_STAGING.ETHNICITY_XWALK ex on ex.CDM_TBL='DEMOGRAPHIC' AND demo.HISPANIC=Ex.Src_Ethnicity 
            LEFT JOIN CDMH_STAGING.RACE_XWALK rx on rx.CDM_TBL='DEMOGRAPHIC' AND demo.RACE=rx.Src_Race 
            ;
            
        personCnt  := sql%rowcount;
        commit;
   DELETE FROM CDMH_STAGING.ST_OMOP53_OBSERVATION WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DEMOGRAPHIC';
   COMMIT;        
    -- demographic -> observation 
    -- PAT_PREF_LANGUAGE_SPOKEN
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
        --PAT_PREF_LANGUAGE_SPOKEN
        select distinct
            DATAPARTNERID as DATA_PARTNER_ID,
            MANIFESTID as MANIFEST_ID,
            obs.n3cds_domain_map_id as OBSERVATION_ID ,
            mp.n3cds_domain_map_id as PERSON_ID,
            obs.target_concept_id as OBSERVATION_CONCEPT_ID,
            ee.OBS_DATE as OBSERVATION_DATE,
            ee.OBS_DATE as OBSERVATION_DATETIME,
            0 as OBSERVATION_TYPE_CONCEPT_ID,
            0 VALUE_AS_NUMBER,
            demo.PAT_PREF_LANGUAGE_SPOKEN as VALUE_AS_STRING,
            lang.TARGET_CONCEPT_ID as VALUE_AS_CONCEPT_ID ,
            0 as QUALIFIER_CONCEPT_ID,
            0 as UNIT_CONCEPT_ID,
            Null as PROVIDER_ID,
            null as VISIT_OCCURRENCE_ID,
            null  as VISIT_DETAIL_ID,
--            demo.PAT_PREF_LANGUAGE_SPOKEN  AS OBSERVATION_SOURCE_VALUE,
            'src=PCORNET.DEMOGRAPHIC dt=earliest ENC for pat'  AS OBSERVATION_SOURCE_VALUE,
            0 OBSERVATION_SOURCE_CONCEPT_ID,
            null as UNIT_SOURCE_VALUE,
            null as QUALIFIER_SOURCE_VALUE,
            'PCORNET_DEMOGRAPHIC' DOMAIN_SOURCE
            FROM NATIVE_PCORNET51_CDM.DEMOGRAPHIC demo
            JOIN CDMH_STAGING.PERSON_CLEAN pc on demo.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
            JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id=demo.PATID AND Mp.Domain_Name='PERSON' AND mp.DATA_PARTNER_ID=DATAPARTNERID 
            JOIN CDMH_STAGING.N3cds_Domain_Map obs on obs.Source_Id=demo.PATID AND obs.domain_name='DEMOGRAPHIC' and obs.Target_Domain_Id='Observation' 
                            AND obs.Target_Concept_Id=4152283 AND obs.DATA_PARTNER_ID=DATAPARTNERID    
            LEFT JOIN CDMH_STAGING.P2O_DEMO_TERM_XWALK lang on lang.src_cdm_column='PAT_PREF_LANGUAGE_SPOKEN' AND lang.SRC_CDM='PCORnet' 
                            AND lang.SRC_CDM_TBL='DEMOGRAPHIC' AND lang.SRC_CODE = demo.PAT_PREF_LANGUAGE_SPOKEN 
            LEFT JOIN ( SELECT PATID,MIN(ADMIT_DATE) AS OBS_DATE FROM Native_PCORNET51_CDM.ENCOUNTER GROUP BY PATID ) ee on ee.patid=demo.patid  
        ;
        observationCnt1  := sql%rowcount;
        commit;

    -- TF/ OT/ NI/ M/ DC/ TM/ F --GENDER_IDENTITY
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
            select distinct
             DATAPARTNERID as DATA_PARTNER_ID
            ,MANIFESTID as MANIFEST_ID
            ,ob.n3cds_domain_map_id as OBSERVATION_ID
            ,mp.n3cds_domain_map_id as PERSON_ID
            ,ob.target_concept_id as OBSERVATION_CONCEPT_ID
            ,ee.OBS_DATE as OBSERVATION_DATE
            ,ee.OBS_DATE as OBSERVATION_DATETIME
            ,0 as OBSERVATION_TYPE_CONCEPT_ID
            ,0 as VALUE_AS_NUMBER
            ,demo.gender_identity as VALUE_AS_STRING
            ,gx.TARGET_CONCEPT_ID as VALUE_AS_CONCEPT_ID
            ,0 as QUALIFIER_CONCEPT_ID
            ,null as UNIT_CONCEPT_ID
            ,null as PROVIDER_ID
            ,null as VISIT_OCCURRENCE_ID
            ,null as VISIT_DETAIL_ID
--            ,demo.gender_identity as OBSERVATION_SOURCE_VALUE
            ,'src=PCORNET.DEMOGRAPHIC dt=earliest ENC for pat'  AS OBSERVATION_SOURCE_VALUE
            ,0 as OBSERVATION_SOURCE_CONCEPT_ID
            ,null as UNIT_SOURCE_VALUE
            ,null as QUALIFIER_SOURCE_VALUE
            ,'PCORNET_DEMOGRAPHIC' as DOMAIN_SOURCE
            FROM NATIVE_PCORNET51_CDM.DEMOGRAPHIC demo
            JOIN CDMH_STAGING.PERSON_CLEAN pc on demo.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
            JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id=demo.PATID AND Mp.Domain_Name='PERSON' AND mp.DATA_PARTNER_ID=DATAPARTNERID  
            JOIN CDMH_STAGING.N3cds_Domain_Map ob on ob.Source_Id=demo.PATID AND ob.domain_name='DEMOGRAPHIC' and ob.Target_Domain_Id='Observation' 
                    AND Ob.Target_Concept_Id=4110772 AND ob.DATA_PARTNER_ID=DATAPARTNERID       
            LEFT JOIN CDMH_STAGING.P2O_DEMO_TERM_XWALK gx on gx.src_cdm_column='GENDER_IDENTITY' AND gx.SRC_CDM='PCORnet' AND gx.SRC_CDM_TBL='DEMOGRAPHIC' 
                    AND gx.SRC_CODE = demo.GENDER_IDENTITY 
            LEFT JOIN ( SELECT PATID,MIN(ADMIT_DATE) AS OBS_DATE FROM Native_PCORNET51_CDM.ENCOUNTER GROUP BY PATID ) ee on ee.patid=demo.patid             
        ;
        observationCnt2  := sql%rowcount;
        commit;
          --SEXUAL_ORIENTATION
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
        select distinct
             DATAPARTNERID as DATA_PARTNER_ID
            ,MANIFESTID as MANIFEST_ID
            ,ob.n3cds_domain_map_id as OBSERVATION_ID
            ,mp.n3cds_domain_map_id as PERSON_ID
            ,ob.target_concept_id as OBSERVATION_CONCEPT_ID
            ,ee.OBS_DATE as OBSERVATION_DATE
            ,ee.OBS_DATE as OBSERVATION_DATETIME
            ,0 as OBSERVATION_TYPE_CONCEPT_ID
            ,0 as VALUE_AS_NUMBER
            ,demo.gender_identity as VALUE_AS_STRING
            ,sx.TARGET_CONCEPT_ID as VALUE_AS_CONCEPT_ID
            ,0 as QUALIFIER_CONCEPT_ID
            ,null as UNIT_CONCEPT_ID
            ,null as PROVIDER_ID
            ,null as VISIT_OCCURRENCE_ID
            ,null as VISIT_DETAIL_ID
--            ,demo.SEXUAL_ORIENTATION AS OBSERVATION_SOURCE_VALUE
            ,'src=PCORNET.DEMOGRAPHIC dt=earliest ENC for pat'  AS OBSERVATION_SOURCE_VALUE
            ,0 as OBSERVATION_SOURCE_CONCEPT_ID
            ,null as UNIT_SOURCE_VALUE
            ,null as QUALIFIER_SOURCE_VALUE
            ,'PCORNET_DEMOGRAPHIC' as DOMAIN_SOURCE 
            FROM NATIVE_PCORNET51_CDM.DEMOGRAPHIC demo
            JOIN CDMH_STAGING.PERSON_CLEAN pc on demo.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
            JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id=demo.PATID AND Mp.Domain_Name='PERSON' AND mp.DATA_PARTNER_ID=DATAPARTNERID  
            JOIN CDMH_STAGING.N3cds_Domain_Map ob on ob.Source_Id=demo.PATID AND ob.domain_name='DEMOGRAPHIC' and ob.Target_Domain_Id='Observation'   
                    AND Ob.Target_Concept_Id=4283657 AND ob.DATA_PARTNER_ID=DATAPARTNERID     
            LEFT JOIN CDMH_STAGING.P2O_DEMO_TERM_XWALK sx on sx.src_cdm_column='SEXUAL_ORIENTATION' AND sx.SRC_CDM='PCORnet' AND sx.SRC_CDM_TBL='DEMOGRAPHIC'  
                    AND sx.SRC_CODE = demo.SEXUAL_ORIENTATION 
            LEFT JOIN ( SELECT PATID,MIN(ADMIT_DATE) AS OBS_DATE FROM Native_PCORNET51_CDM.ENCOUNTER GROUP BY PATID ) ee on ee.patid=demo.patid 
            ;
            
        observationCnt3  := sql%rowcount;
        commit;
        
      DELETE FROM CDMH_STAGING.st_omop53_observation_period WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DEMOGRAPHIC';
      COMMIT;     
        INSERT INTO CDMH_STAGING.st_omop53_observation_period
        (
            data_partner_id,
            manifest_id,
            observation_period_id,
            person_id,
            observation_period_start_date,
            observation_period_end_date,
            period_type_concept_id,
            domain_source
        )
        SELECT
          DATAPARTNERID AS  data_partner_id,
          MANIFESTID AS  manifest_id,
          ob.N3CDS_DOMAIN_MAP_ID AS  observation_period_id,
          mp.N3CDS_DOMAIN_MAP_ID  person_id,
          ee.observation_period_start_date as  observation_period_start_date,
          ee.observation_period_end_date as  observation_period_end_date,
          ob.TARGET_CONCEPT_ID AS period_type_concept_id,
          'PCORNET_DEMOGRAPHIC'  domain_source
        FROM NATIVE_PCORNET51_CDM.DEMOGRAPHIC demo
            JOIN CDMH_STAGING.PERSON_CLEAN pc on demo.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
            JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id=demo.PATID AND Mp.Domain_Name='PERSON' AND mp.DATA_PARTNER_ID=DATAPARTNERID  
            JOIN CDMH_STAGING.N3cds_Domain_Map ob on ob.Source_Id=demo.PATID AND ob.domain_name='DEMOGRAPHIC' and ob.Target_Domain_Id='OBSERVATION_PERIOD'  AND ob.TARGET_CONCEPT_ID=44814724 
                    AND ob.DATA_PARTNER_ID=DATAPARTNERID  
            JOIN ( SELECT PATID,MIN(ADMIT_DATE) AS observation_period_start_date,MAX(NVL(DISCHARGE_DATE,ADMIT_DATE)) AS observation_period_end_date FROM Native_PCORNET51_CDM.ENCOUNTER GROUP BY PATID ) ee on ee.patid=demo.patid 
    ;
      observationPdCnt4  := sql%rowcount;
        commit;
    RECORDCOUNT  := personCnt + observationCnt1+observationCnt2+observationCnt3+observationPdCnt4;
    DBMS_OUTPUT.put_line(RECORDCOUNT || '  PCORnet DEMOGRAPHIC source data inserted to PERSON and OBSERVATION staging table, ST_OMOP53_PERSON, ST_OMOP53_OBSERVATION, ST_OMOP53_OBSERVATION_PERIOD, successfully.');
    
END SP_P2O_SRC_DEMOGRAPHIC;
