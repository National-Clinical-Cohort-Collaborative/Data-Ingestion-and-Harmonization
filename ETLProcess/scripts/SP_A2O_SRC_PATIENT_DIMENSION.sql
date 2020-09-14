
CREATE PROCEDURE     CDMH_STAGING.SP_A2O_SRC_PATIENT_DIMENSION (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS 
/********************************************************************************************************
     Name:      sp_a2o_src_patient_dimension
     Purpose:    Loading NATIVE_I2B2ACT_CDM.patient_dimension to staging table 
        1. CDMH_STAGING.ST_OMOP53_PERSON 
        2. CDMH_STAGING.st_omop53_observation --language_cd
        3. CDMH_STAGING.st_omop53_location -- zip_cd
        4. CDMH_STAGING.st_omop53_observation_period
        5. CDMH_STAGING.st_omop53_death
     Source:    NATIVE_I2B2ACT_CDM.patient_dimension
     Revisions:
     Ver        Date        Author           Description
     0.1         7/20/20     SHONG           Initial version.
     0.2        8/6/20      SHONG            Added death record logic
     0.3        8/25/20     DIH              Updated death record logic
     0.4        9/14/20     SHONG            Most recent update, include death even when there is no death date, if data contains the vital status of D. 
*********************************************************************************************************/
    personcnt           NUMBER;
    locationcnt         NUMBER;
    observationcnt1     NUMBER;
    observationpdcnt4   NUMBER;
    death_recordCount   NUMBER;
    death_recordCount2   NUMBER;
BEGIN
    DELETE FROM cdmh_staging.st_omop53_location
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_PATIENT_DIMENSION';

    COMMIT;
    --location with zip code
    INSERT INTO cdmh_staging.st_omop53_location (
        data_partner_id,
        manifest_id,
        location_id,
        address_1,
        address_2,
        city,
        state,
        zip,
        county,
        location_source_value,
        domain_source
    )
        SELECT DISTINCT
            datapartnerid           AS data_partner_id,
            manifestid              AS manifest_id,
            l.n3cds_domain_map_id   AS location_id,
            NULL AS address_1,
            NULL AS address_2,
            NULL AS city,
            NULL AS state,
            pd.zip_cd               AS zip,
            NULL AS county,
            NULL AS location_source_value,
            'I2B2ACT_PATIENT_DIMENSION' AS domain_source
        FROM
            native_i2b2act_cdm.patient_dimension   pd
            JOIN cdmh_staging.person_clean              pc ON pd.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          l ON l.source_id = pd.zip_cd
                                                    AND l.domain_name = 'PATIENT_DIMENSION'
                                                    AND l.target_domain_id = 'Location'
                                                    AND l.data_partner_id = datapartnerid;

    locationcnt := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_person
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_PATIENT_DIMENSION';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_person (
        data_partner_id,
        manifest_id,
        person_id,
        gender_concept_id,
        year_of_birth,
        month_of_birth,
        day_of_birth,
        birth_datetime,
        race_concept_id,
        ethnicity_concept_id,
        location_id,
        provider_id,
        care_site_id,
        person_source_value,
        gender_source_value,
        gender_source_concept_id,
        race_source_value,
        race_source_concept_id,
        ethnicity_source_value,
        ethnicity_source_concept_id,
        domain_source
    )
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS person_id,
            NVL(gx.target_concept_id,0)     AS gender_concept_id,
            EXTRACT(YEAR FROM birth_date) AS year_of_birth,
            EXTRACT(MONTH FROM birth_date) AS month_of_birth,
            1 AS day_of_birth,
            NULL AS birth_datetime,
            NVL(rx.target_concept_id,0)     AS race_concept_id, 
            --CASE WHEN pd.RACE != '06' OR (demo.RACE='06' AND demo.raw_race is null) then rx.TARGET_CONCEPT_ID
            --    ELSE null
            --    END AS race_concept_id,
            NVL(ex.target_concept_id,0)     AS ethnicity_concept_id,
            l.n3cds_domain_map_id    AS locationid,
            NULL AS provider_id,
            NULL AS care_site_id,
            pd.patient_num           AS person_source_value,
            nvl(pd.sex_cd, sex.concept_cd) AS gender_source_value,
            0 AS gender_source_concept_id,
            nvl(pd.race_cd, race.concept_cd) AS race_source_value,
            0 AS race_source_concept_id,
            nvl(pd.ethnicity_cd, ethnicity.concept_cd) AS ethnicity_source_value,
            0 AS ethnicity_source_concept_id,
            'I2B2ACT_PATIENT_DIMENSION' AS domain_source
        FROM
            native_i2b2act_cdm.patient_dimension   pd
            JOIN cdmh_staging.person_clean              pc ON pd.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = pd.patient_num
                                                     AND mp.domain_name = 'PERSON'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN (
                        select PATIENT_NUM,CONCEPT_CD,ROW_NUMBER() OVER ( PARTITION BY PATIENT_NUM ORDER BY START_DATE DESC) row_num
                         from NATIVE_I2B2ACT_CDM.observation_fact where concept_cd like 'DEM|HISP%'
                        ) ethnicity ON ethnicity.patient_num=pd.patient_num and ethnicity.row_num=1
            LEFT JOIN (
                        select PATIENT_NUM,CONCEPT_CD,ROW_NUMBER() OVER ( PARTITION BY PATIENT_NUM ORDER BY START_DATE DESC) row_num
                        from NATIVE_I2B2ACT_CDM.observation_fact where concept_cd like 'DEM|RACE%'
                        ) race ON race.patient_num=pd.patient_num and race.row_num=1
            LEFT JOIN (
                        select PATIENT_NUM,CONCEPT_CD,ROW_NUMBER() OVER ( PARTITION BY PATIENT_NUM ORDER BY START_DATE DESC) row_num
                        from NATIVE_I2B2ACT_CDM.observation_fact where concept_cd like 'DEM|SEX%'
                        ) sex ON sex.patient_num=pd.patient_num and sex.row_num=1
            LEFT JOIN cdmh_staging.gender_xwalk              gx ON gx.cdm_tbl = 'PATIENT_DIMENSION'
                                                      AND gx.src_gender = nvl(pd.sex_cd, sex.concept_cd)
            LEFT JOIN cdmh_staging.ethnicity_xwalk           ex ON ex.cdm_tbl = 'PATIENT_DIMENSION'
                                                         AND ex.src_ethnicity = nvl(pd.ethnicity_cd, ethnicity.concept_cd)
            LEFT JOIN cdmh_staging.race_xwalk                rx ON rx.cdm_tbl = 'PATIENT_DIMENSION'
                                                    AND rx.src_race = nvl(pd.race_cd, race.concept_cd)
            LEFT JOIN cdmh_staging.n3cds_domain_map          l ON l.source_id = pd.zip_cd
                                                         AND l.domain_name = 'PATIENT_DIMENSION'
                                                         AND l.target_domain_id = 'Location'
                                                         AND l.data_partner_id = datapartnerid;

    personcnt := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_observation
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_PATIENT_DIMENSION';

    COMMIT;
    -- patient dimension -> observation 
    -- language_cd
    INSERT INTO cdmh_staging.st_omop53_observation (
        data_partner_id,
        manifest_id,
        observation_id,
        person_id,
        observation_concept_id,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        value_as_string,
        value_as_concept_id,
        qualifier_concept_id,
        unit_concept_id,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        observation_source_value,
        observation_source_concept_id,
        unit_source_value,
        qualifier_source_value,
        domain_source
    )
        --language_cd 
        SELECT /*+ 
         index(pc,IDX_PERSON_CLEAN) 
         index(mp,IDX_DOMAIN_MAP_DATAPARTNERID)
         index(obs,IDX_DOMAIN_MAP_UNIQUE)
        */ 
            datapartnerid             AS data_partner_id,
            manifestid                AS manifest_id,
            obs.n3cds_domain_map_id   AS observation_id,
            mp.n3cds_domain_map_id    AS person_id,
            obs.target_concept_id     AS observation_concept_id,
            ee.obs_date               AS observation_date,
            ee.obs_date               AS observation_datetime,
            0 AS observation_type_concept_id,
            0 value_as_number,
            pd.language_cd            AS value_as_string,
            lang.target_concept_id    AS value_as_concept_id,
            0 AS qualifier_concept_id,
            0 AS unit_concept_id,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            NULL AS visit_detail_id,
            'src=I2B2ACT_PATIENT_DIM dt=earliest ENC for Person' AS observation_source_value,
            --pd.language_cd as observation_source_value, 
            0 observation_source_concept_id, ---preferred language source concept id 45882691
            NULL AS unit_source_value,
            NULL AS qualifier_source_value,
            'I2B2ACT_PATIENT_DIMENSION' domain_source
        FROM
            native_i2b2act_cdm.patient_dimension                                                                    pd
            JOIN cdmh_staging.person_clean                                                                               pc ON pd.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map                                                                           mp ON mp.source_id = pd.patient_num
                                                     AND mp.domain_name = 'PERSON'
                                                     AND mp.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map                                                                           obs ON obs.source_id = pd.patient_num
                                                      AND obs.domain_name = 'PATIENT_DIMENSION'
                                                      AND obs.data_partner_id = datapartnerid
                                                      AND obs.target_domain_id = 'Observation'  
                                                      AND obs.target_concept_id = 4152283
            LEFT JOIN cdmh_staging.a2o_term_xwalk                                                                             lang ON lang.cdm_tbl_column_name = 'LANGUAGE_CD'
                                                          AND lang.cdm_source = 'I2B2ACT'
                                                          AND lang.cdm_tbl = 'PATIENT_DIMENSION'
                                                          AND lang.src_code = pd.language_cd
            LEFT JOIN (
                SELECT
                    patient_num,
                    MIN(start_date) AS obs_date
                FROM
                    native_i2b2act_cdm.visit_dimension
                GROUP BY
                    patient_num
            ) ee ON ee.patient_num = pd.patient_num
             ;

    observationcnt1 := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_observation_period
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_PATIENT_DIMENSION';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_observation_period (
        data_partner_id,
        manifest_id,
        observation_period_id,
        person_id,
        observation_period_start_date,
        observation_period_end_date,
        period_type_concept_id,
        domain_source
    )
        SELECT /*+ 
         index(pc,IDX_PERSON_CLEAN) 
         index(mp,IDX_DOMAIN_MAP_DATAPARTNERID)
         index(ob,IDX_DOMAIN_MAP_UNIQUE)
        */ 
            datapartnerid                      AS data_partner_id,
            manifestid                         AS manifest_id,
            ob.n3cds_domain_map_id             AS observation_period_id,
            mp.n3cds_domain_map_id             person_id,
            ee.observation_period_start_date   AS observation_period_start_date,
            ee.observation_period_end_date     AS observation_period_end_date,
            ob.target_concept_id               AS period_type_concept_id,
            'I2B2ACT_PATIENT_DIMENSION' domain_source
        FROM
            native_i2b2act_cdm.patient_dimension                                                                                                                                                                   pd
            JOIN cdmh_staging.person_clean                                                                                                                                                                              pc ON pd.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map                                                                                                                                                                          mp ON mp.source_id = pd.patient_num
                                                     AND mp.domain_name = 'PERSON'
                                                     AND mp.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map                                                                                                                                                                          ob ON ob.source_id = pd.patient_num
                                                     AND ob.domain_name = 'PATIENT_DIMENSION'
                                                     AND ob.data_partner_id = datapartnerid
                                                     AND ob.target_domain_id = 'OBSERVATION_PERIOD' 
                                                     AND ob.target_concept_id = 44814724
            JOIN (
                SELECT
                    patient_num,
                    MIN(start_date) AS observation_period_start_date,
                    MAX(nvl(end_date, nvl(dsch_date, start_date))) AS observation_period_end_date
                FROM
                    native_i2b2act_cdm.visit_dimension
                GROUP BY
                    patient_num
            ) ee ON ee.patient_num = pd.patient_num
            ;

    observationpdcnt4 := SQL%rowcount;
    COMMIT;

    DELETE FROM CDMH_STAGING.ST_OMOP53_DEATH WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='I2B2ACT_PATIENT_DIMENSION';
   COMMIT;
   --execute immediate 'truncate table CDMH_STAGING.ST_OMOP53_DEATH';
   INSERT INTO CDMH_STAGING.ST_OMOP53_DEATH ( 
      DATA_PARTNER_ID,
      MANIFEST_ID,
      PERSON_ID,
      DEATH_DATE,
      DEATH_DATETIME,
      DEATH_TYPE_CONCEPT_ID,
      CAUSE_CONCEPT_ID,
      CAUSE_SOURCE_VALUE,
      CAUSE_SOURCE_CONCEPT_ID,
      DOMAIN_SOURCE
    )
       SELECT /*+ 
         index(pc,IDX_PERSON_CLEAN) 
         index(p,IDX_DOMAIN_MAP_DATAPARTNERID)
        */
       DATAPARTNERID as DATA_PARTNER_ID,
       MANIFESTID as MANIFEST_ID,
       p.N3cds_Domain_Map_Id AS PERSON_ID,
       d.DEATH_DATE as DEATH_DATE,
       null as DEATH_DATETIME,
       32510  as DEATH_TYPE_CONCEPT_ID,
       0 as CAUSE_CONCEPT_ID,  -- there is no death cause as concept id , no death cause in ACT
       NULL as CAUSE_SOURCE_VALUE, --put raw ICD10 codes here, no death cause in ACT 
       0 as CAUSE_SOURCE_CONCEPT_ID,  -- this field is number, ICD codes don't fit
       'I2B2ACT_PATIENT_DIMENSION' as domain_source
    FROM NATIVE_I2B2ACT_CDM.patient_dimension d
    JOIN CDMH_STAGING.PERSON_CLEAN pc on d.patient_num=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
    JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pc.person_id AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
    where d.death_date is not null 
;
death_recordCount:=SQL%ROWCOUNT;
commit;

   INSERT INTO CDMH_STAGING.ST_OMOP53_DEATH ( 
      DATA_PARTNER_ID,
      MANIFEST_ID,
      PERSON_ID,
      DEATH_DATE,
      DEATH_DATETIME,
      DEATH_TYPE_CONCEPT_ID,
      CAUSE_CONCEPT_ID,
      CAUSE_SOURCE_VALUE,
      CAUSE_SOURCE_CONCEPT_ID,
      DOMAIN_SOURCE
    )
       SELECT  /*+ 
         index(pc,IDX_PERSON_CLEAN) 
         index(p,IDX_DOMAIN_MAP_DATAPARTNERID)
        */
       DATAPARTNERID as DATA_PARTNER_ID,
       MANIFESTID as MANIFEST_ID,
       p.N3cds_Domain_Map_Id AS PERSON_ID,
       d.DEATH_DATE as DEATH_DATE,
       null as DEATH_DATETIME,
       32510  as DEATH_TYPE_CONCEPT_ID,
       0 as CAUSE_CONCEPT_ID,  -- there is no death cause as concept id , no death cause in ACT
       NULL as CAUSE_SOURCE_VALUE, --put raw ICD10 codes here, no death cause in ACT 
       0 as CAUSE_SOURCE_CONCEPT_ID,  -- this field is number, ICD codes don't fit
       'I2B2ACT_PATIENT_DIMENSION' as domain_source
    FROM NATIVE_I2B2ACT_CDM.patient_dimension d
    JOIN CDMH_STAGING.PERSON_CLEAN pc on d.patient_num=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
    JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pc.person_id AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
    where d.death_date = null and d.vital_status_cd in ('D', 'DEM|VITAL STATUS:D')
;
death_recordCount2:=SQL%ROWCOUNT;
commit;
    recordcount := locationcnt + personcnt + observationcnt1 + observationpdcnt4+death_recordCount+death_recordCount2; 
    dbms_output.put_line(recordcount || '  i2b2Act_Patient_dimension source data inserted to PERSON, LOCATION and  OBSERVATION staging table, ST_OMOP53_LOCATION, ST_OMOP53_PERSON, ST_OMOP53_OBSERVATION, ST_OMOP53_OBSERVATION_PERIOD, ST_OMOP53_DEATH, successfully.'
    );
END sp_a2o_src_patient_dimension;
