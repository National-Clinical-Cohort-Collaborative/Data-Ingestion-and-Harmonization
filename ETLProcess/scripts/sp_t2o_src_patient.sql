
CREATE PROCEDURE CDMH_STAGING.SP_T2O_SRC_PATIENT 
(
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_T2O_SRC_PATIENT
     Purpose:    Loading NATIVE_TRINETX_CDM.patient DIMENSION to staging table 
        1. CDMH_STAGING.ST_OMOP53_PERSON 
        2. CDMH_STAGING.st_omop53_observation --language_cd
        3. CDMH_STAGING.st_omop53_location -- zip_cd
        4. CDMH_STAGING.st_omop53_observation_period
        5. CDMH_STAGING.st_omop53_death

     Source:    NATIVE_TRINETX_CDM.patient
     Revisions:
     Ver        Date        Author           Description
     0.1         8/11/20     SHONG           Initial version.

*********************************************************************************************************/
    personcnt           NUMBER;
    locationcnt         NUMBER;
    observationcnt1     NUMBER;
    observationPdCnt   NUMBER;
    death_recordCount   NUMBER;

BEGIN
    DELETE FROM CDMH_STAGING.st_omop53_location
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'TRINETX_PATIENT';
    COMMIT;

    --location with zip code
    INSERT INTO CDMH_STAGING.st_omop53_location (
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
            p.postal_code               AS zip,
            NULL AS county,
            NULL AS location_source_value,
            'TRINETX_PATIENT' AS domain_source
        FROM
            native_trinetx_cdm.patient   p
            JOIN cdmh_staging.person_clean pc ON p.patient_id = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map l ON l.source_id = p.postal_code
                                                    AND l.domain_name = 'PATIENT'
                                                    AND l.target_domain_id = 'Location'
                                                    AND l.data_partner_id = datapartnerid;

    locationcnt := SQL%rowcount;
    COMMIT;

    ----------------------
    DELETE FROM CDMH_STAGING.st_omop53_person
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'TRINETX_PATIENT';

    COMMIT;
    ---------------------- Person
    INSERT INTO CDMH_STAGING.st_omop53_person (
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
            gx.target_concept_id     AS gender_concept_id,
            EXTRACT(YEAR FROM to_date(p.birth_date, 'YYYY-MM-DD' )) AS year_of_birth,
            EXTRACT(MONTH FROM to_date(p.birth_date, 'YYYY-MM-DD')) AS month_of_birth,
            1 AS day_of_birth,
            NULL AS birth_datetime,
            rx.target_concept_id     AS race_concept_id, 
            --CASE WHEN pd.RACE != '06' OR (demo.RACE='06' AND demo.raw_race is null) then rx.TARGET_CONCEPT_ID
            --    ELSE null
            --    END AS race_concept_id,
            ex.target_concept_id     AS ethnicity_concept_id,
            l.n3cds_domain_map_id    AS locationid,
            NULL AS provider_id,
            NULL AS care_site_id,
            p.patient_id           AS person_source_value,
            nvl(p.mapped_sex, p.sex ) AS gender_source_value,
            0 AS gender_source_concept_id,
            nvl(p.mapped_race, p.race) AS race_source_value,
            0 AS race_source_concept_id,
            nvl(p.mapped_ethnicity, p.ethnicity ) AS ethnicity_source_value,
            0 AS ethnicity_source_concept_id,
            'TRINETX_PATIENT' AS domain_source
        FROM
            native_trinetx_cdm.patient   p
            JOIN CDMH_STAGING.person_clean              pc ON p.patient_id = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN CDMH_STAGING.n3cds_domain_map          mp ON mp.source_id = p.patient_id
                                                     AND mp.domain_name = 'PERSON'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN CDMH_STAGING.t2o_term_xwalk gx ON gx.cdm_tbl = 'PATIENT' and gx.cdm_tbl_column_name = 'mapped_sex' and nvl(p.mapped_sex, p.sex )  = gx.src_code 
            LEFT JOIN CDMH_STAGING.t2o_term_xwalk ex ON ex.cdm_tbl = 'PATIENT' and ex.cdm_tbl_column_name = 'mapped_ethnicity' and nvl(p.mapped_ethnicity, p.ethnicity )= ex.src_code 
            left join CDMH_STAGING.t2o_term_xwalk  rx on rx.cdm_tbl = 'PATIENT' and rx.cdm_tbl_column_name = 'mapped_race' and nvl(p.mapped_race, p.race) = rx.src_code 
            LEFT JOIN cdmh_staging.n3cds_domain_map          l ON l.source_id = p.postal_code
                                                         AND l.domain_name = 'PATIENT'
                                                         AND l.target_domain_id = 'Location'
                                                         AND l.data_partner_id = datapartnerid;

    personcnt := SQL%rowcount;
    COMMIT;

    DELETE FROM CDMH_STAGING.st_omop53_observation
    WHERE
        data_partner_id = datapartnerid 
        AND domain_source = 'TRINETX_PATIENT';

    COMMIT;
    -- patient dimension -> observation 
    -- language
    INSERT INTO CDMH_STAGING.st_omop53_observation (
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
        --language 
        SELECT DISTINCT
            datapartnerid             AS data_partner_id,
            manifestid                AS manifest_id,
            obs.n3cds_domain_map_id   AS observation_id,
            mp.n3cds_domain_map_id    AS person_id,
            obs.target_concept_id     AS observation_concept_id,
            ee.obs_date               AS observation_date,
            ee.obs_date               AS observation_datetime,
            0 AS observation_type_concept_id,
            0 value_as_number,
            p.language          AS value_as_string,
            lang.target_concept_id    AS value_as_concept_id,
            0 AS qualifier_concept_id,
            0 AS unit_concept_id,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            NULL AS visit_detail_id,
            'src=TRINETX_PATIENT dt=earliest ENC for Person' AS observation_source_value,
            --pd.language_cd as observation_source_value, 
            0 observation_source_concept_id, ---preferred language source concept id 45882691
            NULL AS unit_source_value,
            NULL AS qualifier_source_value,
            'TRINETX_PATIENT' domain_source
        FROM
            native_trinetx_cdm.patient  p
            JOIN cdmh_staging.person_clean  pc ON p.patient_id = pc.person_id AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map mp ON mp.source_id = p.patient_id
                                                     AND mp.domain_name = 'PERSON'
                                                     AND mp.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map  obs ON obs.source_id = p.patient_id
                                                      AND obs.domain_name = 'PATIENT'
                                                      AND obs.target_domain_id = 'Observation'
                                                      AND obs.target_concept_id = 4152283
                                                      AND obs.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.T2o_term_xwalk  lang ON lang.cdm_tbl_column_name = 'LANGUAGE'
                                                          AND lang.cdm_source = 'TRINETX'
                                                          AND lang.cdm_tbl = 'PATIENT'
                                                          AND lang.src_code = p.language
            LEFT JOIN (
                SELECT
                    patient_id,
                    MIN(start_date) AS obs_date
                FROM
                    native_TRINETX_cdm.encounter
                GROUP BY
                    patient_id
            ) ee ON ee.patient_id = p.patient_id;

    observationcnt1 := SQL%rowcount;
    COMMIT;
    DELETE FROM CDMH_STAGING.st_omop53_observation_period
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'TRINETX_PATIENT';

    COMMIT;
    INSERT INTO CDMH_STAGING.st_omop53_observation_period (
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
            datapartnerid                      AS data_partner_id,
            manifestid                         AS manifest_id,
            ob.n3cds_domain_map_id             AS observation_period_id,
            mp.n3cds_domain_map_id             person_id,
            ee.observation_period_start_date   AS observation_period_start_date,
            ee.observation_period_end_date     AS observation_period_end_date,
            ob.target_concept_id               AS period_type_concept_id,
            'TRINETX_PATIENT' domain_source
        FROM
            native_i2b2act_cdm.patient_dimension                                                                                                                                                                   pd
            JOIN cdmh_staging.person_clean                                                                                                                                                                              pc ON pd.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map                                                                                                                                                                          mp ON mp.source_id = pd.patient_num
                                                     AND mp.domain_name = 'PERSON'
                                                     AND mp.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map                                                                                                                                                                          ob ON ob.source_id = pd.patient_num
                                                     AND ob.domain_name = 'PATIENT'
                                                     AND ob.target_domain_id = 'OBSERVATION_PERIOD'
                                                     AND ob.target_concept_id = 44814724
                                                     AND ob.data_partner_id = datapartnerid
            JOIN (
                SELECT
                    patient_num,
                    MIN(start_date) AS observation_period_start_date,
                    MAX(nvl(end_date, nvl(dsch_date, start_date))) AS observation_period_end_date
                FROM
                    native_i2b2act_cdm.visit_dimension
                GROUP BY
                    patient_num
            ) ee ON ee.patient_num = pd.patient_num;

    observationPdCnt := SQL%rowcount;
    COMMIT;


    DELETE FROM CDMH_STAGING.ST_OMOP53_DEATH WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='TRINETX_DEATH';
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
       SELECT
       DATAPARTNERID as DATA_PARTNER_ID,
       MANIFESTID as MANIFEST_ID,
       p.N3cds_Domain_Map_Id AS PERSON_ID,
       NULL as DEATH_DATE,
       null as DEATH_DATETIME,
       32510  as DEATH_TYPE_CONCEPT_ID,
       0 as CAUSE_CONCEPT_ID,  -- there is no death cause as concept id , no death cause in ACT
       0 as CAUSE_SOURCE_VALUE, --put raw ICD10 codes here, no death cause in ACT 
       0 as CAUSE_SOURCE_CONCEPT_ID,  -- this field is number, ICD codes don't fit
       'TRINETX_PATIENT' as domain_source
    FROM NATIVE_TRINETX_CDM.patient d
    JOIN CDMH_STAGING.PERSON_CLEAN pc on d.patient_id=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
    JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=pc.person_id AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
    where d.vital_status ='D'
;
death_recordCount:=SQL%ROWCOUNT;
commit;

    recordcount := locationcnt + personcnt + observationcnt1 + observationPdCnt +death_recordCount; --70880
    dbms_output.put_line(recordcount || ' TRINETX_Patient source data inserted to PERSON, LOCATION and  OBSERVATION staging table, ST_OMOP53_LOCATION, ST_OMOP53_PERSON, ST_OMOP53_OBSERVATION, ST_OMOP53_OBSERVATION_PERIOD, ST_OMOP53_DEATH, successfully.'
    );


END SP_T2O_SRC_PATIENT;
