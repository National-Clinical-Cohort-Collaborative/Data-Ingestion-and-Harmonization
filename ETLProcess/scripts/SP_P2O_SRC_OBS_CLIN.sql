
CREATE PROCEDURE                                        CDMH_STAGING.SP_P2O_SRC_OBS_CLIN (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_OBS_CLIN
     Purpose:    Loading The NATIVE_PCORNET51_CDM.OBS_CLIN Table into 
                1. CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE
                2. CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE
                3. CDMH_STAGING.ST_OMOP53_OBSERVATION
                4. CDMH_STAGING.ST_OMOP53_MEASUREMENT
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1       5/16/2020     SHONG          Initial Version
     0.2       6/25/2020     RZHU           Added logic to insert into CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE
     0.3         9/9/2020      DIH          Updated the *_type_concept_id logic
*********************************************************************************************************/
    obs_recordcount         NUMBER;
    measure_recordcount     NUMBER;
    condition_recordcount   NUMBER;
    device_recordcount      NUMBER;
BEGIN

    -- PCORnet OBS_CLIN to OMOP531 Observation
    DELETE FROM cdmh_staging.st_omop53_observation
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_OBS_CLIN';

    COMMIT;
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
        SELECT
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS observation_id,
            p.n3cds_domain_map_id       AS person_id,
            xw.target_concept_id        AS observation_concept_id,
            obs.obsclin_date            AS observation_date,
            obs.obsclin_date            AS observation_datetime, --set same with obsclin_date 
--            CASE
--                WHEN obs.obsclin_type = 'LC' THEN
--                    37079395
--                WHEN obs.obsclin_type = 'SM' THEN
--                    37079429
--                ELSE
--                    0
--            END AS observation_type_concept_id,
            32817 AS observation_type_concept_id,
            obs.obsclin_result_num      AS value_as_number,
            obs.obsclin_result_text     AS value_as_string,
            obs.obsclin_result_snomed   AS value_as_concept_id,
            0 AS qualifier_concept_id, -- all 0 in data_store schema
            0 AS unit_concept_id, -- all 0 in data_store schema
            NULL AS provider_id, --for now, still TBD, Governance group question, Davera will follow up
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            obs.obsclin_code            AS observation_source_value,
            xw.target_concept_id        AS observation_source_concept_id,
            obs.obsclin_result_unit     AS unit_source_value, -- lookup unit_concept_code
            obs.obsclin_result_qual     AS qualifier_source_value,
            'PCORNET_OBS_CLIN' AS domain_source
        FROM
            native_pcornet51_cdm.obs_clin          obs
            JOIN cdmh_staging.person_clean              pc ON obs.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = obs.obsclinid
                                                     AND mp.domain_name = 'OBS_CLIN'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = obs.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = obs.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON obs.obsclin_code = xw.src_code
                                                                 AND xw.cdm_tbl = 'OBS_CLIN'
                                                                 AND xw.target_concept_id = mp.target_concept_id;

    obs_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_measurement
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_OBS_CLIN';

    COMMIT;
    
    -- PCORnet OBS_CLIN to OMOP531 Measurement
    INSERT INTO cdmh_staging.st_omop53_measurement (
        data_partner_id,
        manifest_id,
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_date,
        measurement_datetime,
        measurement_time,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_number,
        value_as_concept_id,
        unit_concept_id,
        range_low,
        range_high,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value,
        value_source_value,
        domain_source
    )
        SELECT
            datapartnerid             AS data_partner_id,
            manifestid                AS manifest_id,
            mp.n3cds_domain_map_id    AS measurement_id,
            p.n3cds_domain_map_id     AS person_id,
            xw.target_concept_id      AS measurement_concept_id,
            obs.obsclin_date          AS measurement_date,
            obs.obsclin_date          AS measurement_datetime,
            obs.obsclin_time          AS measurement_time,
--            CASE
--                WHEN obs.obsclin_type = 'LC' THEN
--                    37079395
--                WHEN obs.obsclin_type = 'SM' THEN
--                    37079429
--                ELSE
--                    0
--            END AS measurement_type_concept_id,
            32817 AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            obs.obsclin_result_num    AS value_as_number,
            NULL AS value_as_concept_id,
            u.target_concept_id       AS unit_concept_id,
            NULL AS range_low,
            NULL AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id     AS visit_occurrence_id,
            NULL AS visit_detail_id,
            obs.obsclin_code          AS measurement_source_value,
            xw.target_concept_id      AS measurement_source_concept_id,
            obs.obsclin_result_unit   AS unit_source_value,
            obs.obsclin_result_qual   AS value_source_value,
            'PCORNET_OBS_CLIN' AS domain_source
        FROM
            native_pcornet51_cdm.obs_clin          obs
            JOIN cdmh_staging.person_clean              pc ON obs.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = obs.obsclinid
                                                     AND mp.domain_name = 'OBS_CLIN'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = obs.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = obs.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON obs.obsclin_code = xw.src_code
                                                                 AND xw.cdm_tbl = 'OBS_CLIN'
                                                                 AND xw.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.p2o_code_xwalk_standard   lab ON obs.obsclin_code = lab.src_code
                                                                  AND lab.cdm_tbl = 'LAB_RESULT_CM'
                                                                  AND lab.target_domain_id = 'Measurement'
            LEFT JOIN cdmh_staging.p2o_medadmin_term_xwalk   u ON obs.obsclin_result_unit = u.src_code
                                                                AND u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT';

    measure_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_condition_occurrence
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_OBS_CLIN';

    COMMIT;

    -- PCORnet OBS_CLIN to OMOP531 Condition_Occurrence
    INSERT INTO cdmh_staging.st_omop53_condition_occurrence (
        data_partner_id,
        manifest_id,
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_end_date,
        condition_end_datetime,
        condition_type_concept_id,
        stop_reason,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        condition_source_value,
        condition_source_concept_id,
        condition_status_source_value,
        condition_status_concept_id,
        domain_source
    )
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS condition_occurrence_id,
            p.n3cds_domain_map_id    AS person_id,
            xw.target_concept_id     AS condition_concept_id,
            obs.obsclin_date         AS condition_start_date,
            obs.obsclin_date         AS condition_start_datetime,
            obs.obsclin_date         AS condition_end_date,
            obs.obsclin_date         AS condition_end_datetime,
--            CASE
--                WHEN obs.obsclin_type = 'LC' THEN
--                    37079395
--                WHEN obs.obsclin_type = 'SM' THEN
--                    37079429
--                ELSE
--                    0
--            END AS condition_type_concept_id,
            32817 AS condition_type_concept_id,
            NULL AS stop_reason,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            obs.obsclin_code         AS condition_source_value,
            xw.target_concept_id     AS condition_source_concept_id,
            NULL AS condition_status_source_value,
            NULL AS condition_status_concept_id,
            'PCORNET_OBS_CLIN' AS domain_source
        FROM
            native_pcornet51_cdm.obs_clin          obs
            JOIN cdmh_staging.person_clean              pc ON obs.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = obs.obsclinid
                                                     AND mp.domain_name = 'OBS_CLIN'
                                                     AND mp.target_domain_id = 'Condition'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = obs.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = obs.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON obs.obsclin_code = xw.src_code
                                                                 AND xw.cdm_tbl = 'OBS_CLIN'
                                                                 AND xw.target_concept_id = mp.target_concept_id;

    condition_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_device_exposure
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_OBS_CLIN';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_device_exposure (
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
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS device_exposure_id,
            p.n3cds_domain_map_id    AS person_id,
            xw.target_concept_id     AS device_concept_id,
            obs.obsclin_date         AS device_exposure_start_date,
            obs.obsclin_date         AS device_exposure_start_datetime,
            obs.obsclin_date         AS device_exposure_end_date,
            obs.obsclin_date         AS device_exposure_end_datetime,
--            44818707 AS device_type_concept_id, -- default values from draft mappings spreadsheet
            32817 AS device_type_concept_id,
            NULL AS unique_device_id, --??
            NULL AS quantity, --??
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            obs.obsclin_code         AS device_source_value, --??
            NULL AS device_source_concept_id, --??
            'PCORNET_OBS_CLIN' AS domain_source
        FROM
            native_pcornet51_cdm.obs_clin          obs
            JOIN cdmh_staging.person_clean              pc ON obs.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = obs.obsclinid
                                                     AND mp.domain_name = 'OBS_CLIN'
                                                     AND mp.target_domain_id = 'Device'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = obs.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = obs.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON obs.obsclin_code = xw.src_code
                                                                 AND xw.cdm_tbl = 'OBS_CLIN'
                                                                 AND xw.target_concept_id = mp.target_concept_id;

    device_recordcount := SQL%rowcount;
    COMMIT;
    recordcount := obs_recordcount + measure_recordcount + condition_recordcount + device_recordcount;
    dbms_output.put_line(recordcount || ' PCORnet OBS_CLIN source data inserted to ST_OMOP53_CONDITION_OCCURRENCE/ST_OMOP53_MEASUREMENT/ST_OMOP53_OBSERVATION/ST_OMOP53_DEVICE_EXPOSURE, successfully.'
    );
END sp_p2o_src_obs_clin;
