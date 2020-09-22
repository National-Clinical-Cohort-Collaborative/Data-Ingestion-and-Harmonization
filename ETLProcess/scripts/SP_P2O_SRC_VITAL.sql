
CREATE PROCEDURE                                                     CDMH_STAGING.SP_P2O_SRC_VITAL (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS
/*******************************************************************************************************
     Name:      SP_P2O_SRC_VITAL
     Purpose:    Loading The NATIVE_PCORNET51_CDM.vital    Table into 
                1. CDMH_STAGING.st_omop53_measurement

     Edit History:
     Ver          Date        Author               Description
       0.1         8/30/20     SHONG               Intial Version.
       
*********************************************************************************************************/

    ht_recordcount          NUMBER;
    wt_recordcount          NUMBER;
    bmi_recordcount         NUMBER;
    diastolic_recordcount   NUMBER;
    systolic_recordcount    NUMBER;
    smoking_recordcount     NUMBER;
    tobacco_recordcount     NUMBER;
BEGIN
    DELETE FROM cdmh_staging.st_omop53_measurement
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_VITAL';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_measurement (--for Height
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
    )--22 items
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS measurement_id,
            p.n3cds_domain_map_id    AS person_id,
            mp.target_concept_id     AS measurement_concept_id, --concept id for Height, from notes
            v.measure_date           AS measurement_date,
            v.measure_date           AS measurement_datetime,
            v.measure_time           AS measurement_time,
            vt.target_concept_id     AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            v.ht                     AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
            NULL AS value_as_concept_id,
            9327 AS unit_concept_id,
            NULL AS range_low,
            NULL AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            'Height in inches' AS measurement_source_value,
            NULL AS measurement_source_concept_id,
            'Inches' AS unit_source_value,
            v.ht                     AS value_source_value,
            'PCORNET_VITAL' AS domain_source
        FROM
            native_pcornet51_cdm.vital          v
            JOIN cdmh_staging.person_clean           pc ON v.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map       mp ON mp.source_id = v.vitalid
                                                     AND mp.domain_name = 'VITAL'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id = 4177340 --ht
            LEFT JOIN cdmh_staging.n3cds_domain_map       p ON p.source_id = v.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map       e ON e.source_id = v.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Vital'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL'
                                                              AND vt.src_cdm_column = 'VITAL_SOURCE'
                                                              AND vt.src_code = v.vital_source;

    ht_recordcount := SQL%rowcount;
    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_measurement (-- For weight
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
    )--22 items
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS measurement_id,
            p.n3cds_domain_map_id    AS person_id,
            mp.target_concept_id     AS measurement_concept_id, --concept id for Weight, from notes
            v.measure_date           AS measurement_date,
            v.measure_date           AS measurement_datetime,
            v.measure_time           AS measurement_time,
            vt.target_concept_id     AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            v.wt                     AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
            NULL AS value_as_concept_id,
            8739 AS unit_concept_id,
            NULL AS range_low,
            NULL AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            'Weight in pounds' AS measurement_source_value,
            NULL AS measurement_source_concept_id,
            'Pounds' AS unit_source_value,
            v.wt                     AS value_source_value,
            'PCORNET_VITAL' AS domain_source
        FROM
            native_pcornet51_cdm.vital          v
            JOIN cdmh_staging.person_clean           pc ON v.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map       mp ON mp.source_id = v.vitalid
                                                     AND mp.domain_name = 'VITAL'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id = 4099154 --htv.WT!=0
            LEFT JOIN cdmh_staging.n3cds_domain_map       p ON p.source_id = v.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map       e ON e.source_id = v.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Vital'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL'
                                                              AND vt.src_cdm_column = 'VITAL_SOURCE'
                                                              AND vt.src_code = v.vital_source;

    wt_recordcount := SQL%rowcount;
    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_measurement (-- For Diastolic BP
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
    )--22 items
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS measurement_id,
            p.n3cds_domain_map_id    AS person_id,
            bp.target_concept_id     AS measurement_concept_id, --concept id for DBPs, from notes
            v.measure_date           AS measurement_date,
            v.measure_date           AS measurement_datetime,
            v.measure_time           AS measurement_time,
            vt.target_concept_id     AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            v.diastolic              AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
            NULL AS value_as_concept_id,
            8876 AS unit_concept_id,
            NULL AS range_low,
            NULL AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            bp.target_concept_name   AS measurement_source_value,
            NULL AS measurement_source_concept_id,
            'millimeter mercury column' AS unit_source_value,
            v.diastolic              AS value_source_value,
            'PCORNET_VITAL' AS domain_source
        FROM
            native_pcornet51_cdm.vital          v
            JOIN cdmh_staging.person_clean           pc ON v.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_vital_term_xwalk   bp ON bp.src_cdm_tbl = 'VITAL'
                                                         AND bp.src_cdm_column = 'DIASTOLIC_BP_POSITION'
                                                         AND bp.src_code = v.bp_position
            JOIN cdmh_staging.n3cds_domain_map       mp ON mp.source_id = v.vitalid
                                                     AND mp.domain_name = 'VITAL'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND bp.target_concept_id = mp.target_concept_id --v.DIASTOLIC!=0
            LEFT JOIN cdmh_staging.n3cds_domain_map       p ON p.source_id = v.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map       e ON e.source_id = v.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Vital'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL'
                                                              AND vt.src_cdm_column = 'VITAL_SOURCE'
                                                              AND vt.src_code = v.vital_source;

    diastolic_recordcount := SQL%rowcount;
    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_measurement (-- For Systolic BP
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
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS measurement_id,
            p.n3cds_domain_map_id    AS person_id,
            bp.target_concept_id     AS measurement_concept_id, --concept id for SBPs, from notes
            v.measure_date           AS measurement_date,
            NULL AS measurement_datetime,
            v.measure_time           AS measurement_time,
            vt.target_concept_id     AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            v.systolic               AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
            NULL AS value_as_concept_id,
            8876 AS unit_concept_id,
            NULL AS range_low,
            NULL AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            bp.target_concept_name   AS measurement_source_value,
            NULL AS measurement_source_concept_id,
            'millimeter mercury column' AS unit_source_value,
            v.systolic               AS value_source_value,
            'PCORNET_VITAL' AS domain_source
        FROM
            native_pcornet51_cdm.vital          v
            JOIN cdmh_staging.person_clean           pc ON v.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_vital_term_xwalk   bp ON bp.src_cdm_tbl = 'VITAL'
                                                         AND bp.src_cdm_column = 'SYSTOLIC_BP_POSITION'
                                                         AND bp.src_code = v.bp_position
            JOIN cdmh_staging.n3cds_domain_map       mp ON mp.source_id = v.vitalid
                                                     AND mp.domain_name = 'VITAL'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id = bp.target_concept_id -- v.SYSTOLIC!=0
            LEFT JOIN cdmh_staging.n3cds_domain_map       p ON p.source_id = v.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map       e ON e.source_id = v.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Vital'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL'
                                                              AND vt.src_cdm_column = 'VITAL_SOURCE'
                                                              AND vt.src_code = v.vital_source;

    systolic_recordcount := SQL%rowcount;
    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_measurement (-- For Original BMI
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
    )--22 items
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS measurement_id,
            p.n3cds_domain_map_id    AS person_id,
            mp.target_concept_id     AS measurement_concept_id, --concept id for BMI, from notes
            v.measure_date           AS measurement_date,
            NULL AS measurement_datetime,
            v.measure_time           AS measurement_time,
            vt.target_concept_id     AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            v.original_bmi           AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
            NULL AS value_as_concept_id,
            NULL AS unit_concept_id,
            NULL AS range_low,
            NULL AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            'Original BMI' AS measurement_source_value,
            NULL AS measurement_source_concept_id,
            NULL AS unit_source_value,
            v.original_bmi           AS value_source_value,
            'PCORNET_VITAL' AS domain_source
        FROM
            native_pcornet51_cdm.vital          v
            JOIN cdmh_staging.person_clean           pc ON v.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map       mp ON mp.source_id = v.vitalid
                                                     AND mp.domain_name = 'VITAL'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id = 4245997 --v.ORIGINAL_BMI!=0
            LEFT JOIN cdmh_staging.n3cds_domain_map       p ON p.source_id = v.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map       e ON e.source_id = v.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Vital'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL'
                                                              AND vt.src_cdm_column = 'VITAL_SOURCE'
                                                              AND vt.src_code = v.vital_source;

    bmi_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_observation
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_VITAL';

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
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS observation_id,
            p.n3cds_domain_map_id    AS person_id,
            mp.target_concept_id     AS observation_concept_id, --concept id for Smoking, from notes
            v.measure_date           AS observation_date,
            v.measure_date           AS observation_datetime,
            vt.target_concept_id     AS observation_type_concept_id,
            NULL AS value_as_number,
            s.target_concept_name    AS value_as_string,
            s.target_concept_id      AS value_as_concept_id,
            NULL AS qualifier_concept_id,
            NULL AS unit_concept_id,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            concat('VITAL.SMOKING= ', v.smoking) AS observation_source_value,
            v.smoking                AS observation_source_concept_id,
            NULL AS unit_source_value,
            NULL AS qualifier_source_value,
            'PCORNET_VITAL' AS domain_source
        FROM
            native_pcornet51_cdm.vital          v
            JOIN cdmh_staging.person_clean           pc ON v.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_vital_term_xwalk   s ON s.src_cdm_tbl = 'VITAL'
                                                        AND s.src_cdm_column = 'SMOKING'
                                                        AND s.src_code = v.smoking
            JOIN cdmh_staging.n3cds_domain_map       mp ON mp.source_id = v.vitalid
                                                     AND mp.domain_name = 'VITAL'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND s.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.n3cds_domain_map       p ON p.source_id = v.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map       e ON e.source_id = v.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL'
                                                              AND vt.src_cdm_column = 'VITAL_SOURCE'
                                                              AND vt.src_code = v.vital_source;

    smoking_recordcount := SQL%rowcount;
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
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS observation_id,
            p.n3cds_domain_map_id    AS person_id,
            mp.target_concept_id     AS observation_concept_id, --concept id for TOBACCO, from notes
            v.measure_date           AS observation_date,
            v.measure_date           AS observation_datetime,
            vt.target_concept_id     AS observation_type_concept_id,
            NULL AS value_as_number,
            s.target_concept_name    AS value_as_string,
            s.target_concept_id      AS value_as_concept_id,
            NULL AS qualifier_concept_id,
            NULL AS unit_concept_id,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            concat('VITAL.TOBACCO= ', v.tobacco) AS observation_source_value,
    --NULL as OBSERVATION_SOURCE_VALUE,
            v.tobacco                AS observation_source_concept_id,
            NULL AS unit_source_value,
            NULL AS qualifier_source_value,
            'PCORNET_VITAL' AS domain_source
        FROM
            native_pcornet51_cdm.vital          v
            JOIN cdmh_staging.person_clean           pc ON v.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_vital_term_xwalk   s ON s.src_cdm_tbl = 'VITAL'
                                                        AND s.src_cdm_column = 'TOBACCO'
                                                        AND s.src_code = v.tobacco
            JOIN cdmh_staging.n3cds_domain_map       mp ON mp.source_id = v.vitalid
                                                     AND mp.domain_name = 'VITAL'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND s.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.n3cds_domain_map       p ON p.source_id = v.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map       e ON e.source_id = v.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.p2o_vital_term_xwalk      vt ON vt.src_cdm_tbl = 'VITAL'
                                                           AND vt.src_cdm_column = 'VITAL_SOURCE'
                                                           AND vt.src_code = v.vital_source;

    tobacco_recordcount := SQL%rowcount;
    COMMIT;
    recordcount := bmi_recordcount + ht_recordcount + wt_recordcount + systolic_recordcount + diastolic_recordcount + smoking_recordcount
    + tobacco_recordcount;
    dbms_output.put_line(recordcount || ' PCORnet VITAL source data inserted to following staging tables, ST_OMOP53_MEASUREMENT/ST_OMOP53_OBSERVATION, successfully.'
    );
END sp_p2o_src_vital;
