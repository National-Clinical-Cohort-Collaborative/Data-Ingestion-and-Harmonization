--------------------------------------------------------
--  File created - Thursday-November-19-2020   
--------------------------------------------------------
-- Unable to render PROCEDURE DDL for object CDMH_STAGING.SP_P2O_SRC_LAB_RESULT_CM with DBMS_METADATA attempting internal generator.
CREATE PROCEDURE              CDMH_STAGING.SP_P2O_SRC_LAB_RESULT_CM (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_LAB_RESULT_CM
     Purpose:    Loading The NATIVE_PCORNET51_CDM.LAB_RESULT_CM Table into 
                1. CDMH_STAGING.ST_OMOP53_MEASUREMENT
                2. CDMH_STAGING.ST_OMOP53_OBSERVATION

     Edit History :
     Ver          Date        Author               Description
    0.1       5/16/2020     SHONG Initial Version
    0.2.        DI&S update_type_concept_id
    0.3       10/7/20 SHONG if the result_qual is NI, UN or OT set value_as_concept_id to NULL, not mappable. 
    0.4       10/16/20 SHONG, SNAREDLA  Insert blood type results to measurement domain.
*********************************************************************************************************/
    measure_recordcount       NUMBER;
    observation_recordcount   NUMBER;
    bloodcount                NUMBER;
BEGIN
    DELETE FROM cdmh_staging.st_omop53_measurement
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_LAB_RESULT_CM';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_measurement (--for lab_result_cm 
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
            xw.target_concept_id     AS measurement_concept_id, --concept id for lab - measurement
            lab.result_date          AS measurement_date,
            lab.result_date          AS measurement_datetime,
            lab.result_time          AS measurement_time, 
--            case when lab.lab_result_source ='OD' then 5001
--                when lab.lab_result_source ='BI' then 32466
--                when lab.lab_result_source ='CL' then 32466
--                when lab.lab_result_source ='DR' then 45754907
--                when lab.lab_result_source ='NI' then 46237210
--                when lab.lab_result_source ='UN' then 45877986
--                when lab.lab_result_source ='OT' then 45878142
--                else 0 end AS measurement_type_concept_id,  -----lab_result_source determines the ------------
            nvl(xw2.target_concept_id, 0) AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            lab.result_num           AS value_as_number, --result_num
            CASE
                WHEN lower(TRIM(result_qual)) = 'positive'             THEN
                    45884084
                WHEN lower(TRIM(result_qual)) = 'negative'             THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'pos'                  THEN
                    45884084
                WHEN lower(TRIM(result_qual)) = 'neg'                  THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'presumptive positive' THEN
                    45884084
                WHEN lower(TRIM(result_qual)) = 'presumptive negative' THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'detected'             THEN
                    45884084
                WHEN lower(TRIM(result_qual)) = 'not detected'         THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'inconclusive'         THEN
                    45877990
                WHEN lower(TRIM(result_qual)) = 'normal'               THEN
                    45884153
                WHEN lower(TRIM(result_qual)) = 'abnormal'             THEN
                    45878745
                WHEN lower(TRIM(result_qual)) = 'low'                  THEN
                    45881666
                WHEN lower(TRIM(result_qual)) = 'high'                 THEN
                    45876384
                WHEN lower(TRIM(result_qual)) = 'borderline'           THEN
                    45880922
                WHEN lower(TRIM(result_qual)) = 'elevated'             THEN
                    4328749  --ssh add issue number 55 - 6/26/2020 
                WHEN lower(TRIM(result_qual)) = 'undetermined'         THEN
                    45880649
                WHEN lower(TRIM(result_qual)) = 'undetectable'         THEN
                    45878583 
--                when lower(trim(result_qual)) = 'un' then 0
--                when lower(trim(result_qual)) = 'unknown' then 0
--                when lower(trim(result_qual)) = 'no information' then 46237210
                WHEN lower(TRIM(result_qual)) IN (
                    'ni',
                    'ot',
                    'un',
                    'no information',
                    'unknown',
                    'other'
                ) THEN
                    NULL
                WHEN result_qual IS NULL THEN
                    NULL
                ELSE
                    45877393 -- all other cases INVALID, Not Detected, See Comments, TNP, QNS, =45877393	LA10613-0	Other/Unknown/Refuse To Answer
            END AS value_as_concept_id,
            u.target_concept_id      AS unit_concept_id,
            ---lab.NORM_RANGE_LOW as RANGE_LOW, -- non-numeric fields are found
            ---lab.NORM_RANGE_HIGH as RANGE_HIGH, -- non-numeric data will error on a insert, omop define this column as float
--            case when LENGTH(TRIM(TRANSLATE(NORM_RANGE_LOW, ' +-.0123456789', ' '))) is null Then cast(NORM_RANGE_LOW as float) else 0 end as range_low,
--            case when LENGTH(TRIM(TRANSLATE(NORM_RANGE_HIGH, ' +-.0123456789', ' '))) is null Then cast(NORM_RANGE_HIGH as integer ) else 0 end as range_high,
            CASE
                WHEN fn_is_number(norm_range_low) = 1 THEN
                    CAST(norm_range_low AS FLOAT)
                ELSE
                    0
            END AS range_low,
            CASE
                WHEN fn_is_number(norm_range_high) = 1 THEN
                    CAST(norm_range_high AS INTEGER)
                ELSE
                    0
            END AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            lab.lab_loinc            AS measurement_source_value,
        --cast(xw.source_code_concept_id as int ) as MEASUREMENT_SOURCE_CONCEPT_ID,
            0 AS measurement_source_concept_id,
            lab.result_unit          AS unit_source_value,
            nvl(lab.raw_result, decode(lab.result_num, 0, lab.result_qual)) AS value_source_value,
            'PCORNET_LAB_RESULT_CM' AS domain_source
        FROM
            native_pcornet51_cdm.lab_result_cm     lab
            JOIN cdmh_staging.person_clean              pc ON lab.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = lab.lab_result_cm_id
                                                     AND mp.domain_name = 'LAB_RESULT_CM'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = lab.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = lab.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
        ---LEFT JOIN CDMH_STAGING.N3cds_Domain_Map prv on prv.Source_Id=e.source_id.providerid AND prv.Domain_Name='PROVIDER' AND prv.DATA_PARTNER_ID=DATAPARTNERID 
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON lab.lab_loinc = xw.src_code
                                                            AND xw.cdm_tbl = 'LAB_RESULT_CM'
                                                            AND xw.target_domain_id = 'Measurement'
                                                            AND xw.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.p2o_medadmin_term_xwalk   u ON lab.result_unit = u.src_code
                                                                AND u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'
            LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON lab.lab_result_source = xw2.src_code
                                                         AND xw2.cdm_tbl = 'LAB_RESULT_CM'
                                                         AND xw2.cdm_tbl_column_name = 'LAB_RESULT_SOURCE'
        WHERE
            lab.lab_loinc NOT IN (
                '882-1'
            );
        -- raw_unit contains the correct unit, but the correct column we should use here is result_unit
        -----LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on lab.result_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'; 
        ----SSH: change to use the result_unit/UNC contained error in their original data sent
        ----lab.result_unit use the lab.result_unit when joining to retrieve the target concept id associated with the unit

    measure_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_observation
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_LAB_RESULT_CM';

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
    ) --1870
        SELECT
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS observation_id,
            p.n3cds_domain_map_id       AS person_id,
            xw.target_concept_id        AS observation_concept_id,
            lab.result_date             AS observation_date,
            lab.result_date             AS observation_datetime
--        ,case when lab_result_source = 'OD' then 5001 
--        else 44818702 end as OBSERVATION_TYPE_CONCEPT_ID
            ,
            nvl(xw2.target_concept_id, 0) AS observation_type_concept_id,
            lab.result_num              AS value_as_number,
            lab.lab_loinc               AS value_as_string,
            4188539 AS value_as_concept_id --ssh observation of the measurement -- recorded as yes observation 6/26/2020
            ,
            CASE
                WHEN lower(TRIM(result_qual)) = 'positive'             THEN
                    45884084
                WHEN lower(TRIM(result_qual)) = 'negative'             THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'pos'                  THEN
                    45884084
                WHEN lower(TRIM(result_qual)) = 'neg'                  THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'presumptive positive' THEN
                    45884084
                WHEN lower(TRIM(result_qual)) = 'presumptive negative' THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'detected'             THEN
                    45884084
                WHEN lower(TRIM(result_qual)) = 'not detected'         THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'inconclusive'         THEN
                    45877990
                WHEN lower(TRIM(result_qual)) = 'normal'               THEN
                    45884153
                WHEN lower(TRIM(result_qual)) = 'abnormal'             THEN
                    45878745
                WHEN lower(TRIM(result_qual)) = 'low'                  THEN
                    45881666
                WHEN lower(TRIM(result_qual)) = 'high'                 THEN
                    45876384
                WHEN lower(TRIM(result_qual)) = 'borderline'           THEN
                    45880922
                WHEN lower(TRIM(result_qual)) = 'elevated'             THEN
                    4328749  --ssh add issue number 55 - 6/26/2020
                WHEN lower(TRIM(result_qual)) = 'undetermined'         THEN
                    45880649
                WHEN lower(TRIM(result_qual)) = 'undetectable'         THEN
                    45878583
                WHEN lower(TRIM(result_qual)) = 'un'                   THEN
                    0
                WHEN lower(TRIM(result_qual)) = 'unknown'              THEN
                    0
                WHEN lower(TRIM(result_qual)) = 'no information'       THEN
                    46237210
                ELSE
                    45877393
            END AS qualifier_concept_id --null/un/elevated/boderline/ot/low/high/normal/negative/positive/abnormal
            ,
            u.target_concept_id         AS unit_concept_id,
            p.n3cds_domain_map_id       AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            lab.lab_loinc               AS observation_source_value,
            xw.source_code_concept_id   AS observation_source_concept_id,
            lab.result_unit             AS unit_source_value,
            lab.result_modifier         AS qualifier_source_value,
            'PCORNET_LAB_RESULT_CM' AS domain_source
        --select lab.*, xw.* --7000/22
        FROM
            native_pcornet51_cdm.lab_result_cm     lab
            JOIN cdmh_staging.person_clean              pc ON lab.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = lab.lab_result_cm_id
                                                     AND mp.domain_name = 'LAB_RESULT_CM'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = lab.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = lab.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON lab.lab_loinc = xw.src_code
                                                            AND xw.cdm_tbl = 'LAB_RESULT_CM'
                                                            AND xw.target_domain_id = 'Observation'
                                                            AND xw.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.p2o_medadmin_term_xwalk   u ON lab.result_unit = u.src_code
                                                                AND u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'
            LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON lab.lab_result_source = xw2.src_code
                                                         AND xw2.cdm_tbl = 'LAB_RESULT_CM'
                                                         AND xw2.cdm_tbl_column_name = 'LAB_RESULT_SOURCE';

    observation_recordcount := SQL%rowcount;
    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_measurement (--for lab_result_cm
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
            xw.target_concept_id     AS measurement_concept_id, --concept id for labTest/ABO and Rh group [Type] in Blood
            lab.result_date          AS measurement_date,
            lab.result_date          AS measurement_datetime,
            lab.result_time          AS measurement_time,
            nvl(xw2.target_concept_id, 0) AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            lab.result_num           AS value_as_number, --result_num
            --- add blood type result concept id here
--            bloodresult.target_concept_id   AS value_as_concept_id,
            CASE
                WHEN lower(nvl(result_qual, 'OT')) = 'ot' THEN
                    bloodresult1.target_concept_id
                ELSE
                    bloodresult.target_concept_id
            END AS value_as_concept_id,
            u.target_concept_id      AS unit_concept_id,
            NULL AS range_low,
            NULL AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            lab.lab_loinc            AS measurement_source_value,
            xw2.target_concept_id    AS measurement_source_concept_id,
            lab.result_unit          AS unit_source_value,
--            nvl(lab.raw_result, decode(lab.result_num, 0, lab.result_qual)) AS value_source_value,
            CASE
                WHEN lower(nvl(result_qual, 'OT')) = 'ot' THEN
                    lab.raw_result
                ELSE
                    lab.result_qual
            END AS  value_source_value,
            'PCORNET_LAB_RESULT_CM' AS domain_source
        FROM
            native_pcornet51_cdm.lab_result_cm     lab
            JOIN cdmh_staging.person_clean              pc ON lab.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = lab.lab_result_cm_id
                                                     AND mp.domain_name = 'LAB_RESULT_CM'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = lab.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = lab.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON lab.lab_loinc = xw.src_code
                                                            AND xw.cdm_tbl = 'LAB_RESULT_CM'
                                                            AND xw.target_domain_id = 'Measurement'
                                                            AND xw.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.p2o_medadmin_term_xwalk   u ON lab.result_unit = u.src_code
                                                                AND u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'
            LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON lab.lab_result_source = xw2.src_code
                                                         AND xw2.cdm_tbl = 'LAB_RESULT_CM'
                                                         AND xw2.cdm_tbl_column_name = 'LAB_RESULT_SOURCE'
            LEFT JOIN cdmh_staging.p2o_term_xwalk            bloodresult ON lower(lab.result_qual) = lower(bloodresult.src_code)
                                                                 AND bloodresult.cdm_tbl = 'LAB_RESULT_CM'
                                                                 AND bloodresult.cdm_tbl_column_name = 'RESULT_QUAL'
            LEFT JOIN cdmh_staging.p2o_term_xwalk            bloodresult1 ON lower(lab.raw_result) = lower(bloodresult1.src_code)
                                                                  AND bloodresult1.cdm_tbl = 'LAB_RESULT_CM'
                                                                  AND bloodresult1.cdm_tbl_column_name = 'RAW_RESULT'
        WHERE
            lab.lab_loinc IN (
                '882-1'
            );

    bloodcount := SQL%rowcount;
    COMMIT;
    recordcount := measure_recordcount + observation_recordcount + bloodcount;
    dbms_output.put_line(recordcount || ' PCORnet LAB_RESULT_CM source data inserted to MEASUREMENT staging table, ST_OMOP53_MEASUREMENT, successfully.'
    );
END sp_p2o_src_lab_result_cm;
