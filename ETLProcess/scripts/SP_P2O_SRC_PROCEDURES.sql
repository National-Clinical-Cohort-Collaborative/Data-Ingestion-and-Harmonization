
CREATE PROCEDURE                                                                  CDMH_STAGING.SP_P2O_SRC_PROCEDURES (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_PROCEDURES
     Purpose:    Loading The NATIVE_PCORNET51_CDM.PROCEDURES Table into 
                1. CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE
                2. CDMH_STAGING.ST_OMOP53_MEASUREMENT
                3. CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
                4. CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1         8/30/20     SHONG               Intial Version.
	   0.2       6/25/2020     TZHANG              Added logic to insert into CDMH_STAGING.ST_OMOP53_MEASUREMENT
	   0.3       6/25/2020     TZHANG              Added logic to insert into CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
     0.4         6/26/2020     TZHANG              Added logic to insert into CDMH_STAGING.ST_OMOP53_DEVICE_EXPOSURE
                                                                            CDMH_STAGING.ST_OMOP53_OBSERVATION
    0.5         9/9/2020    DIH                    Updated the *_type_concept_id logic  
    
    
*********************************************************************************************************/

--obs_recordCount number;
    proc_recordcount      NUMBER;
    measure_recordcount   NUMBER;
    drug_recordcount      NUMBER;
    device_recordcount    NUMBER;
    obs_recordcount       NUMBER;
BEGIN
    DELETE FROM cdmh_staging.st_omop53_procedure_occurrence
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_PROCEDURES';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_procedure_occurrence (
        data_partner_id,
        manifest_id,
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        modifier_concept_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        procedure_source_value,
        procedure_source_concept_id,
        modifier_source_value,
        domain_source
    )
        SELECT
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS procedure_occurrence_id,
            p.n3cds_domain_map_id       AS person_id,
            xw.target_concept_id        AS procedure_concept_id,
            pr.px_date                  AS procedure_date,
            NULL AS procedure_datetime,
--            CASE
--                WHEN pr.px_source = 'BI'  THEN
--                    257
--                WHEN pr.px_source = 'CL'  THEN
--                    32468
--                WHEN pr.px_source = 'OD'  THEN
--                    38000275 --ORDER /EHR 
--                WHEN pr.px_source = 'UN'  THEN
--                    0 --UN This is not a type concept and it really has no value,  so set to 0 / do not use 45877986 for UN - 6/18/20 SSH
--                WHEN pr.px_source = 'NI'  THEN
--                    46237210
--                WHEN pr.px_source = 'OT'  THEN
--                    45878142
--                ELSE
--                    0
--            END AS procedure_type_concept_id, -- use this type concept id for ehr order list
            NVL(xw2.target_concept_id,0) AS procedure_type_concept_id,
            0 modifier_concept_id, -- need to create a cpt_concept_id table based on the source_code_concept id
            NULL AS quantity,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            xw.src_code                 AS procedure_source_value,
            xw.source_code_concept_id   AS procedure_source_concept_id,
            xw.src_code_type            AS modifier_source_value,
            'PCORNET_PROCEDURES' AS domain_source
        FROM
            native_pcornet51_cdm.procedures        pr
            JOIN cdmh_staging.person_clean              pc ON pr.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = pr.proceduresid
                                                     AND mp.domain_name = 'PROCEDURES'
                                                     AND mp.target_domain_id = 'Procedure'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = pr.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = pr.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid 
    --LEFT JOIN CDMH_STAGING.visit_xwalk vx ON vx.cdm_tbl='ENCOUNTER' AND vx.CDM_NAME='PCORnet' AND vx.src_visit_type=d.ENC_TYPE
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON pr.px = xw.src_code
                                                                 AND xw.cdm_tbl = 'PROCEDURES'
                                                                 AND xw.target_domain_id = 'Procedure'
                                                                 AND xw.target_concept_id = mp.target_concept_id
                                                                 AND xw.src_code_type = pr.px_type
             LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON pr.px_source = xw2.src_code
                                                         AND xw2.cdm_tbl = 'PROCEDURES'
                                                         AND xw2.cdm_tbl_column_name = 'PX_SOURCE'                                                                   
                                                                 ;

    proc_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_measurement
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_PROCEDURES';

    COMMIT;
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
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS measurement_id,
            p.n3cds_domain_map_id       AS person_id,
            xw.target_concept_id        AS measurement_concept_id,
            pr.px_date                  AS measurement_date,
            pr.px_date                  AS measurement_datetime,
            NULL AS measurement_time, 
--            null as measurement_type_concept_id, --TBD, do we have a concept id to indicate 'procedure' in measurement
--            CASE
--                WHEN pr.px_source = 'OD'  THEN
--                    38000179
--                WHEN pr.px_source = 'BI'  THEN
--                    38000177
--                WHEN pr.px_source = 'CL'  THEN
--                    38000177
--                ELSE
--                    45769798
--            END AS measurement_type_concept_id,
            NVL(xw2.target_concept_id,0) AS measurement_type_concept_id,
            NULL AS operator_concept_id,
            NULL AS value_as_number, --result_num
            NULL AS value_as_concept_id,
            NULL AS unit_concept_id,
            NULL AS range_low,
            NULL AS range_high,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            pr.px                       AS measurement_source_value,
            xw.source_code_concept_id   AS measurement_source_concept_id,
            NULL AS unit_source_value,
            NULL AS value_source_value,
            'PCORNET_PROCEDURES' AS domain_source
        FROM
            native_pcornet51_cdm.procedures        pr
            JOIN cdmh_staging.person_clean              pc ON pr.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = pr.proceduresid
                                                     AND mp.domain_name = 'PROCEDURES'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = pr.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = pr.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid 
--        LEFT JOIN CDMH_STAGING.N3cds_Domain_Map prv on prv.Source_Id=e.source_id.providerid AND prv.Domain_Name='PROVIDER' AND prv.DATA_PARTNER_ID=DATAPARTNERID 
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON pr.px = xw.src_code
                                                                 AND xw.cdm_tbl = 'PROCEDURES'
                                                                 AND xw.target_domain_id = 'Measurement'
                                                                 AND xw.target_concept_id = mp.target_concept_id
                                                                 AND xw.src_code_type = pr.px_type
             LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON pr.px_source = xw2.src_code
                                                         AND xw2.cdm_tbl = 'PROCEDURES'
                                                         AND xw2.cdm_tbl_column_name = 'PX_SOURCE'                                                                   
                                                                 ;

    measure_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_drug_exposure
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_PROCEDURES';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_drug_exposure (
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
        stop_reason,
        refills,
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
        domain_source
    )
        SELECT
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS drug_exposure_id,
            p.n3cds_domain_map_id       AS person_id,
            xw.target_concept_id        AS drug_concept_id,
            pr.px_date                  AS drug_exposure_start_date,
            pr.px_date                  AS drug_exposure_start_datetime,
            pr.px_date                  AS drug_exposure_end_date,
            NULL AS drug_exposure_end_datetime,
            NULL AS verbatim_end_date,
    --xw2.TARGET_CONCEPT_ID as drug_type_concept_id,
--            38000179 AS drug_type_concept_id,
            NVL(xw2.target_concept_id,0) AS drug_type_concept_id,
            NULL AS stop_reason,
            NULL AS refills,
            NULL AS quantity,
            NULL AS days_supply,
            NULL AS sig,
            NULL AS route_concept_id,
            NULL AS lot_number,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            pr.px                      AS drug_source_value,
            xw.source_code_concept_id   AS drug_source_concept_id,
            NULL AS route_source_value,
            NULL AS dose_unit_source_value,
            'PCORNET_PROCEDURES' AS domain_source
        FROM
            native_pcornet51_cdm.procedures        pr
            JOIN cdmh_staging.person_clean              pc ON pr.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = pr.proceduresid
                                                     AND mp.domain_name = 'PROCEDURES'
                                                     AND mp.target_domain_id = 'Drug'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = pr.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = pr.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
--this is to look for drug_concept_id line 27 can use mp.target_concept_id, makes no difference
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON pr.px = xw.src_code
                                                                 AND xw.cdm_tbl = 'PROCEDURES'
                                                                 AND xw.target_domain_id = 'Drug'
                                                                 AND xw.target_concept_id = mp.target_concept_id
                                                                 AND xw.src_code_type = pr.px_type
            LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON pr.px_source = xw2.src_code
                                                         AND xw2.cdm_tbl = 'PROCEDURES'
                                                         AND xw2.cdm_tbl_column_name = 'PX_SOURCE'                                                       
                                                                 ;

    drug_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_device_exposure
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_PROCEDURES';

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
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS device_exposure_id,
            p.n3cds_domain_map_id       AS person_id,
            xw.target_concept_id        AS device_concept_id,
            pr.px_date                  AS device_exposure_start_date,
            pr.px_date                  AS device_exposure_start_datetime,
            NULL AS device_exposure_end_date,
            NULL AS device_exposure_end_datetime,
--            44818707 AS device_type_concept_id,
            NVL(xw2.target_concept_id,0) AS device_type_concept_id,
            NULL AS unique_device_id,
            NULL AS quantity,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            pr.px                       AS device_source_value,
            xw.source_code_concept_id   AS device_source_concept_id,
            'PCORNET_PROCEDURES' AS domain_source
        FROM
            native_pcornet51_cdm.procedures        pr
            JOIN cdmh_staging.person_clean              pc ON pr.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = pr.proceduresid
                                                     AND mp.domain_name = 'PROCEDURES'
                                                     AND mp.target_domain_id = 'Device'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = pr.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = pr.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON pr.px = xw.src_code
                                                                 AND xw.cdm_tbl = 'PROCEDURES'
                                                                 AND xw.target_domain_id = 'Device'
                                                                 AND xw.target_concept_id = mp.target_concept_id
                                                                 AND xw.src_code_type = pr.px_type
            LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON pr.px_source = xw2.src_code
                                                         AND xw2.cdm_tbl = 'PROCEDURES'
                                                         AND xw2.cdm_tbl_column_name = 'PX_SOURCE'                                                    
                                                                 ;

    device_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_observation
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_PROCEDURES';

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
            pr.px_date                  AS observation_date,
            pr.px_date                  AS observation_datetime,
            NVL(xw2.target_concept_id,0)       AS observation_type_concept_id,
            NULL AS value_as_number,
            NULL AS value_as_string,
            NULL AS value_as_concept_id,
            NULL AS qualifier_concept_id,
            NULL AS unit_concept_id,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            pr.px                       AS observation_source_value,
            xw.source_code_concept_id   AS observation_source_concept_id,
            NULL AS unit_source_value,
            NULL AS qualifier_source_value,
            'PCORNET_PROCEDURES' AS domain_source
        FROM
            native_pcornet51_cdm.procedures        pr
            JOIN cdmh_staging.person_clean              pc ON pr.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = pr.proceduresid
                                                     AND mp.domain_name = 'PROCEDURES'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = pr.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = pr.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON pr.px = xw.src_code
                                                                 AND xw.cdm_tbl = 'PROCEDURES'
                                                                 AND xw.target_domain_id = 'Observation'
                                                                 AND xw.target_concept_id = mp.target_concept_id
                                                                 AND xw.src_code_type = pr.px_type
            LEFT JOIN cdmh_staging.p2o_term_xwalk            xw2 ON pr.px_source = xw2.src_code
                                                         AND xw2.cdm_tbl = 'PROCEDURES'
                                                         AND xw2.cdm_tbl_column_name = 'PX_SOURCE';

    obs_recordcount := SQL%rowcount;
    COMMIT;
    recordcount := proc_recordcount + measure_recordcount + drug_recordcount + device_recordcount + obs_recordcount;
    dbms_output.put_line(recordcount || ' PCORnet PROCEDURE source data inserted to PROCEDURE staging table-ST_OMOP53_PROCEDURE_OCCURRENCE, 
                                         Measurement staging table-ST_OMOP53_MEASUREMENT, 
                                         DRUG staging table-ST_OMOP53_DRUG_EXPOSURE,
                                         Observation staging table-ST_OMOP53_OBSERVATION,
                                         Device staging table-ST_OMOP53_DEVICE_EXPOSURE,
                                         ,successfully.'
    );
END sp_p2o_src_procedures;
