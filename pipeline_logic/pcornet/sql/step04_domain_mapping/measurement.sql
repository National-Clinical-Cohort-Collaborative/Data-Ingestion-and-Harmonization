CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/04 - domain mapping/measurement` AS

with condition as (
    SELECT 
        patid AS site_patid, 
        CAST(xw.target_concept_id as int) as measurement_concept_id,
        CAST(c.report_date as date) as measurement_date,
        -- c.report_date as measurement_datetime, 
        CAST(null as timestamp) as measurement_datetime, --** MB: no time info
        CAST(null as string) measurement_time,
        -- case when c.condition_source = 'HC' then 38000245 --HC=Healthcare  problem  list/EHR problem list entry
        --     when c.condition_source ='PR' then 45905770 --PR : patient reported
        --     when c.condition_source = 'NI' then 46237210
        --     when c.condition_source = 'OT' then 45878142
        --     when c.condition_source = 'UN' then 0 --charles mapping doc ref. Unknown (concept_id = 45877986/charles mapping doc ref. Unknown (concept_id = 45877986)
        --     when c.condition_source in ('RG', 'DR', 'PC') then 0 ---PC    PC=PCORnet-defined  condition  algorithm       See mapping comments,
        --     ELSE 0
        --     END AS measurement_type_concept_id,
        32817 AS measurement_type_concept_id,
        CAST(null as int) as operator_concept_id,
        CAST(null as float) as value_as_number, --result_num
        CAST(null as int) as value_as_concept_id,
        CAST(null as int) as unit_concept_id,
        CAST(null as float) as range_low,
        CAST(null as float) as range_high,
        CAST(null as int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) as visit_detail_id,
        CAST(c.condition as string) as measurement_source_value,
        CAST(xw.source_concept_id as int) as measurement_source_concept_id,
        CAST(null as string) as unit_source_value,
        CAST(null as string) as value_source_value,
        'CONDITION' as domain_source,
        conditionid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/condition` c
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Measurement'
            AND c.condition = xw.src_code
            -- AND xw.target_concept_id=mp.target_concept_id 
            AND xw.src_code_type = c.condition_type
),

diagnosis as (
    SELECT
        patid AS site_patid,  
        CAST(xw.target_concept_id as int) as measurement_concept_id,
        CAST(COALESCE(d.dx_date, d.admit_date) as date) as measurement_date, 
        -- COALESCE(d.dx_date, d.admit_date) as measurement_datetime,
        CAST(null as timestamp) as measurement_datetime, --** MB: no time info
        CAST(null as string) measurement_time,
        -- null AS measurement_type_concept_id, 
        -- 5001 AS measurement_type_concept_id, --default values from draft mappings spreadsheet --added on 6/26
        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) as measurement_type_concept_id,
        CAST(null as int) as operator_concept_id,
        CAST(null as float) as value_as_number,
        CAST(null as int) as value_as_concept_id,
        CAST(null as int) as unit_concept_id,
        CAST(null as float) as range_low,
        CAST(null as float) as range_high,
        CAST(null as int) as provider_id,
        encounterid as site_encounterid,
        CAST(null as int) as visit_detail_id,
        CAST(d.dx as string) as measurement_source_value,
        CAST(xw.source_concept_id as int) as measurement_source_concept_id,
        CAST(null as string) as unit_source_value,
        CAST(null as string) as value_source_value,
        'DIAGNOSIS' as domain_source,
        diagnosisid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/diagnosis` d
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Measurement' 
            AND d.dx = xw.src_code 
            -- AND xw.target_concept_id = mp.target_concept_id
            AND xw.src_code_type = d.dx_type
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
            ON d.dx_origin = xw2.SRC_CODE
            AND xw2.CDM_TBL = 'DIAGNOSIS'
            AND xw2.CDM_TBL_COLUMN_NAME = 'dx_origin'
),

lab_result_cm as (
    SELECT
        patid AS site_patid,  
        CAST(xw.target_concept_id as int) as measurement_concept_id, --concept id for lab - measurement
        CAST(lab.result_date as date) as measurement_date, 
        CAST(lab.RESULT_DATETIME as timestamp) as measurement_datetime,
        CAST(lab.result_time as string) as measurement_time, 
        -- case when lab.lab_result_source ='OD' then 5001
        --     when lab.lab_result_source ='BI' then 32466
        --     when lab.lab_result_source ='CL' then 32466
        --     when lab.lab_result_source ='DR' then 45754907
        --     when lab.lab_result_source ='NI' then 46237210
        --     when lab.lab_result_source ='UN' then 45877986
        --     when lab.lab_result_source ='OT' then 45878142
        --     else 0 end AS measurement_type_concept_id,  -----lab_result_source determines the ------------
        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS measurement_type_concept_id,
        CAST(null as int) as operator_concept_id,
        CAST(lab.result_num as float) as value_as_number, --result_num
        CAST(xw3.TARGET_CONCEPT_ID as int) as value_as_concept_id,
        CAST(u.TARGET_CONCEPT_ID as int) as unit_concept_id,
        ---lab.NORM_range_low as range_low, -- non-numeric fields are found
        ---lab.NORM_range_high as range_high, -- non-numeric data will error on a insert, omop define this column as float
        -- case when LENGTH(TRIM(TRANSLATE(NORM_range_low, ' +-.0123456789', ' '))) is null Then cast(NORM_range_low as float) else 0 end as range_low,
        -- case when LENGTH(TRIM(TRANSLATE(NORM_range_high, ' +-.0123456789', ' '))) is null Then cast(NORM_range_high as integer ) else 0 end as range_high,
        -- CASE 
        --     WHEN ISNULL(lab.nomr_range_low) OR ISNAN(cast(lab.nomr_range_low as float))
        --         THEN CAST(0 as float) 
        --     ELSE 
        --         CAST(norm_range_low as float) 
        -- END as range_low,
        -- CASE 
        --     WHEN ISNULL(lab.norm_range_high) OR ISNAN(cast(lab.norm_range_high as float))
        --         THEN CAST(0 as float)
        --     ELSE 
        --         CAST(norm_range_high as float) 
        -- END as range_high,
        COALESCE(CAST(norm_range_low as float), 0) as range_low,
        COALESCE(CAST(norm_range_high as float), 0) as range_high,
        CAST(null as int) as provider_id,
        encounterid as site_encounterid,
        CAST(null as int) as visit_detail_id,
        CAST(COALESCE(lab.lab_loinc, lab.raw_lab_code ) as string) as measurement_source_value,
        -- cast(xw.source_concept_id as int ) as measurement_source_concept_id,
        0 as measurement_source_concept_id,
        CAST(lab.result_unit as string) as unit_source_value,
        CAST(COALESCE(lab.result_qual, lab.raw_result) as string) as value_source_value,
        'LAB_RESULT_CM' AS domain_source,
        lab_result_cm_id as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/lab_result_cm` lab
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
        ON xw.CDM_TBL = 'LAB_RESULT_CM' AND xw.target_domain_id = 'Measurement' 
        AND lab.lab_loinc = xw.src_code
        AND xw.src_code_type = 'LOINC'
        -- AND xw.target_concept_id=mp.target_concept_id
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` u 
        ON u.CDM_TBL_COLUMN_NAME = 'RX_DOSE_ORDERED_UNIT'
        AND lab.result_unit = u.SRC_CODE 
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
        ON xw2.CDM_TBL = 'LAB_RESULT_CM' AND xw2.CDM_TBL_COLUMN_NAME = 'LAB_RESULT_SOURCE'
        AND upper(trim(lab.lab_result_source)) = upper(trim(xw2.SRC_CODE))
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw3
        ON xw3.CDM_TBL = 'LAB_RESULT_CM' AND xw3.CDM_TBL_COLUMN_NAME = 'RESULT_QUAL'
        AND upper(trim(lab.result_qual)) = upper(xw3.SRC_CODE)
    where lab.lab_loinc NOT IN ( '882-1')

    UNION ALL 
    --map blood type that is passed in the raw_result 
       SELECT
        patid AS site_patid,  
        CAST(xw.target_concept_id as int) as measurement_concept_id, --concept id for lab - measurement
        CAST(lab.result_date as date) as measurement_date, 
        CAST(lab.RESULT_DATETIME as timestamp) as measurement_datetime,
        CAST(lab.result_time as string) as measurement_time, 

        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS measurement_type_concept_id,
        CAST(null as int) as operator_concept_id,
        CAST(lab.result_num as float) as value_as_number, --result_num

        -- Blood type is passed in in the raw_result column
        CAST( COALESCE(bloodresult.TARGET_CONCEPT_ID, xw3.TARGET_CONCEPT_ID ) as INT) as value_as_concept_id,

        CAST(u.TARGET_CONCEPT_ID as int) as unit_concept_id,

        COALESCE(CAST(norm_range_low as float), 0) as range_low,
        COALESCE(CAST(norm_range_high as float), 0) as range_high,
        CAST(null as int) as provider_id,
        encounterid as site_encounterid,
        CAST(null as int) as visit_detail_id,
        CAST(lab.lab_loinc as string) as measurement_source_value,
        -- cast(xw.source_concept_id as int ) as measurement_source_concept_id,
        0 as measurement_source_concept_id,
        CAST(lab.result_unit as string) as unit_source_value,
        
        --updated based on the value_as_concept_id logic
        CAST(COALESCE( lab.raw_result, lab.result_qual) as string)  as value_source_value,

        'LAB_RESULT_CM' AS domain_source,
        lab_result_cm_id as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/lab_result_cm` lab
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
        ON xw.CDM_TBL = 'LAB_RESULT_CM' AND xw.target_domain_id = 'Measurement' 
        AND lab.lab_loinc = xw.src_code
        -- AND xw.target_concept_id=mp.target_concept_id
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` u 
        ON u.CDM_TBL_COLUMN_NAME = 'RX_DOSE_ORDERED_UNIT'
        AND lab.result_unit = u.SRC_CODE 
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
        ON xw2.CDM_TBL = 'LAB_RESULT_CM' AND xw2.CDM_TBL_COLUMN_NAME = 'LAB_RESULT_SOURCE'
        AND upper(trim(lab.lab_result_source)) = upper(trim(xw2.SRC_CODE))
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw3
        ON xw3.CDM_TBL = 'LAB_RESULT_CM' AND xw3.CDM_TBL_COLUMN_NAME = 'RESULT_QUAL'
        AND upper(trim(lab.result_qual)) = upper(xw3.SRC_CODE)

    --blood type is passed in raw_result
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` bloodresult
        ON bloodresult.CDM_TBL = 'LAB_RESULT_CM' AND bloodresult.CDM_TBL_COLUMN_NAME = 'RAW_RESULT'
        AND upper(trim(lab.raw_result)) = upper(trim(bloodresult.SRC_CODE))
    where lab.lab_loinc IN ( '882-1')

),

obs_clin as (
    SELECT
        patid AS site_patid, 
        CAST(xw.target_concept_id as int) AS measurement_concept_id,
        CAST(obs.obsclin_start_date as date) AS measurement_date,
        CAST(obs.OBSCLIN_START_DATETIME as timestamp) AS measurement_datetime,
        CAST(obs.obsclin_start_time as string) AS measurement_time,
        -- CASE
        --     WHEN obs.obsclin_type = 'LC' THEN
        --         37079395
        --     WHEN obs.obsclin_type = 'SM' THEN
        --         37079429
        --     ELSE
        --         0
        -- END AS measurement_type_concept_id,
        32817 AS measurement_type_concept_id,
        CAST(null as int) AS operator_concept_id,
        CAST(obs.obsclin_result_num as float) AS value_as_number,
        CAST(null as int) AS value_as_concept_id,
        CAST(u.TARGET_CONCEPT_ID as int) AS unit_concept_id,
        CAST(null as float) AS range_low,
        CAST(null as float) AS range_high,
        CAST(null as int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(obs.obsclin_code as string) AS measurement_source_value,
        CAST(xw.target_concept_id as int) AS measurement_source_concept_id,
        CAST(obs.obsclin_result_unit as string) AS unit_source_value,
        CAST(obs.obsclin_result_qual as string) AS value_source_value,
        'OBS_CLIN' AS domain_source,
        obsclinid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/obs_clin` obs
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'OBS_CLIN' AND xw.target_domain_id = 'Measurement'
            AND obs.obsclin_code = xw.src_code
            AND obs.mapped_obsclin_type = xw.src_vocab_code
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` u 
            ON obs.obsclin_result_unit = u.SRC_CODE
            AND u.CDM_TBL_COLUMN_NAME = 'RX_DOSE_ORDERED_UNIT'
),

procedures as (
    SELECT
        patid AS site_patid, 
        CAST(xw.target_concept_id as int) AS measurement_concept_id,
        CAST(pr.px_date as date) AS measurement_date,
        -- pr.px_date AS measurement_datetime,
        CAST(null as timestamp) as measurement_datetime, --** MB: no time info
        CAST(null as string) measurement_time,
        -- null as measurement_type_concept_id, --TBD, do we have a concept id to indicate 'procedure' in measurement
        -- CASE
        --     WHEN pr.px_source = 'OD'  THEN
        --         38000179
        --     WHEN pr.px_source = 'BI'  THEN
        --         38000177
        --     WHEN pr.px_source = 'CL'  THEN
        --         38000177
        --     ELSE
        --         45769798
        -- END AS measurement_type_concept_id,
        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS measurement_type_concept_id,
        CAST(null as int) AS operator_concept_id,
        CAST(null as float) AS value_as_number, --result_num
        CAST(null as int) AS value_as_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as float) AS range_low,
        CAST(null as float) AS range_high,
        CAST(null as int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(pr.px as string) AS measurement_source_value,
        CAST(xw.source_concept_id as int) AS measurement_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS value_source_value,
        'PROCEDURES' AS domain_source,
        proceduresid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/procedures` pr
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'PROCEDURES' AND xw.target_domain_id = 'Measurement'
            AND pr.px = xw.src_code
            AND xw.src_code_type = pr.px_type
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
            ON xw2.CDM_TBL = 'PROCEDURES' AND xw2.CDM_TBL_COLUMN_NAME = 'PX_SOURCE'     
            AND pr.px_source = xw2.SRC_CODE
), 

vital as (
-- Height
    SELECT
        patid AS site_patid, 
        4177340 AS measurement_concept_id, --concept id for Height, from notes
        CAST(v.measure_date as date) AS measurement_date,
        CAST(v.MEASURE_DATETIME as timestamp) AS measurement_datetime,
        CAST(v.measure_time as string) AS measurement_time,
        CAST(vt.TARGET_CONCEPT_ID as int) AS measurement_type_concept_id,
        CAST(null as int) AS operator_concept_id,
        CAST(v.ht as float) AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
        CAST(null as int) AS value_as_concept_id,
        9327 AS unit_concept_id,
        CAST(null as float) AS range_low,
        CAST(null as float) AS range_high,
        CAST(null as int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        'Height in inches' AS measurement_source_value,
        CAST(null as int) AS measurement_source_concept_id,
        'Inches' AS unit_source_value,
        CAST(v.ht as string) AS value_source_value,
        'VITAL' AS domain_source,
        vitalid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/vital` v
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` vt 
            ON vt.CDM_TBL = 'VITAL' AND vt.CDM_TBL_COLUMN_NAME = 'VITAL_SOURCE'
            AND vt.SRC_CODE = v.vital_source
    WHERE v.ht IS NOT NULL --** MB: is this the best way to filter to only the rows we want, now that we don't inner join on domain_map? 

UNION ALL

-- Weight
    SELECT
        patid AS site_patid, 
        4099154 AS measurement_concept_id, --concept id for Weight, from notes
        CAST(v.measure_date as date) AS measurement_date,
        CAST(v.MEASURE_DATETIME as timestamp) AS measurement_datetime,
        CAST(v.measure_time as string) AS measurement_time,
        CAST(vt.TARGET_CONCEPT_ID as int) AS measurement_type_concept_id,
        CAST(null as int) AS operator_concept_id,
        CAST(v.wt as float) AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
        CAST(null as int) AS value_as_concept_id,
        8739 AS unit_concept_id,
        CAST(null as float) AS range_low,
        CAST(null as float) AS range_high,
        CAST(null as int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        'Weight in pounds' AS measurement_source_value,
        CAST(null as int) AS measurement_source_concept_id,
        'Pounds' AS unit_source_value,
        v.wt AS value_source_value,
        'VITAL' AS domain_source,
        vitalid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/vital` v
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` vt 
            ON vt.CDM_TBL = 'VITAL' AND vt.CDM_TBL_COLUMN_NAME = 'VITAL_SOURCE'
            AND vt.SRC_CODE = v.vital_source
    WHERE v.wt IS NOT NULL

UNION ALL

-- Diastolic BP
    SELECT
        patid AS site_patid, 
        CAST(bp.TARGET_CONCEPT_ID as int) AS measurement_concept_id, --concept id for DBPs, from notes
        CAST(v.measure_date as date) AS measurement_date,
        CAST(v.MEASURE_DATETIME as timestamp) AS measurement_datetime,
        CAST(v.measure_time as string) AS measurement_time,
        CAST(vt.TARGET_CONCEPT_ID as int) AS measurement_type_concept_id,
        CAST(null as int) AS operator_concept_id,
        CAST(v.diastolic as float) AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
        CAST(null as int) AS value_as_concept_id,
        8876 AS unit_concept_id,
        CAST(null as float) AS range_low,
        CAST(null as float) AS range_high,
        CAST(null as int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(bp.TARGET_CONCEPT_NAME as string) AS measurement_source_value,
        CAST(null as int) AS measurement_source_concept_id,
        'millimeter mercury column' AS unit_source_value,
        CAST(v.diastolic as string) AS value_source_value,
        'VITAL' AS domain_source,
        vitalid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/vital` v
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` bp 
            ON bp.CDM_TBL = 'VITAL' AND bp.CDM_TBL_COLUMN_NAME = 'DIASTOLIC_BP_POSITION'
            AND bp.SRC_CODE = v.bp_position
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` vt 
            ON vt.CDM_TBL = 'VITAL' AND vt.CDM_TBL_COLUMN_NAME = 'VITAL_SOURCE'
            AND vt.SRC_CODE = v.vital_source
    WHERE v.diastolic IS NOT NULL

UNION ALL

-- Systolic BP
    SELECT
        patid AS site_patid, 
        CAST(bp.TARGET_CONCEPT_ID as int) AS measurement_concept_id, --concept id for SBPs, from notes
        CAST(v.measure_date as date) AS measurement_date,
        -- CAST(null as timestamp) AS measurement_datetime, --** MB: why is this null?
        CAST(v.MEASURE_DATETIME as timestamp) AS measurement_datetime,
        CAST(v.measure_time as string) AS measurement_time,
        CAST(vt.TARGET_CONCEPT_ID as int) AS measurement_type_concept_id,
        CAST(null as int) AS operator_concept_id,
        CAST(v.systolic as float) AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
        CAST(null as int) AS value_as_concept_id,
        8876 AS unit_concept_id,
        CAST(null as float) AS range_low,
        CAST(null as float) AS range_high,
        CAST(null as int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(bp.TARGET_CONCEPT_NAME as string) AS measurement_source_value,
        CAST(null as int) AS measurement_source_concept_id,
        'millimeter mercury column' AS unit_source_value,
        CAST(v.systolic as string) AS value_source_value,
        'VITAL' AS domain_source,
        vitalid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/vital` v
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` bp 
            ON bp.CDM_TBL = 'VITAL' AND bp.CDM_TBL_COLUMN_NAME = 'SYSTOLIC_BP_POSITION'
            AND bp.SRC_CODE = v.bp_position
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` vt 
            ON vt.CDM_TBL = 'VITAL' AND vt.CDM_TBL_COLUMN_NAME = 'VITAL_SOURCE'
            AND vt.SRC_CODE = v.vital_source
    WHERE v.systolic IS NOT NULL

UNION ALL

-- Original BMI
    SELECT
        patid AS site_patid, 
        4245997 AS measurement_concept_id, --concept id for BMI, from notes
        CAST(v.measure_date as date) AS measurement_date,
        -- CAST(null as timestamp) AS measurement_datetime, --** MB: why is this null?
        CAST(v.MEASURE_DATETIME as timestamp) AS measurement_datetime,
        CAST(v.measure_time as string) AS measurement_time,
        CAST(vt.TARGET_CONCEPT_ID as int) AS measurement_type_concept_id,
        CAST(null as int) AS operator_concept_id,
        CAST(v.original_bmi as float) AS value_as_number, --Height (in inches) Weight (in pounds) Diastolic blood pressure (in mmHg)
        CAST(null as int) AS value_as_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as float) AS range_low,
        CAST(null as float) AS range_high,
        CAST(null as int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        'Original BMI' AS measurement_source_value,
        CAST(null as int) AS measurement_source_concept_id,
        CAST(null as string) unit_source_value,
        CAST(v.original_bmi as string) AS value_source_value,
        'VITAL' AS domain_source,
        vitalid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/vital` v
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` vt 
            ON vt.CDM_TBL = 'VITAL'
            AND vt.CDM_TBL_COLUMN_NAME = 'VITAL_SOURCE'
            AND vt.SRC_CODE = v.vital_source
    WHERE v.original_bmi IS NOT NULL
),

final_table as (
    -- Finding only unique measurements, per discussion with Matvey Palchuk + Kristin Kosta 13-08-2020
    SELECT DISTINCT * 
    FROM (
        SELECT * FROM condition 
            UNION ALL  
        SELECT * FROM diagnosis 
            UNION ALL 
        SELECT * FROM lab_result_cm 
            UNION ALL 
        SELECT * FROM obs_clin 
            UNION ALL 
        SELECT * FROM procedures 
            UNION ALL 
        SELECT * FROM vital
    )
    WHERE measurement_concept_id IS NOT NULL
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(base_10_hash_value as bigint) & 2251799813685247 as measurement_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patid
    , measurement_concept_id
    , measurement_date
    , measurement_datetime
    , measurement_time
    , measurement_type_concept_id
    , operator_concept_id
    , value_as_number
    , value_as_concept_id
    , unit_concept_id
    , range_low
    , range_high
    , provider_id
    , site_encounterid
    , visit_detail_id
    , measurement_source_value
    , measurement_source_concept_id
    , unit_source_value
    , value_source_value
    , domain_source
    , site_pkey
    , data_partner_id
    , payload
FROM (
    SELECT
          *
        , conv(sub_hash_value, 16, 10) as base_10_hash_value
    FROM (
        SELECT
              * 
            , substr(hashed_id, 1, 15) as sub_hash_value
        FROM (
            SELECT
                  *
                -- Create primary key by concatenating row, hashing to 128bit hexademical with md5,
                -- and converting to 51 bit int by first taking first 15 hexademical digits and converting
                --  to base 10 (60 bit) and then bit masking to extract the first 51 bits
                , md5(concat_ws(
                    ';'
                    , COALESCE(site_patid, '')
                    , COALESCE(measurement_concept_id, '')
                    , COALESCE(measurement_date, '')
                    , COALESCE(measurement_datetime, '')
                    , COALESCE(measurement_time, '')
                    , COALESCE(measurement_type_concept_id, '')
                    , COALESCE(operator_concept_id, '')
                    , COALESCE(value_as_number, '')
                    , COALESCE(value_as_concept_id, '')
                    , COALESCE(unit_concept_id, '')
                    , COALESCE(range_low, '')
                    , COALESCE(range_high, '')
                    , COALESCE(provider_id, '')
                    , COALESCE(site_encounterid, '')
                    , COALESCE(visit_detail_id, '')
                    , COALESCE(measurement_source_value, '')
                    , COALESCE(measurement_source_concept_id, '')
                    , COALESCE(unit_source_value, '')
                    , COALESCE(value_source_value, '')
                    , site_pkey
                )) as hashed_id
            FROM final_table
        )
    )
)