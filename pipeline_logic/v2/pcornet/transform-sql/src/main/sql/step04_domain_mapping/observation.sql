CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/observation`
TBLPROPERTIES (foundry_transform_profile = 'high-memory') AS

with condition as (
    SELECT DISTINCT
        patid AS site_patid, 
        CAST(xw.target_concept_id as int) as observation_concept_id,
        CAST(c.report_date as date) as observation_date,
        CAST(null as timestamp) as observation_datetime,
        -- HC/NI/OT/PC/PR/RG/UN/DR
        -- case when c.condition_source = 'HC' then 38000245
        --     when c.condition_source ='PR' then 45905770
        --     when c.condition_source = 'NI' then 46237210
        --     when c.condition_source = 'OT' then 45878142
        --     when c.condition_source = 'UN' then 45877986  --Unknown (concept_id = 45877986)
        --     when c.condition_source in ('RG', 'DR', 'PC') then 0 ---PC    PC=PCORnet-defined  condition  algorithm       See mapping comments
        --     else 0
        --     end as observation_type_concept_id,
        32817 as observation_type_concept_id,
        CAST(null as float) as value_as_number,
        CAST(null as string) as value_as_string,
        CAST(xw.source_concept_id as int) as value_as_concept_id,
        CAST(null as int) as qualifier_concept_id,
        CAST(null as int) as unit_concept_id,
        CAST(null as long) as provider_id,
        encounterid as site_encounterid,
        CAST(null as long) as visit_detail_id,
        CAST(c.condition as string) AS observation_source_value,
        CAST(xw.source_concept_id as int) as observation_source_concept_id,
        CAST(null as string) as unit_source_value,
        CAST(null as string) as qualifier_source_value,
        'CONDITION' as domain_source,
        conditionid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/condition` c
        JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Observation'  
            AND c.condition = xw.src_code 
            AND xw.src_code_type = c.condition_type
),

demographic as (
-- PAT_PREF_LANGUAGE_SPOKEN
    SELECT DISTINCT
        demo.patid AS site_patid,
        4152283 as observation_concept_id,
        CAST(ee.obs_date as date) as observation_date,
        CAST(null as timestamp) as observation_datetime,
        0 as observation_type_concept_id,
        0 value_as_number,
        CAST(demo.pat_pref_language_spoken as string) as value_as_string,
        CAST(lang.TARGET_CONCEPT_ID as int) as value_as_concept_id,
        0 as qualifier_concept_id,
        0 as unit_concept_id,
        CAST(null as long) as provider_id,
        null as site_encounterid,
        CAST(null as long) as visit_detail_id,
        'src=PCORNET.DEMOGRAPHIC dt=earliest ENC for pat' AS observation_source_value,
        0 observation_source_concept_id,
        CAST(null as string) as unit_source_value,
        CAST(null as string) as qualifier_source_value,
        'DEMOGRAPHIC' domain_source,
        demo.patid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/demographic` demo
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` lang 
            ON lang.CDM_TBL_COLUMN_NAME='PAT_PREF_LANGUAGE_SPOKEN' AND lang.CDM_SOURCE='PCORnet' 
            AND lang.CDM_TBL='DEMOGRAPHIC' AND lang.SRC_CODE = demo.pat_pref_language_spoken 
        LEFT JOIN (
            SELECT patid, MIN(admit_date) AS obs_date 
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` 
            GROUP BY patid
            ) ee 
            ON ee.patid = demo.patid  

UNION ALL

-- GENDER_IDENTITY
    SELECT DISTINCT
        demo.patid AS site_patid
        , 4110772 as observation_concept_id
        , CAST(ee.obs_date as date) as observation_date
        , CAST(null as timestamp) as observation_datetime
        , 0 as observation_type_concept_id
        , 0 as value_as_number
        , CAST(demo.gender_identity as string) as value_as_string
        , CAST(gx.TARGET_CONCEPT_ID as int) as value_as_concept_id
        , 0 as qualifier_concept_id
        , CAST(null as int) as unit_concept_id
        , CAST(null as long) as provider_id
        , null as site_encounterid
        , CAST(null as long) as visit_detail_id
        , 'src=PCORNET.DEMOGRAPHIC dt=earliest ENC for pat'  AS observation_source_value
        , 0 as observation_source_concept_id
        , CAST(null as string) as unit_source_value
        , CAST(null as string) as qualifier_source_value
        , 'DEMOGRAPHIC' as domain_source
        , demo.patid as site_pkey
        , data_partner_id
        , payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/demographic` demo     
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` gx 
            ON gx.CDM_TBL_COLUMN_NAME='GENDER_IDENTITY' AND gx.CDM_SOURCE='PCORnet' AND gx.CDM_TBL='DEMOGRAPHIC' 
            AND gx.SRC_CODE = demo.gender_identity
        LEFT JOIN (
            SELECT patid, MIN(admit_date) AS obs_date 
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` 
            GROUP BY patid
            ) ee 
            ON ee.patid = demo.patid  

UNION ALL

-- SEXUAL_ORIENTATION
    SELECT DISTINCT
        demo.patid AS site_patid
        , 4283657 as observation_concept_id
        , CAST(ee.obs_date as date) as observation_date
        , CAST(null as timestamp) as observation_datetime
        , 0 as observation_type_concept_id
        , 0 as value_as_number
        , CAST(demo.sexual_orientation as string) as value_as_string
        , CAST(sx.TARGET_CONCEPT_ID as int) as value_as_concept_id
        , 0 as qualifier_concept_id
        , CAST(null as int) as unit_concept_id
        , CAST(null as long) as provider_id
        , null as site_encounterid
        , CAST(null as long) as visit_detail_id
        , 'src=PCORNET.DEMOGRAPHIC dt=earliest ENC for pat'  AS observation_source_value
        , 0 as observation_source_concept_id
        , CAST(null as string) as unit_source_value
        , CAST(null as string) as qualifier_source_value
        , 'DEMOGRAPHIC' as domain_source 
        , demo.patid as site_pkey
        , data_partner_id
        , payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/demographic` demo   
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` sx 
            ON sx.CDM_TBL_COLUMN_NAME='SEXUAL_ORIENTATION' AND sx.CDM_SOURCE='PCORnet' AND sx.CDM_TBL='DEMOGRAPHIC'  
            AND sx.SRC_CODE = demo.sexual_orientation 
        LEFT JOIN (
            SELECT patid, MIN(admit_date) AS obs_date 
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` 
            GROUP BY patid
            ) ee 
            ON ee.patid = demo.patid  
), 

diagnosis as (
    SELECT
        patid AS site_patid,
        CAST(xw.target_concept_id as int) as observation_concept_id,
        CAST(COALESCE(d.dx_date, d.admit_date) as date) as observation_date,
        CAST(null as timestamp) as observation_datetime, -- same as observation_date
        -- 0  as observation_type_concept_id, --d.DX_SOURCE: need to find concept_id or from P2O_Term_xwalk
        -- case when d.PDX = 'P' then 4307107
        -- when d.PDX = 'S' then 4309641
        -- else 38000280 end as observation_type_concept_id, --default values from draft mappings spreadsheet --added 6/26
        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) as observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) as value_as_string,
        CAST(xw.target_concept_id as int) as value_as_concept_id,
        CAST(null as int) as qualifier_concept_id,
        CAST(null as int) as unit_concept_id,
        CAST(null as long) as provider_id,
        encounterid as site_encounterid,
        CAST(null as long) as visit_detail_id,
        CAST(d.dx as string) as observation_source_value,
        CAST(xw.source_concept_id as int) as observation_source_concept_id,
        CAST(null as string) as unit_source_value,
        CAST(null as string) as qualifier_source_value,
        'DIAGNOSIS' as domain_source,
        diagnosisid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/diagnosis` d
        JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Observation'
            AND d.dx = xw.src_code  
            AND xw.src_code_type = d.dx_type
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
            ON d.dx_origin = xw2.SRC_CODE
            AND xw2.CDM_TBL = 'DIAGNOSIS'
            AND xw2.CDM_TBL_COLUMN_NAME = 'dx_origin'
), 

lab_result_cm as (
    SELECT
        patid AS site_patid
        , CAST(xw.target_concept_id as int) as observation_concept_id
        , CAST(lab.result_date as date) as observation_date
        , CAST(lab.RESULT_DATETIME as timestamp) as observation_datetime
        , CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) as observation_type_concept_id 
        , CAST(lab.result_num as float) as value_as_number
        , CAST(lab.lab_loinc as string) as value_as_string
        , 4188539 as value_as_concept_id --ssh observation of the measurement -- recorded as yes observation 6/26/2020
        , case 
            when lower(trim(result_qual)) = 'positive' then 45884084
            when lower(trim(result_qual)) = 'negative' then 45878583
            when lower(trim(result_qual)) = 'pos' then 45884084
            when lower(trim(result_qual)) = 'neg' then 45878583
            when lower(trim(result_qual)) = 'presumptive positive' then 45884084
            when lower(trim(result_qual)) = 'presumptive negative' then 45878583
            when lower(trim(result_qual)) = 'detected' then 45884084
            when lower(trim(result_qual)) = 'not detected' then 45878583
            when lower(trim(result_qual)) = 'inconclusive' then 45877990
            when lower(trim(result_qual)) = 'normal' then 45884153
            when lower(trim(result_qual)) = 'abnormal' then 45878745
            when lower(trim(result_qual)) = 'low' then 45881666
            when lower(trim(result_qual)) = 'high' then 45876384
            when lower(trim(result_qual)) = 'borderline' then 45880922
            when lower(trim(result_qual)) = 'elevated' then 4328749  --ssh add issue number 55 - 6/26/2020
            when lower(trim(result_qual)) = 'undetermined' then 45880649
            when lower(trim(result_qual)) = 'undetectable' then 45878583
            when lower(trim(result_qual)) = 'un' then 0
            when lower(trim(result_qual)) = 'unknown' then 0
            when lower(trim(result_qual)) = 'no information' then 46237210
            else 45877393
         end as qualifier_concept_id --null/un/elevated/boderline/ot/low/high/normal/negative/positive/abnormal
        , CAST(u.TARGET_CONCEPT_ID as int) as unit_concept_id
        , CAST(null as long) as provider_id
        , encounterid as site_encounterid
        , CAST(null as long) as visit_detail_id
        , CAST(lab.lab_loinc as string) as observation_source_value
        , CAST(xw.source_concept_id as int) as observation_source_concept_id
        , CAST(lab.result_unit as string) as unit_source_value
        , CAST(lab.result_modifier as string) as qualifier_source_value
        , 'LAB_RESULT_CM' as domain_source
        , lab_result_cm_id as site_pkey
        , data_partner_id
        , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/lab_result_cm` lab
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON lab.lab_loinc = xw.src_code 
            AND xw.src_code_type = 'LOINC'
            AND xw.CDM_TBL = 'LAB_RESULT_CM' 
            AND xw.target_domain_id = 'Observation' 
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` u 
            ON lab.result_unit = u.SRC_CODE 
            AND u.CDM_TBL_COLUMN_NAME = 'RX_DOSE_ORDERED_UNIT'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
            ON lab.lab_result_source = xw2.SRC_CODE
            AND xw2.CDM_TBL = 'LAB_RESULT_CM'
            AND xw2.CDM_TBL_COLUMN_NAME = 'LAB_RESULT_SOURCE'
),

obs_clin as (
    SELECT
        patid AS site_patid,
        CAST(xw.target_concept_id as int) AS observation_concept_id,
        CAST(obs.obsclin_start_date as date) AS observation_date,
        CAST(obs.OBSCLIN_START_DATETIME as timestamp) AS observation_datetime, --set same with obsclin_date 
        -- CASE
        --     WHEN obs.obsclin_type = 'LC' THEN
        --         37079395
        --     WHEN obs.obsclin_type = 'SM' THEN
        --         37079429
        --     ELSE
        --         0
        -- END AS observation_type_concept_id,
        32817 AS observation_type_concept_id,
        CAST(obs.obsclin_result_num as float) AS value_as_number,
        CAST(obs.obsclin_result_text as string) AS value_as_string,
        CAST(obs.obsclin_result_snomed as int) AS value_as_concept_id,
        0 AS qualifier_concept_id, -- all 0 in data_store schema
        0 AS unit_concept_id, -- all 0 in data_store schema
        CAST(null as long) AS provider_id, --for now, still TBD, Governance group question, Davera will follow up
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        CAST(obs.obsclin_code as string) AS observation_source_value,
        CAST(xw.target_concept_id as int) AS observation_source_concept_id,
        CAST(obs.obsclin_result_unit as string) AS unit_source_value, -- lookup unit_concept_code
        CAST(obs.obsclin_result_qual as string) AS qualifier_source_value,
        'OBS_CLIN' AS domain_source,
        obsclinid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_clin` obs
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON obs.obsclin_code = xw.src_code
            AND obs.mapped_obsclin_type = xw.src_vocab_code
            AND xw.CDM_TBL = 'OBS_CLIN' AND xw.target_domain_id = 'Observation' 
),

---obs_gen, long covid clinic visit data to observation
obs_gen as ( 
    SELECT patid as site_patid, 
    2004207791 as observation_concept_id, ---2004207791=N3C observation concept id for long COVID clinic visit 
    CAST(obsg.obsgen_start_date as date) AS observation_date,
    CAST(obsg.OBSGEN_START_DATETIME as timestamp) AS observation_datetime, --set same with obsclin_date 
    32817 AS observation_type_concept_id,
    CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS value_as_concept_id,  --- this would be the concept id of the categorical data on obsgen_result_text
        CAST(null as int) AS qualifier_concept_id, -- all 0 in data_store schema
        CAST(null as int) AS unit_concept_id, -- all 0 in data_store schema
        CAST(null as long) AS provider_id, --for now, still TBD, Governance group question, Davera will follow up
    encounterid as site_encounterid, 
    CAST(null as long) AS visit_detail_id,
    CAST(obsg.obsgen_code as string) AS observation_source_value,
    CAST(2004207791 as int) AS observation_source_concept_id,
    CAST(null as string) AS unit_source_value, -- lookup unit_concept_code
    CAST(null as string) AS qualifier_source_value,
        'OBS_GEN' AS domain_source,
        obsgenid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` obsg
    WHERE obsg.obsgen_type = 'UD_CLINICTYPE' and obsg.obsgen_code = 'PASC'
),
--- SDoH data
obsgen_sdoh as (
    ---grab SDoH data from obs_gen and map to observation
    ---obsgen_type is LC and the obsgen_result_text contains LOINC answer codes
    -- obsgen_result_text, the answer code text would need to be mapped if not found in the mapping table. 
    SELECT 
    patid as site_patid, 
    xw.target_concept_id as observation_concept_id, ---2004207791=N3C observation concept id for long COVID clinic visit 
    CAST(obs.obsgen_start_date as date) AS observation_date,
    CAST(obs.OBSGEN_START_DATETIME as timestamp) AS observation_datetime, --set same with obsclin_date 
    32817 AS observation_type_concept_id,
    CAST(null as float) AS value_as_number,
    CAST(obs.obsgen_result_text as string) AS value_as_string,
   --- COALESCE(xw_map1.TARGET_CONCEPT_ID, xw_map2.TARGET_CONCEPT_ID) AS value_as_concept_id,  --- use xw2 or xw3, this would be the concept id of the categorical data on obsgen_result_text
    CAST( answercode.target_concept_id as int) as value_as_concept_id, ---TODO: shong, generate an resultxw table for the answer codes
    CAST(null as int) AS qualifier_concept_id, -- all 0 in data_store schema
    CAST(null as int) AS unit_concept_id, -- all 0 in data_store schema
    CAST(null as long) AS provider_id, --for now, still TBD, Governance group question, Davera will follow up
    encounterid as site_encounterid, 
    CAST(null as long) AS visit_detail_id,
    CAST(obs.obsgen_source || obs.obsgen_code as string) AS observation_source_value,
    CAST( obsgensrc.TARGET_CONCEPT_ID as int) AS observation_source_concept_id,
    CAST(null as string) AS unit_source_value, -- lookup unit_concept_code
    CAST(null as string) AS qualifier_source_value,
    'OBS_GEN' AS domain_source,
    obsgenid as site_pkey,
    data_partner_id,
    payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` obs
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON obs.obsgen_code = xw.src_code
            --AND obs.mapped_obsgen_type = xw.src_vocab_code --- mapped_obsclin_type is null, shong 4/20/2022
            -- it should be set to LOINC if the obsgen_type is set to LC
            AND 'LOINC' = xw.src_vocab_code
            AND xw.CDM_TBL = 'OBS_GEN' AND xw.target_domain_id = 'Observation' 
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` obsgensrc
            ON obs.obsgen_source = obsgensrc.SRC_CODE
            AND obsgensrc.CDM_TBL = 'OBS_GEN'
            AND obsgensrc.CDM_TBL_COLUMN_NAME = 'OBSGEN_SOURCE'
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/answer_code_xwalk`  answercode
            on trim(answercode.answer_code) = trim(obs.obsgen_result_text) and obs.obsgen_type = 'LC' and obs.obsgen_result_modifier= 'TX'
    WHERE obs.obsgen_result_num is null ----LC type and TX modifier with categrical answers
    union all 
    -- SDoH data with numerical results - SM type with numerical result values 
    ---grab SDoH data from obs_gen and map to observation
    ---obsgen_type is SM and the obsgen_result_num contains result with numerical value
    SELECT 
    patid as site_patid, 
    xw.target_concept_id as observation_concept_id, ---2004207791=N3C observation concept id for long COVID clinic visit 
    CAST(obs.obsgen_start_date as date) AS observation_date,
    CAST(obs.OBSGEN_START_DATETIME as timestamp) AS observation_datetime, --set same with obsclin_date 
    32817 AS observation_type_concept_id,
    CAST(obs.obsgen_result_num as float) AS value_as_number,
    CAST(obs.obsgen_result_text as string) AS value_as_string,
   --- COALESCE(xw_map1.TARGET_CONCEPT_ID, xw_map2.TARGET_CONCEPT_ID) AS value_as_concept_id,  --- use xw2 or xw3, this would be the concept id of the categorical data on obsgen_result_text
    CAST(categorical.TARGET_CONCEPT_ID as int) as value_as_concept_id, ---this is numeric answer, so no value_as_concept_id
    CAST(modifier.TARGET_CONCEPT_ID AS int) AS qualifier_concept_id, -- all 0 in data_store schema
    CAST(null as int) AS unit_concept_id, -- all 0 in data_store schema
    CAST(null as long) AS provider_id, --for now, still TBD, Governance group question, Davera will follow up
    encounterid as site_encounterid, 
    CAST(null as long) AS visit_detail_id,
    CAST(obs.obsgen_type || obs.obsgen_code || obsgen_result_num || obsgen_result_qual as string) AS observation_source_value,
    CAST( obsgensrc.TARGET_CONCEPT_ID as int) AS observation_source_concept_id,
    CAST(null as string) AS unit_source_value, -- lookup unit_concept_code
    CAST(null as string) AS qualifier_source_value,
    'OBS_GEN' AS domain_source,
    obsgenid as site_pkey,
    data_partner_id,
    payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` obs
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON obs.obsgen_code = xw.src_code
            --AND obs.mapped_obsgen_type = xw.src_vocab_code --- mapped_obsclin_type is null, shong 4/20/2022
            -- it should be set to LOINC if the obsgen_type is set to LC
            AND 'SNOMED' = xw.src_vocab_code
            AND xw.CDM_TBL = 'OBS_GEN' AND xw.target_domain_id = 'Observation' 
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` obsgensrc
            ON trim(obs.obsgen_source) = trim(obsgensrc.SRC_CODE)
            AND obsgensrc.CDM_TBL = 'OBS_GEN'
            AND obsgensrc.CDM_TBL_COLUMN_NAME = 'OBSGEN_SOURCE'
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` modifier
            on upper(trim(modifier.SRC_CODE)) = upper(trim(obs.obsgen_result_modifier)) 
            AND modifier.CDM_TBL = 'OBS_GEN' 
            AND modifier.CDM_TBL_COLUMN_NAME = 'OBSGEN_RESULT_MODIFIER'
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` categorical
            on upper(trim(categorical.SRC_CODE)) = upper(trim(obs.obsgen_result_text)) 
            AND categorical.CDM_TBL = 'OBS_GEN' 
            AND categorical.CDM_TBL_COLUMN_NAME = 'OBSGEN_RESULT_TEXT'   
    WHERE obs.obsgen_type = 'SM' ----SM type can have both categorical answers or the obsgen_result_num result.
    -- shong, examine the data and see how to resovle both categorical and numerical answers, shong 6/9/22 (site 198 is sending both ways) 
), 

procedures as (
    SELECT
        patid AS site_patid,
        CAST(xw.target_concept_id as int) AS observation_concept_id,
        CAST(pr.px_date as date) AS observation_date,
        CAST(null as timestamp) AS observation_datetime,
        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        CAST(pr.px as string) AS observation_source_value,
        CAST(xw.source_concept_id as int) AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'PROCEDURES' AS domain_source,
        proceduresid as site_pkey,
        data_partner_id,
        payload
    FROM
        `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/procedures` pr
        JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON pr.px = xw.src_code
            AND xw.CDM_TBL = 'PROCEDURES'
            AND xw.target_domain_id = 'Observation'
            AND xw.src_code_type = pr.px_type
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
            ON pr.px_source = xw2.SRC_CODE
            AND xw2.CDM_TBL = 'PROCEDURES'
            AND xw2.CDM_TBL_COLUMN_NAME = 'PX_SOURCE'
),

vital as (
-- Smoking
    SELECT
        patid AS site_patid,
        -- mp.target_concept_id     AS observation_concept_id, --concept id for Smoking, from notes
        --** MB: replace above^ with the target_concept_id from the valueset mapping table, which is equivalent based on the join
        CAST(s.TARGET_CONCEPT_ID as int) AS observation_concept_id, 
        CAST(v.measure_date as date) AS observation_date,
        CAST(v.MEASURE_DATETIME as timestamp) AS observation_datetime,
        CAST(vt.TARGET_CONCEPT_ID as int) AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(s.TARGET_CONCEPT_NAME as string) AS value_as_string,
        CAST(s.TARGET_CONCEPT_ID as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        'VITAL.SMOKING = ' || v.smoking AS observation_source_value,
        CAST(null as int) AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'VITAL' AS domain_source,
        vitalid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/vital` v
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` s 
            ON s.CDM_TBL = 'VITAL'
            AND s.CDM_TBL_COLUMN_NAME = 'SMOKING'
            AND s.SRC_CODE = v.smoking
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` vt 
            ON vt.CDM_TBL = 'VITAL'
            AND vt.CDM_TBL_COLUMN_NAME = 'VITAL_SOURCE'
            AND vt.SRC_CODE = v.vital_source
    WHERE v.smoking NOT IN ('NI', 'UN')

UNION ALL

-- Tobacco
    SELECT
        patid AS site_patid,
        -- mp.target_concept_id     AS observation_concept_id, --concept id for TOBACCO, from notes
        --** MB: replace above^ with the target_concept_id from the valueset mapping table, which is equivalent based on the join
        CAST(s.TARGET_CONCEPT_ID as int) AS observation_concept_id,
        CAST(v.measure_date as date) AS observation_date,
        CAST(null as timestamp) AS observation_datetime, --** MB: no time info
        CAST(vt.TARGET_CONCEPT_ID as int) AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(s.TARGET_CONCEPT_NAME as string) AS value_as_string,
        CAST(s.TARGET_CONCEPT_ID as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        'VITAL.TOBACCO = ' || v.tobacco AS observation_source_value,
        -- NULL as observation_source_value,
        CAST(null as int) AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'VITAL' AS domain_source,
        vitalid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/vital` v
        JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` s 
            ON s.CDM_TBL = 'VITAL'
            AND s.CDM_TBL_COLUMN_NAME = 'TOBACCO'
            AND s.SRC_CODE = v.tobacco
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` vt 
            ON vt.CDM_TBL = 'VITAL'
            AND vt.CDM_TBL_COLUMN_NAME = 'VITAL_SOURCE'
            AND vt.SRC_CODE = v.vital_source
    WHERE v.tobacco NOT IN ('NI', 'UN')
),

encounter as (
    SELECT
        patid AS site_patid,
        4021968 AS observation_concept_id,
        CAST(COALESCE(d.discharge_date, d.admit_date) as date) AS observation_date,
        CAST(null as timestamp) AS observation_datetime, --** MB: no time info
        32823 AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        'Discharge Status-AM' AS observation_source_value,
        44814692 AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'ENCOUNTER' domain_source,
        encounterid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` d
    WHERE d.discharge_status = 'AM'

UNION ALL

    SELECT
        patid AS site_patid,
        4137274 AS observation_concept_id,
        CAST(COALESCE(d.discharge_date, d.admit_date) as date) AS observation_date,
        CAST(null as timestamp) AS observation_datetime, --** MB: no time info
        32823 AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        CASE
            WHEN d.discharge_status = 'AW' THEN
                'Discharge Status-AW'
            WHEN d.discharge_status = 'HO' THEN
                'Discharge Status-HO'
            WHEN d.discharge_status = 'IP' THEN
                'Discharge Status-IP'
        END AS observation_source_value,
        CASE
            WHEN d.discharge_status = 'AW' THEN
                306685000
            WHEN d.discharge_status = 'HO' THEN
                44814696
            WHEN d.discharge_status = 'IP' THEN
                44814698
        END AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'ENCOUNTER' as domain_source,
        encounterid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` d
    WHERE d.discharge_status IN (
        'AW',
        'HO',
        'IP'
    )

UNION ALL 

    SELECT
        patid AS site_patid,
        4216643 AS observation_concept_id,
        CAST(COALESCE(d.discharge_date, d.admit_date) as date) AS observation_date,
        CAST(null as timestamp) AS observation_datetime, --** MB: no time info
        44818516 AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        'Discharge Status-EX' AS observation_source_value,
        4216643 AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'ENCOUNTER' as domain_source,
        encounterid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` d
    WHERE d.discharge_status = 'EX'
),

-- Grab records from dispensing, immunization, med_admin, prescribing, and death_cause
-- These will be records that probably don't belong in the Observation table but as part of exception handling have been mapped to a concept_id = 0
all_other_tables AS (
    SELECT
        other.patid AS site_patid,
        other.concept_id AS observation_concept_id,
        CAST(other.record_date as date) AS observation_date,
        CAST(null as timestamp) AS observation_datetime,
        CAST(null as int) AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        other.encounterid AS site_encounterid,
        CAST(null as long) AS visit_detail_id,
        other.source_value AS observation_source_value,
        other.source_concept_id AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        other.domain_source AS domain_source,
        other.pkey AS site_pkey,
        data_partner_id,
        payload
    FROM (
        SELECT patid, data_partner_id, payload
            , 'DEATH_CAUSE'         AS domain_source
            , xw.target_concept_id  AS concept_id
            , CAST(null as date)    AS record_date
            , CAST(null as string)  AS encounterid
            , 'death_cause_code:' || dc.death_cause_code || '|death_cause:' || dc.death_cause AS source_value
            , xw.source_concept_id  AS source_concept_id
            , dc.patid || '|' || dc.death_cause  AS pkey
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/death_cause` dc
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'DEATH_CAUSE' 
            AND xw.target_domain_id = 'Observation'
            AND dc.death_cause_code = xw.src_code_type 
            AND dc.death_cause = xw.src_code
        
        UNION
        
        SELECT patid, data_partner_id, payload
            , 'DISPENSING'          AS domain_source
            , xw.target_concept_id  AS concept_id
            , d.dispense_date       AS record_date
            , CAST(null as string)  AS encounterid
            , 'NDC:' || d.ndc       AS source_value
            , xw.source_concept_id  AS source_concept_id
            , dispensingid          AS pkey
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/dispensing` d
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'DISPENSING' 
            AND xw.target_domain_id = 'Observation'
            AND xw.src_code_type = 'NDC'
            AND xw.src_code = d.ndc

        UNION

        SELECT patid, data_partner_id, payload
            , 'IMMUNIZATION'            AS domain_source
            , xw.target_concept_id      AS concept_id
            , i.vx_record_date          AS record_date
            , i.encounterid             AS encounterid
            , 'vx_code_type:' || vx_code_type || '|vx_code:' || vx_code AS source_value
            , xw.source_concept_id      AS source_concept_id
            , immunizationid            AS pkey
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/immunization` i
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'IMMUNIZATION' 
            AND xw.target_domain_id = 'Observation'
            AND xw.src_code_type = i.vx_code_type
            AND xw.src_code = i.vx_code

        UNION

        SELECT patid, data_partner_id, payload
            , 'MED_ADMIN'               AS domain_source
            , xw.target_concept_id      AS concept_id
            , medadmin_start_date       AS record_date
            , encounterid               AS encounterid
            , 'medadmin_type:' || medadmin_type || '|medadmin_code:' || medadmin_code AS source_value
            , xw.source_concept_id      AS source_concept_id
            , medadminid                AS pkey
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/med_admin` m
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'MED_ADMIN' 
            AND xw.target_domain_id = 'Observation'
            AND xw.src_code_type = m.medadmin_type
            AND xw.src_code = m.medadmin_code

        UNION

        SELECT 
            patid, data_partner_id, payload
            , 'PRESCRIBING'                 AS domain_source
            , xw.target_concept_id          AS concept_id
            , rx_order_date                 AS record_date
            , encounterid                   AS encounterid
            , 'rxnorm_cui:' || rxnorm_cui   AS source_value
            , xw.source_concept_id          AS source_concept_id
            , prescribingid                 AS pkey
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/prescribing` p
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON xw.CDM_TBL = 'PRESCRIBING' 
            AND xw.target_domain_id = 'Observation'
            AND xw.src_code_type = 'rxnorm_cui'
            AND xw.src_code = p.rxnorm_cui
    ) other
),

final_table AS (
    -- Finding only unique measurements, per discussion with Matvey Palchuk + Kristin Kosta 13-08-2020
    SELECT DISTINCT
          site_patid
        , observation_concept_id
        , observation_date
        , observation_datetime
        , observation_type_concept_id
        , value_as_number
        , value_as_string
        , value_as_concept_id
        , qualifier_concept_id
        , unit_concept_id
        , provider_id
        , site_encounterid
        , visit_detail_id
        , observation_source_value
        , observation_source_concept_id
        , unit_source_value
        , qualifier_source_value
        , domain_source
        , site_pkey
        , data_partner_id
        , payload
    FROM (
        SELECT * FROM condition 
            UNION ALL
        SELECT * FROM demographic 
            UNION ALL
        SELECT * FROM diagnosis 
            UNION ALL 
        SELECT * FROM lab_result_cm 
            UNION ALL 
        SELECT * FROM obs_clin 
            UNION ALL
        SELECT * FROM obs_gen    
            UNION ALL
        SELECT * FROM obsgen_sdoh    
            UNION ALL 
        SELECT * FROM procedures 
            UNION ALL 
        SELECT * FROM vital
            UNION ALL 
        SELECT * FROM encounter 
            UNION ALL
        SELECT * FROM all_other_tables
    )
    WHERE observation_concept_id IS NOT NULL
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    -- In case of collisions, this will be joined on a lookup table in the next step
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patid
    , observation_concept_id
    , observation_date
    , observation_datetime
    , observation_type_concept_id
    , value_as_number
    , value_as_string
    , value_as_concept_id
    , qualifier_concept_id
    , unit_concept_id
    , provider_id
    , site_encounterid
    , visit_detail_id
    , observation_source_value
    , observation_source_concept_id
    , unit_source_value
    , qualifier_source_value
    , domain_source
    , site_pkey
    , data_partner_id
    , payload
FROM (
    SELECT
          *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patid, '')
			, COALESCE(observation_concept_id, '')
			, COALESCE(observation_date, '')
			, COALESCE(observation_datetime, '')
			, COALESCE(observation_type_concept_id, '')
			, COALESCE(value_as_number, '')
			, COALESCE(value_as_string, '')
			, COALESCE(value_as_concept_id, '')
			, COALESCE(qualifier_concept_id, '')
			, COALESCE(unit_concept_id, '')
			, COALESCE(provider_id, '')
			, COALESCE(site_encounterid, '')
			, COALESCE(visit_detail_id, '')
			, COALESCE(observation_source_value, '')
			, COALESCE(observation_source_concept_id, '')
			, COALESCE(unit_source_value, '')
			, COALESCE(qualifier_source_value, '')
            , site_pkey
        )) as hashed_id
    FROM final_table
)