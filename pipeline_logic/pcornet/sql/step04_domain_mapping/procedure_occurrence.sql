CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/04 - domain mapping/procedure_occurrence` AS

with condition as (
    SELECT 
        patid AS site_patid,
		CAST(xw.target_concept_id as int) as procedure_concept_id,
		CAST(c.report_date as date) as procedure_date,
		CAST(null as timestamp) as procedure_datetime,
		-- case when c.condition_source = 'HC' then 38000245 --HC=Healthcare  problem  list/EHR problem list entry
		-- 			when c.condition_source ='PR' then 45905770 --PR : patient reported
		-- 			when c.condition_source = 'NI' then 46237210
		-- 			when c.condition_source = 'OT' then 45878142
		-- 			when c.condition_source = 'UN' then 0 --charles mapping doc ref. Unknown (concept_id = 45877986/charles mapping doc ref. Unknown (concept_id = 45877986)
		-- 			when c.condition_source in ('RG', 'DR', 'PC') then 0 ---PC    PC=PCORnet-defined  condition  algorithm       See mapping comments,
		-- 			ELSE 0
		-- 			END AS procedure_type_concept_id, -- use this type concept id for ehr order list
		32817 as procedure_type_concept_id,
		0 modifier_concept_id, -- need to create a cpt_concept_id table based on the source_code_concept id
		CAST(null as int) as quantity,
		CAST(null as int) as provider_id,
		encounterid as site_encounterid,
		CAST(null as int) as visit_detail_id,
		CAST(c.condition as string) as procedure_source_value,
		CAST(xw.source_concept_id as int) as procedure_source_concept_id,
		CAST(xw.src_code_type as string) as modifier_source_value,
		'CONDITION' AS domain_source,
		data_partner_id,
		payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/condition` c
		INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
			ON c.condition = xw.src_code 
			AND xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Procedure'
			AND xw.src_code_type = c.condition_type
),

diagnosis as (
  	SELECT     
        patid AS site_patid,
		CAST(xw.target_concept_id as int) as procedure_concept_id, 
		CAST(COALESCE(d.dx_date, d.admit_date) as date) as procedure_date, 
		CAST(null as timestamp) as procedure_datetime,
		-- 0 AS procedure_type_concept_id, -- use this type concept id for ehr order list
		-- 38000275 AS procedure_type_concept_id, --default values from draft mappings spreadsheet --added 6/26
		CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS procedure_type_concept_id,
		0 modifier_concept_id, -- need to create a cpt_concept_id table based on the source_code_concept id
		CAST(null as int) as quantity,
		CAST(null as int) as provider_id,
		encounterid as site_encounterid,
		CAST(null as int) as visit_detail_id,
		CAST(xw.src_code as string) as procedure_source_value,
		CAST(xw.source_concept_id as int) as procedure_source_concept_id,
		CAST(xw.src_code_type as string) as modifier_source_value,
		'DIAGNOSIS' AS domain_source,
		data_partner_id,
		payload
  FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/diagnosis` d 
	INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
		ON d.dx = xw.src_code 
		AND xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Procedure' 
		AND xw.src_code_type = d.dx_type
	LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
		ON d.dx_origin = xw2.SRC_CODE
		AND xw2.CDM_TBL = 'DIAGNOSIS' AND xw2.CDM_TBL_COLUMN_NAME = 'dx_origin'
),

procedures as (
    SELECT
        patid AS site_patid,
		CAST(xw.target_concept_id as int) AS procedure_concept_id,
		CAST(pr.px_date as date) AS procedure_date,
		CAST(null as timestamp) AS procedure_datetime,
		-- CASE
		-- 	WHEN pr.px_source = 'BI'  THEN
		-- 		257
		-- 	WHEN pr.px_source = 'CL'  THEN
		-- 		32468
		-- 	WHEN pr.px_source = 'OD'  THEN
		-- 		38000275 --ORDER /EHR 
		-- 	WHEN pr.px_source = 'UN'  THEN
		-- 		0 --UN This is not a type concept and it really has no value,  so set to 0 / do not use 45877986 for UN - 6/18/20 SSH
		-- 	WHEN pr.px_source = 'NI'  THEN
		-- 		46237210
		-- 	WHEN pr.px_source = 'OT'  THEN
		-- 		45878142
		-- 	ELSE
		-- 		0
		-- END AS procedure_type_concept_id, -- use this type concept id for ehr order list
		CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS procedure_type_concept_id,
		0 modifier_concept_id, -- need to create a cpt_concept_id table based on the source_code_concept id
		CAST(null as int) AS quantity,
		CAST(null as int) AS provider_id,
		encounterid as site_encounterid,
		CAST(null as int) AS visit_detail_id,
		CAST(xw.src_code as string) AS procedure_source_value,
		CAST(xw.source_concept_id as int) AS procedure_source_concept_id,
		CAST(xw.src_code_type as string) AS modifier_source_value,
		'PROCEDURES' AS domain_source,
		data_partner_id,
		payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/procedures` pr
            INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
				ON pr.px = xw.src_code
				AND xw.CDM_TBL = 'PROCEDURES' AND xw.target_domain_id = 'Procedure'
				AND xw.src_code_type = pr.px_type
            LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
			 	ON pr.px_source = xw2.SRC_CODE 
			 	AND xw2.CDM_TBL = 'PROCEDURES' AND xw2.CDM_TBL_COLUMN_NAME = 'PX_SOURCE'                                                                   
),

obs_gen as (
    SELECT
        patid AS site_patid,
		4230167 AS procedure_concept_id, ----------- artificial respiration concept id
		CAST(obg.obsgen_start_date as date) AS procedure_date,
		CAST(obg.OBSGEN_START_DATETIME as timestamp) AS procedure_datetime,
		--  38000275 AS procedure_type_concept_id, -- use this type concept id for ehr order list
		32817 as procedure_type_concept_id, 
		0 as modifier_concept_id, -- need to create a cpt_concept_id table based on the source_code_concept id
		CAST(null as int) AS quantity,
		CAST(null as int) AS provider_id,
		encounterid as site_encounterid,
		CAST(null as int) AS visit_detail_id,
		'obsgen_type:' || COALESCE(obg.obsgen_type, '') || '|obsgen_code:' || COALESCE(obg.obsgen_code, '') || '|obsgen_source: ' || COALESCE(obg.obsgen_source, '') 
            || '|obsgen_result_text:' || COALESCE(obsgen_result_text, '') AS procedure_source_value,
		38000275 AS procedure_source_concept_id,
		CAST(null as string) AS modifier_source_value,
		'OBS_GEN' AS domain_source,
		data_partner_id,
		payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/obs_gen` obg
	WHERE
		obg.obsgen_type = 'PC_COVID'
		AND obg.obsgen_code = 3000
		AND obg.obsgen_source = 'DR'
		AND obg.obsgen_result_text = 'Y'
),

all_domains as (
    SELECT * FROM (
        SELECT * FROM condition 
            UNION ALL  
        SELECT * FROM diagnosis 
            UNION ALL 
        SELECT * FROM procedures
            UNION ALL 
        SELECT * FROM obs_gen
    )
),

final_table AS (
    SELECT
          *
        -- Required for identical rows so that their IDs differ when hashing
        , ROW_NUMBER() OVER (
            PARTITION BY
              site_patid
            , procedure_concept_id
            , procedure_date
            , procedure_datetime
            , procedure_type_concept_id
            , modifier_concept_id
            , quantity
            , provider_id
            , site_encounterid
            , visit_detail_id
            , procedure_source_value
            , procedure_source_concept_id
            , modifier_source_value
            ORDER BY site_patid
        ) as row_index
    FROM all_domains
    WHERE procedure_concept_id IS NOT NULL
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id_51_bit
    , hashed_id
    , site_patid
    , procedure_concept_id
    , procedure_date
    , procedure_datetime
    , procedure_type_concept_id
    , modifier_concept_id
    , quantity
    , provider_id
    , site_encounterid
    , visit_detail_id
    , procedure_source_value
    , procedure_source_concept_id
    , modifier_source_value
	, domain_source
    , data_partner_id
	, payload
FROM (
    SELECT
          *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patid, '')
			, COALESCE(procedure_concept_id, '')
			, COALESCE(procedure_date, '')
			, COALESCE(procedure_datetime, '')
			, COALESCE(procedure_type_concept_id, '')
			, COALESCE(modifier_concept_id, '')
			, COALESCE(quantity, '')
			, COALESCE(provider_id, '')
			, COALESCE(site_encounterid, '')
			, COALESCE(visit_detail_id, '')
			, COALESCE(procedure_source_value, '')
			, COALESCE(procedure_source_concept_id, '')
			, COALESCE(modifier_source_value, '')
			, COALESCE(row_index, '')
        )) as hashed_id
    FROM final_table
)
