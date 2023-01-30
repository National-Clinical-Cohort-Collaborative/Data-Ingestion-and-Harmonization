CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/device_exposure` AS

with obs_clin as (
    SELECT
        patid AS site_patid,
        CAST(xw.target_concept_id as int) AS device_concept_id,
        CAST(obs.obsclin_start_date as date) AS device_exposure_start_date,
        CAST(obs.OBSCLIN_START_DATETIME as timestamp) AS device_exposure_start_datetime,
        CAST(obs.obsclin_stop_date as date) AS device_exposure_end_date,
        CAST(obs.OBSCLIN_STOP_DATETIME as timestamp) AS device_exposure_end_datetime, -- SH, Using the end date, now that is available from the obsclin
        -- 44818707 AS device_type_concept_id, -- default values from draft mappings spreadsheet
        32817 AS device_type_concept_id,
        CAST(null as string) AS unique_device_id, --??
        CAST(null as int) AS quantity, --??
        CAST(null as long) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        CAST(obs.obsclin_code as string) AS device_source_value, --??
        CAST(null as int) AS device_source_concept_id, --??
        'OBS_CLIN' AS domain_source,
        obsclinid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_clin` obs
        JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON obs.obsclin_code = xw.src_code
            AND obs.mapped_obsclin_type = xw.src_vocab_code
            AND xw.CDM_TBL = 'OBS_CLIN' AND xw.target_domain_id = 'Device' 
), 

procedures as (
    SELECT
        patid AS site_patid,
        CAST(xw.target_concept_id as int) AS device_concept_id,
        CAST(pr.px_date as date) AS device_exposure_start_date,
        CAST(null as timestamp) AS device_exposure_start_datetime,
        CAST(null as date) AS device_exposure_end_date,
        CAST(null as timestamp) AS device_exposure_end_datetime,
        -- 44818707 AS device_type_concept_id,
        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS device_type_concept_id,
        CAST(null as string) AS unique_device_id,
        CAST(null as int) AS quantity,
        CAST(null as long) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as long) AS visit_detail_id,
        CAST(pr.px as string) AS device_source_value,
        CAST(xw.source_concept_id as int) AS device_source_concept_id,
        'PROCEDURES' AS domain_source,
        proceduresid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/procedures` pr
        JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
            ON pr.px = xw.src_code
            AND xw.CDM_TBL = 'PROCEDURES' AND xw.target_domain_id = 'Device' 
            -- AND xw.target_concept_id = mp.target_concept_id 
            AND xw.src_code_type = pr.px_type
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
            ON pr.px_source = xw2.SRC_CODE
            AND xw2.CDM_TBL = 'PROCEDURES'
            AND xw2.CDM_TBL_COLUMN_NAME = 'PX_SOURCE'                                                    
),

med_admin as (
    SELECT 
        patid AS site_patid,
        CAST(xw.target_concept_id as int) as device_concept_id,
        CAST(m.medadmin_start_date as date) as device_exposure_start_date,
        CAST(m.MEDADMIN_START_DATETIME as timestamp) as device_exposure_start_datetime,
        --** MB: populating end date/datetime:
        CAST(m.medadmin_stop_date as date) as drug_exposure_end_date,
        CAST(m.MEDADMIN_STOP_DATETIME as timestamp) as drug_exposure_end_datetime, 
        -- 44818707 as device_type_concept_id, -- default values from draft mappings spreadsheet
        32817 as device_type_concept_id,
        CAST(null as string) AS unique_device_id, 
        CAST(null as int) as quantity, 
        CAST(null as long) as provider_id,
        encounterid as site_encounterid,
        CAST(null as long) as visit_detail_id,
        CAST(m.medadmin_code as string) as device_source_value, 
        CAST(xw.source_concept_id as int) as device_source_concept_id, 
        'MED_ADMIN' as domain_source,
        medadminid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/med_admin` m
    JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` xw 
        ON m.medadmin_code = xw.src_code 
        AND xw.CDM_TBL = 'MED_ADMIN' AND xw.target_domain_id = 'Device' 
        AND xw.src_code_type = m.medadmin_type
),

-- vent data from obs_gen to device 
-- If the O2 device maps to a SNOMED code, OBSGEN_TYPE should equal ‘SM’ and OBGEN_CODE should include the SNOMED code.
-- If using either of the custom codes, OBSGEN_TYPE should equal ‘UD_N3C_O2_DEVICE’ and 
-- OBSGEN_CODE should include either ‘OT_O2_DEVICE’, or ‘ROOM_AIR’.
obs_gen as 
(
    SELECT 
        patid AS site_patid,
        -- ROOM_AIR : 2004208005, OT_O2_DEVICE=2004208004
        -- N3C custom codes have been added for the ROOM_AIR and OT_O2_DEVICE, shong 5/9/2022
        -- https://unite.nih.gov/workspace/fusion/spreadsheet/ri.fusion.main.document.0ad59020-752a-4865-b335-f12009ca6d77/spreadsheet/41579302943086893
        -- if obsgen_type = UD_N3C_O2_DEVICE  and obsgen_code is either ‘OT_O2_DEVICE’, or ‘ROOM_AIR’
        --and if o.obsgen_type = 'SM' lookup the concept_id in the pre-generated ventcode_xwalk table
        CASE WHEN o.obsgen_type = 'UD_N3C_O2_DEVICE' AND o.obsgen_code ='ROOM_AIR' THEN 2004208005 
             WHEN o.obsgen_type = 'UD_N3C_O2_DEVICE' AND o.obsgen_code ='OT_O2_DEVICE' THEN 2004208004
             WHEN o.obsgen_type = 'SM' THEN CAST(COALESCE(ventcode_xwalk.target_concept_id,0) as int)
             ELSE 0        
             END as device_concept_id,
        CAST(o.obsgen_start_date as date) as device_exposure_start_date,
        CAST(o.OBSGEN_START_DATETIME as timestamp) as device_exposure_start_datetime,
        CAST(o.obsgen_stop_date as date) as drug_exposure_end_date,
        CAST(o.OBSGEN_STOP_DATETIME as timestamp) as drug_exposure_end_datetime, 
        32817 as device_type_concept_id,
        CAST(null as string) AS unique_device_id, 
        CAST(null as int) as quantity, 
        CAST(null as long) as provider_id,
        encounterid as site_encounterid,
        CAST(null as long) as visit_detail_id,
        CAST(o.raw_obsgen_name as string) as device_source_value, 
        CAST(null as int) as device_source_concept_id, 
        'OBS_GEN' as domain_source,
        obsgenid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/obs_gen` o
    LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/vent_code_xwalk` ventcode_xwalk
    ON o.obsgen_code = ventcode_xwalk.src_code and ventcode_xwalk.CDM_TBL = 'OBS_GEN' and ventcode_xwalk.domain_id = 'Device'
    WHERE o.obsgen_type = 'SM' OR  obsgen_type = 'UD_N3C_O2_DEVICE' 
    AND o.obsgen_code is not null
),

all_domains as (
    SELECT * FROM (
        SELECT * FROM obs_clin 
            UNION ALL  
        SELECT * FROM med_admin 
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
      , row_number() OVER (
          PARTITION BY
            site_patid
          , device_concept_id
          , device_exposure_start_date
          , device_exposure_start_datetime
          , device_exposure_end_date
          , device_exposure_end_datetime
          , device_type_concept_id
          , unique_device_id
          , quantity
          , provider_id
          , site_encounterid
          , visit_detail_id
          , device_source_value
          , device_source_concept_id
          ORDER BY site_patid
      ) as row_index
  FROM all_domains
  WHERE device_concept_id IS NOT NULL
)

SELECT
  -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as device_exposure_id_51_bit
  -- Pass through the hashed id to join on lookup table in case of conflicts
  , hashed_id
  , site_patid
  , device_concept_id
  , device_exposure_start_date
  , device_exposure_start_datetime
  , device_exposure_end_date
  , device_exposure_end_datetime
  , device_type_concept_id
  , unique_device_id
  , quantity
  , provider_id
  , site_encounterid
  , visit_detail_id
  , device_source_value
  , device_source_concept_id
  , data_partner_id
  , domain_source
  , site_pkey
  , payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
                , COALESCE(site_patid, '')
                , COALESCE(device_concept_id, '')
                , COALESCE(device_exposure_start_date, '')
                , COALESCE(device_exposure_start_datetime, '')
                , COALESCE(device_exposure_end_date, '')
                , COALESCE(device_exposure_end_datetime, '')
                , COALESCE(device_type_concept_id, '')
                , COALESCE(unique_device_id, '')
                , COALESCE(quantity, '')
                , COALESCE(provider_id, '')
                , COALESCE(site_encounterid, '')
                , COALESCE(visit_detail_id, '')
                , COALESCE(device_source_value, '')
                , COALESCE(device_source_concept_id, '')
                , COALESCE(row_index, '')
                , site_pkey
        )) as hashed_id
        FROM final_table
    )
