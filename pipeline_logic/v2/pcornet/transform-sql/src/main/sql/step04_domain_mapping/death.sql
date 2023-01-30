CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/death` AS

with death_cause_ext as (
  SELECT dc.*, 
  cs.target_concept_id,
  cs.source_concept_id
  FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/death_cause` dc
  INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/p2o_code_xwalk` cs 
    ON cs.CDM_TBL ='DEATH_CAUSE' 
    AND dc.death_cause_code = cs.src_code_type 
    AND trim(dc.death_cause) = trim(cs.source_code)
)

SELECT
     death.patid as site_patid
   , CAST(death_date as date) as death_date
   , CAST(null as timestamp) as death_datetime 
   , 32817 as death_type_concept_id
   , CAST(dc.target_concept_id as int) as cause_concept_id  -- this field is number, ICD codes don't fit
   , CAST(dc.death_cause as string) as cause_source_value --put raw ICD10 codes here, it fits the datatype -VARCHAR, and is useful for downstream analytics
   , CAST(dc.source_concept_id as int) as cause_source_concept_id -- this field is number, ICD codes don't fit
   , 'DEATH' AS domain_source
   , death.data_partner_id
   , death.payload
FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/death` death
LEFT JOIN death_cause_ext dc
  ON dc.patid = death.patid
LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` dt 
  ON dt.CDM_TBL = 'DEATH' 
  AND dt.CDM_TBL_COLUMN_NAME = 'DEATH_SOURCE' 
  AND dt.SRC_CODE = death.death_source

UNION ALL

-- encounter
SELECT
    site_patid,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id,
    domain_source,
    data_partner_id,
    payload
FROM
    (
    SELECT
      d.patid as site_patid,
      -- COALESCE(d.discharge_date, d.admit_date) AS death_date, --** MB: coalesce, or just use discharge date? 
      -- COALESCE(d.discharge_date, d.admit_date) AS death_datetime,
      CAST(d.discharge_date as date) AS death_date,
      CAST(null as timestamp) AS death_datetime,
      32823 AS death_type_concept_id,
      -- cs.target_concept_id    AS cause_concept_id,
      -- c.condition_source      AS cause_source_value,
      -- nvl(cs.source_concept_id, 0) AS cause_source_concept_id,
      CAST(null as int) AS cause_concept_id,
      CAST(null as string) AS cause_source_value,
      CAST(null as int) AS cause_source_concept_id,
      'ENCOUNTER' AS domain_source,
      d.data_partner_id,
      d.payload,
      ROW_NUMBER() OVER(
        PARTITION BY d.patid
        ORDER BY d.discharge_date DESC
        ) rn
    FROM
        `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` d
      LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/death` dc 
        ON dc.patid = d.patid
      WHERE
          discharge_status = 'EX'
          AND dc.patid IS NULL    -- Prevents duplicate death entries
    ) cte_ex
WHERE cte_ex.rn = 1
