CREATE TABLE `ri.foundry.main.dataset.711bdb10-2e9a-4786-954f-7e793a95df93` AS

with cms_care_site as ( 
  SELECT DISTINCT 
    care_site_id
    , chsp.ccn 
    , chsp.health_sys_id
  --, CAST( chsp.HOSPITAL_NAME AS string) AS care_site_name
  , CAST( null AS string) AS care_site_name
  --, CAST( place_of_service_concept_id as int ) AS place_of_service_concept_id
  , CAST( null as int ) AS place_of_service_concept_id
  , CAST( null as long ) as location_id
  , CAST( null as string ) as care_site_source_value
 -- , CAST( place_of_service_source_value as string ) as place_of_service_source_value
  , CAST( null as string ) as place_of_service_source_value
  ,chsp.compendium_year as chsp_year
    FROM  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/care_site` cs
    -- reference the clean file from cms provider characterization repo - ...analysis-provider-characterization/characterization files/pipeline/transform/clean/clean_chsp_compendium
    -- use the latest version here : /UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/characterization files/chsp-hospital-linkage_20-21-22_2024-02-21
    LEFT JOIN  `ri.foundry.main.dataset.d24703a2-1d42-424f-8e33-6aed5981f7c9` chsp
    on cs.care_site_id = chsp.ccn
    ---where chsp.health_sys_id is not null and chsp.ccn is not null 
    --- it is possible to have a ccn number without the health_sys_id
    where chsp.ccn is not null 
),

-- additional columns to add - approved as of December 2023, shong
-- For the external researcher version, we’ve been approved to include 3 variables
-- •	Size: small (1), medium (2), large (3), XL (4) 
-- •	Multistate: hospitals in 1 state (1), 2 states (2), or 3 or more states (3)
-- •	Teaching Intensity: non teaching (0), minor teaching (1), major teaching (3)
--sys_BEDS contain number of beds within a care site. 
hosp_size_rank as (
   select cs.*
  , compendium.compendium_year
  , compendium.chsp_compendium_id
  , compendium.health_sys_id as compendium_health_sys_id
  , compendium.teaching_intensity
  , compendium.teaching_intensity_category
  , compendium.multistate
  , compendium.multistate_category
  , compendium.care_site_size
  , compendium.care_site_size_category
  --, cast( SYS_BEDS as int) as sys_bed_num
  --, PERCENT_RANK() over ( order by cast(SYS_BEDS as int)) as percentRank
  --, compendium.care_site_size_category0
  -- TODO: use the updated chsp-compendium-dataset-from2018to12sept2023
  -- use the latest version: /UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/characterization files/chsp-compendium_20-21-22_2024-02-21
  from cms_care_site cs
  left join  `ri.foundry.main.dataset.1b448ee5-1fd2-46c8-a360-c91cce841fc0` compendium on cs.health_sys_id = compendium.health_sys_id and cs.chsp_year = compendium.compendium_year
  where compendium.health_sys_id is not null and cs.ccn is not null --and compendium.ranking = 1
), 

-- hosp as (
--   SELECT cs.* 
--     , hrank.HEALTH_SYS_ID as hrank_health_sys_id
--     , cast (hrank.teaching_intensity as int) as  teaching_intensity-- possibe values are  null, 0, 1, 2 
--     , CASE 
--           WHEN hrank.teaching_intensity = 0 THEN 'No Teaching'
--           WHEN hrank.teaching_intensity = 1 THEN 'Minor Teaching'
--           WHEN hrank.teaching_intensity = 2 THEN 'Major Teaching'
--           ELSE 'Not specified'
--       END AS teaching_intensity_category    
--     , cast(multistate as int) as multistate --- null, 1, 2, 3
--     , CASE 
--         WHEN multistate = 1 THEN 'one state'
--         WHEN multistate = 2 THEN 'two states'
--         WHEN multistate = 3 THEN 'three or more states'
--         ELSE 'Not specified'  
--         END AS multistate_category
--     , sys_bed_num
--     , ROUND(percentRank, 2) * 100 AS pc
--     , CASE 
--         WHEN ROUND(percentRank, 2) <= 0.30 THEN 'Small'
--         WHEN ROUND(percentRank, 2) > 0.30 AND ROUND(percentRank, 2) <= 0.60 THEN 'Medium'
--         WHEN ROUND(percentRank, 2) > 0.60 AND ROUND(percentRank, 2) < 0.95 THEN 'Large'
--         WHEN ROUND(percentRank, 2) >= 0.95 THEN 'Extra Large'
--         ELSE 'Not specified'
--     END AS hosp_size_category
--   from cms_care_site cs
--   LEFT JOIN hosp_size_rank hrank
--   on cs.health_sys_id = hrank.HEALTH_SYS_ID
-- where care_site_id is not NULL    
--)
care_site_final as (
  select distinct 
  ccn
  , compendium_health_sys_id as health_sys_id
  , compendium_year
  , care_site_name
  , place_of_service_concept_id
  , location_id
  , care_site_source_value
  , place_of_service_source_value
  -- additional care_site characteristics to add from compendiumm 
  , teaching_intensity
  , teaching_intensity_category
  , multistate
  , multistate_category
  , care_site_size
  , care_site_size_category
  , md5(concat_ws(
              ';'
        , COALESCE(care_site_id, '')
        , COALESCE(health_sys_id, '')
        , COALESCE(compendium_year, '')
        , COALESCE(care_site_id, '')      
        , COALESCE(health_sys_id, '')
        , COALESCE( chsp_compendium_id, '')
        )) as health_system_hash_id
  -- , care_site_id as orig_care_site_id
  from hosp_size_rank 
)

SELECT DISTINCT 
    cast(conv(substr(health_system_hash_id, 1, 15), 16, 10) as bigint) as health_system_id
    , *
    FROM care_site_final

-- schema check -  expected type : 
    -- "care_site": {
    --     "CARE_SITE_ID": T.LongType(),
    --     "CARE_SITE_NAME": T.StringType(),
    --     "PLACE_OF_SERVICE_CONCEPT_ID": T.IntegerType(),
    --     "LOCATION_ID": T.LongType(),
    --     "CARE_SITE_SOURCE_VALUE": T.StringType(),
    --     "PLACE_OF_SERVICE_SOURCE_VALUE": T.StringType(),
    -- },