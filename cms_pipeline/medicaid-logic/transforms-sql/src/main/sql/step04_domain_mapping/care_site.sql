CREATE TABLE `ri.foundry.main.dataset.792e64d0-ca52-4c6b-9f7f-bd88dca342ff` AS
   
    ---457 1% care_site
    --in vocab: Medicare Specialty, CMS Place of Service, domain Visit 
   ---SELECT DISTINCT  BLG_PRVDR_SPCLTY_CD as place_of_service 
   ---FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` 
  
  --other NPI columns contained provider npi not listed in the BLG_PRVDR_NPI column. Find the missing ones and add to the care_site dataset
  with  entity_care_site_2_include_from_other_npi_columns as (
     select distinct 
     care_site_npi 
     , specialty_cd
     from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/care_site_2_include`
     where care_site_npi not in ( select distinct BLG_PRVDR_NPI from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/ip`)
   ),
 
  all_providers AS (
    -- care_site from ip
    -- unique combination of care_site_npi and place of service based on BLG_PRVDR_SPCLTY_CD code value.  BLG_PRVDR_SPCLTY_CD is referenced to look up the mapped placed of visit. Refer to the cms_medicaid_place_of_visit_xwalk
    SELECT DISTINCT 
    md5(concat_ws(
              ';'
          , COALESCE(ip.BLG_PRVDR_NPI, '')
          , COALESCE(ip.BLG_PRVDR_SPCLTY_CD, '')
          , COALESCE(vx.target_concept_id, '')
          , COALESCE(vx.source_concept_name, '')
          ,COALESCE(l.location_source_value, '')
          )) as care_site_id
    -- , CAST(trim(ip.BLG_PRVDR_NPI) as long) as l_care_site_npi
     , trim(ip.BLG_PRVDR_NPI) as care_site_npi
     , l.location_source_value as care_site_name
     , vx.target_concept_id as place_of_service_concept_id
     ---, location_id from location
     , CAST( l.location_id AS long) 
     , ip.BLG_PRVDR_SPCLTY_CD as place_of_service_code 
     , CAST( BLG_PRVDR_NPI  as string) as care_site_source_value
     , CAST(( BLG_PRVDR_SPCLTY_CD ||':'|| vx.source_concept_name) as string) as place_of_service_source_value
     FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip
     LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/cms_medicaid_place_of_visit_xwalk` vx on vx.source_concept_code = ip.BLG_PRVDR_SPCLTY_CD
     --- BLG_PRVDR_NPI is the location id 
     LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location` l ON l.location_id = CAST(trim(ip.BLG_PRVDR_NPI) as long)
     WHERE CHAR_LENGTH(trim(ip.BLG_PRVDR_NPI)) > 0 and CHAR_LENGTH(trim(ip.BLG_PRVDR_SPCLTY_CD)) > 0 and trim(ip.BLG_PRVDR_NPI) is not null-- not null or not empty string 

     UNION
     
     -- include care_site from the other npi columns not included in the BLG_PRVDR_NPI
     SELECT DISTINCT 
      md5(concat_ws(
              ';'
          , COALESCE(othernpi.care_site_npi, '')
          , COALESCE(othernpi.specialty_cd, '')
          , COALESCE(vx.target_concept_id, '')
          , COALESCE(vx.source_concept_name, '')
          , COALESCE(l.location_source_value, '')
          )) as care_site_id
      , care_site_npi 
      , l.location_source_value as care_site_name
      , vx.target_concept_id as place_of_service_concept_id
      , CAST( l.location_id AS long)
      , othernpi.specialty_cd as place_of_service_code 
      , CAST( othernpi.care_site_npi as string) as care_site_source_value
      , CAST(( care_site_npi ||':'|| vx.source_concept_name) as string) as place_of_service_source_value
     FROM entity_care_site_2_include_from_other_npi_columns othernpi
     LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/cms_medicaid_place_of_visit_xwalk` vx on vx.source_concept_code = othernpi.specialty_cd
     --- care_site_npi is the location id 
     LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location` l ON l.location_id = CAST(trim(othernpi.care_site_npi) as long)
     WHERE CHAR_LENGTH(trim(othernpi.care_site_npi)) > 0 and CHAR_LENGTH(trim(othernpi.specialty_cd)) > 0 and trim(othernpi.specialty_cd) is not null-- not null or not empty string 
 ),

medicaid_care_site as ( 
  SELECT DISTINCT 
    care_site_id
    , care_site_id AS orig_care_site_id
    , care_site_npi
    , CAST( null AS string) AS care_site_name
    , CAST( null as int ) AS place_of_service_concept_id
    , CAST( null as long ) as location_id
    , CAST( null as string ) as care_site_source_value -- null out the source value 
    , CAST( null as string ) as place_of_service_source_value
  FROM all_providers cs 
)

select distinct * from medicaid_care_site
   
