CREATE TABLE `ri.foundry.main.dataset.5c6b9d92-b68f-46da-9401-d7e47bb49327` AS

WITH all_providers AS ( 
 
    SELECT DISTINCT 
      PROVIDER as care_site_id 
      , CAST( 8717 AS int ) as place_of_service_concept_id
      , 'Inpatient Facility' as place_of_service_source_value
    FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6`
    UNION 

    SELECT DISTINCT 
      PROVIDER as care_site_id 
       , CAST( 8756 AS int ) as place_of_service_concept_id
      , 'Outpatient Facility' as place_of_service_source_value
    FROM `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a`
    UNION

    SELECT DISTINCT 
      PROVIDER as care_site_id 
      , CAST( 8546 AS int ) as place_of_service_concept_id
      , 'Hospice Facility' as place_of_service_source_value
      FROM `ri.foundry.main.dataset.4b887dbc-f591-482f-85a7-c11582686823`
    UNION
    SELECT DISTINCT 
      PROVIDER as care_site_id
      , CAST( 8863 AS int ) as place_of_service_concept_id
      , 'Skilled Nursing Facility' as place_of_service_source_value
    FROM `ri.foundry.main.dataset.a983c4ae-0a51-470d-9286-e19c1193be74`
    UNION
    SELECT DISTINCT 
      PROVIDER as care_site_id 
     , CAST( 581476 AS int ) as place_of_service_concept_id
      , 'Home Visit' as place_of_service_source_value
    FROM `ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4`
    -- tax num may be associated with a provider or / person/ - how to distinguish from facility vs provider
    --UNION
    --SELECT TAX_NUM_ID as care_site_id FROM `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0`
    --UNION
    -- SELECT TAX_NUM as care_site_id FROM `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f`
    -- UNION 
    -- SELECT PRESCRIBER_ID as care_site_id FROM  `ri.foundry.main.dataset.654c7522-38f1-4ca6-8a09-62abbbbb1991`
    -- UNION
    -- SELECT SRVC_PROVIDER_ID as care_site_id FROM  `ri.foundry.main.dataset.654c7522-38f1-4ca6-8a09-62abbbbb1991`
    WHERE length( trim(PROVIDER)) > 0  
), 

cms_care_site as ( 
  SELECT DISTINCT 
    care_site_id
    , care_site_id as orig_care_site_id --- save for mapping file, orig care site id ( ccn ) mapping to new care_site_id 
    , chsp.ccn 
    --, chsp.health_sys_id -- multiple health_sys_id can exist for a given ccn value, therefore we are linking this number in care_site_year (a.k.a. health_system)
    , CAST( null AS string) AS care_site_name
  --, CAST( place_of_service_concept_id as int ) AS place_of_service_concept_id
  , CAST( null as int ) AS place_of_service_concept_id
  , CAST( null as long ) as location_id
  , CAST( null as string ) as care_site_source_value -- null out the source value 
  , CAST( null as string ) as place_of_service_source_value
  FROM all_providers cs 
  LEFT JOIN  `ri.foundry.main.dataset.d24703a2-1d42-424f-8e33-6aed5981f7c9` chsp
    on cs.care_site_id = chsp.ccn
    where chsp.health_sys_id is not null and chsp.ccn is not null
)

select distinct * from cms_care_site
   