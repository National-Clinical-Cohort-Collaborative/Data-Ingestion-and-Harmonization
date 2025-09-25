CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_eth_values` AS
    with multi_eth_person as 
    (
    --Identify person with multiple race values,  if we have many person with multiple race values, we will need to indicate those people as multi-race
    --use 0 as race in a new column, OMOP does not have a multi-race concept id 
    -- 1	White, non-Hispanic
    -- 2	Black, non-Hispanic
    -- 3	Asian, non-Hispanic
    -- 4	American Indian and Alaska Native (AIAN), non-Hispanic
    -- 5	Hawaiian/Pacific Islander
    -- 6	Multiracial, non-Hispanic
    -- 7	Hispanic, all races
    SELECT distinct
    PSEUDO_ID, 
    case when RACE_ETHNCTY_CD in ('1','2','3', '4','5', '6' ) then cast( 38003564 as int)  -- non hispanic
    when RACE_ETHNCTY_CD in ('7') then cast( 38003563 as int)  --hispanic 
    else  0 
    end as ethnicity_concept_id 
    FROM `ri.foundry.main.dataset.7aa0e696-fe53-4e63-be90-75fcca58392a` p
    where LENGTH (RACE_ETHNCTY_CD) > 0 
    ),

    multi_eth as (
        SELECT distinct -----692
        PSEUDO_ID, count( distinct ethnicity_concept_id) as eth_cnt
        FROM multi_eth_person p  
        ---having count( DISTINCT cast(ethnicity_concept_id) > 1
    group by PSEUDO_ID 
    ),

    person_with_multi_eth as (
        select distinct PSEUDO_ID,  eth_cnt from multi_eth where eth_cnt > 1
    )
    SELECT DISTINCT 
        PSEUDO_ID,    
        True as multi_race_flag,
        (collect_set(trim(p.RACE_ETHNCTY_CD))) as multi_ethnicityvals
        FROM `ri.foundry.main.dataset.7aa0e696-fe53-4e63-be90-75fcca58392a` p
        where PSEUDO_ID in ( select distinct PSEUDO_ID from person_with_multi_eth)
        GROUP BY 1, 2
        order by PSEUDO_ID
