CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_race_values` AS
    
    --Identify person with multiple race values,  if we have many person with multiple race values, we will need to indicate those people as multi-race
    --use 0 as race in a new column, OMOP does not have a multi-race concept id 
    -- 1	White, non-Hispanic
    -- 2	Black, non-Hispanic
    -- 3	Asian, non-Hispanic
    -- 4	American Indian and Alaska Native (AIAN), non-Hispanic
    -- 5	Hawaiian/Pacific Islander
    -- 6	Multiracial, non-Hispanic
    -- 7	Hispanic, all races
    with multi_race_person as (
    SELECT distinct
    PSEUDO_ID
    FROM `ri.foundry.main.dataset.7aa0e696-fe53-4e63-be90-75fcca58392a` p
    where LENGTH (RACE_ETHNCTY_CD) > 0 and RACE_ETHNCTY_CD != 7 and RACE_ETHNCTY_CD != 6
    group by PSEUDO_ID
    having count( DISTINCT RACE_ETHNCTY_CD) > 1
    ),
    person_with_multi_race_values as (
        SELECT 
        PSEUDO_ID, 
        RACE_ETHNCTY_CD
        FROM `ri.foundry.main.dataset.7aa0e696-fe53-4e63-be90-75fcca58392a` p
        where PSEUDO_ID in ( select PSEUDO_ID from multi_race_person) and RACE_ETHNCTY_CD !=7 and RACE_ETHNCTY_CD !=6
        order by PSEUDO_ID
    ),
    multi_race_values as (--196 7/29/23
    SELECT distinct
        PSEUDO_ID,
        (collect_set(pr.RACE_ETHNCTY_CD)) as multiracevals
    FROM person_with_multi_race_values pr
    group by 1
    order by PSEUDO_ID    
    )
    SELECT ---196
        PSEUDO_ID,
        True as multi_race_flag,
        multiracevals, 
        cardinality(multiracevals)  as multiracecnt
        FROM multi_race_values