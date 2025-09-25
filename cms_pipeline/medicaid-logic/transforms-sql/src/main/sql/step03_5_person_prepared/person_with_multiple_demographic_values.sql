CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_demographic_values` AS
    ---1	White, non-Hispanic
    -- 2	Black, non-Hispanic
    -- 3	Asian, non-Hispanic
    -- 4	American Indian and Alaska Native (AIAN), non-Hispanic
    -- 5	Hawaiian/Pacific Islander
    -- 6	Multiracial, non-Hispanic
    -- 7	Hispanic, all races
    -- do we have a person who has multiple values for race as well as multiple valures for ethnicity?
    -- check to see if there are person who are found in both datasets:
    --if the person is found in both build the dataset found in both.
    with person_with_multi_race_and_multi_eth as (
        SELECT distinct
        eth.PSEUDO_ID 
        FROM  `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person_with_duplicate_demographics/person_with_multiple_eth_values` eth 
        INNER JOIN  `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person_with_duplicate_demographics/person_with_multiple_race_values` mrace 
        on mrace.PSEUDO_ID = eth.PSEUDO_ID
    )
    select distinct 
        PSEUDO_ID, 
        (collect_set(trim(p.RACE_ETHNCTY_CD))) as multidemo_vals
        FROM `ri.foundry.main.dataset.7aa0e696-fe53-4e63-be90-75fcca58392a` p
        where PSEUDO_ID in ( select PSEUDO_ID from person_with_multi_race_and_multi_eth) 
        group by 1
        order by PSEUDO_ID
    
