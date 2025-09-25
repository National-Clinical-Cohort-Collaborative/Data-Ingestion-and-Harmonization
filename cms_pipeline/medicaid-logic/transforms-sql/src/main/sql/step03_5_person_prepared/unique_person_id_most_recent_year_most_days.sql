CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/unique_person_id_most_recent_year_most_days` AS

with unique_person as (    
    SELECT distinct PSEUDO_ID, COALESCE(trim(RACE_ETHNCTY_CD), '0') AS RACE, YEAR AS YEAR, MDCD_ENRLMT_DAYS_YR,
    RANK() OVER (PARTITION BY PSEUDO_ID ORDER BY YEAR DESC ) as idx 
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` p
    WHERE char_length(RACE_ETHNCTY_CD) > 0 
    order by PSEUDO_ID
),
--most recent year
unique_person_id_most_recent as (
    select PSEUDO_ID, YEAR, cast(MDCD_ENRLMT_DAYS_YR as int) as MDCD_ENRLMT_DAYS_YR
    FROM unique_person
    where idx = 1
),
-- most days in the year
unique_person_id_most_days as (
    select PSEUDO_ID, YEAR, MDCD_ENRLMT_DAYS_YR,
    RANK() OVER (PARTITION BY PSEUDO_ID ORDER BY MDCD_ENRLMT_DAYS_YR DESC) as idx 
    FROM unique_person_id_most_recent
)
--most recent and most days
select DISTINCT PSEUDO_ID, YEAR, MDCD_ENRLMT_DAYS_YR
    FROM unique_person_id_most_days
    where idx = 1
