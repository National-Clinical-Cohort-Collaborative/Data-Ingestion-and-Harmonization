CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location_latest` AS

   with multiple_location as (
    -- if multiple locations exist per person, pick the latest year location
    SELECT 
    location_id, PSEUDO_ID, year_num, BENE_STATE_CD, BENE_ZIP_CD,
    -- rank over the year_num so we can pick the lastest one by year
    RANK() OVER (PARTITION BY PSEUDO_ID ORDER BY year_num DESC) as idx2 
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location_history` 
   )

   SELECT *
   FROM multiple_location
   WHERE idx2=1


