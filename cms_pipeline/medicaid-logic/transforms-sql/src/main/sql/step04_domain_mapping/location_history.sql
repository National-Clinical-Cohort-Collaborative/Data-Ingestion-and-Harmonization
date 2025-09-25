CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location_history` AS
      --- only one address is stored for a person in OMOP so we have to pick one from history of different addresses the person may have
      --- take the latest by year and then again by zip code if dup exists.   
      -- multiple age group can exist in the data for the same person, pick the one with oldest age i.e. most recent data   
      with most_recent_age_data as 
      ( SELECT DISTINCT 
        PSEUDO_ID,
        CAST( AGE AS INT) AS age_num,
        CAST( YEAR as int) as year_num,
        BENE_STATE_CD,
        BENE_ZIP_CD
        FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` b
      ),
      most_recent_age as 
      (
        SELECT   PSEUDO_ID,
        age_num,
        year_num,
        BENE_STATE_CD,
        BENE_ZIP_CD, 
        rank() over (partition by PSEUDO_ID, year_num, BENE_STATE_CD, BENE_ZIP_CD order by age_num DESC) as age_rank
        FROM most_recent_age_data
      ),
      most_recent_age_group as (
        SELECT  * 
        FROM most_recent_age
        WHERE age_rank = 1 
      ), 
      person_location_history as (
        SELECT DISTINCT 
        PSEUDO_ID,
        --YEAR,
        age_num,
        year_num, 
        BENE_STATE_CD,
        BENE_ZIP_CD, 
        --BENE_CNTY_CD, -- it is possible to have two different county values for a same person 
        --need location id 
        md5( concat_ws(';'
        , COALESCE(PSEUDO_ID, '')  
       -- , COALESCE(BENE_CNTY_CD, '')
        , COALESCE(BENE_STATE_CD, '')
        , COALESCE(BENE_ZIP_CD, '')
        , COALESCE(year_num, '')
        , COALESCE('medicaid', '')
        )) as hashed_location_id
        --FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` b
        FROM most_recent_age_group
        ), 
        rank_location_by_year as (
        SELECT distinct 
        hashed_location_id, 
        PSEUDO_ID,
        year_num,
        BENE_STATE_CD,
        BENE_ZIP_CD, 
       -- BENE_CNTY_CD,
        RANK() OVER (PARTITION BY PSEUDO_ID, BENE_STATE_CD, BENE_ZIP_CD  ORDER BY year_num DESC) as idx --985 rows distinct list of ids where id=1
        FROM person_location_history
        WHERE char_length(PSEUDO_ID) > 0 
        order by PSEUDO_ID
        ), 
        last_known_by_year as (
                --pick the latest by year
                select distinct *,
                --CAST(conv(substr(hashed_location_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as location_id
                CAST(conv(substr(hashed_location_id, 1, 15), 16, 10) as bigint) as location_id
                from rank_location_by_year 
                where idx = 1
        )
        -- incase dups exist within the same year sort again by zip so that we only have one address for a given person id
        select * from (
            SELECT distinct 
            location_id, 
            PSEUDO_ID,
            year_num,
            BENE_STATE_CD,
            BENE_ZIP_CD, 
            ---BENE_CNTY_CD,
            RANK() OVER (PARTITION BY PSEUDO_ID ORDER BY BENE_ZIP_CD DESC) as idx --985 rows distinct list of ids where id=1
            FROM last_known_by_year
            WHERE char_length(PSEUDO_ID) > 0 
            order by PSEUDO_ID
            )
            where idx = 1
 
