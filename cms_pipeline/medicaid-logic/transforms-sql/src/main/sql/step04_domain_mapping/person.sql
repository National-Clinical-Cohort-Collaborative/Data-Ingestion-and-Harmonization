CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/person` AS
-- build location, person then death in that order
-- intermediate transformation done in "step03_5_person_prepared" folder

with filtered_person as (
    SELECT DISTINCT 
    up.PSEUDO_ID
    , p.BIRTH_DT
    , p.RACE_ETHNCTY_CD
    , p.SEX_CD
    , up.YEAR
    , up.MDCD_ENRLMT_DAYS_YR
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` p
    INNER JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/unique_person_id_most_recent_year_most_days`up on p.PSEUDO_ID = up.PSEUDO_ID and p.MDCD_ENRLMT_DAYS_YR = up.MDCD_ENRLMT_DAYS_YR and p.YEAR = up.YEAR
    WHERE char_length(p.RACE_ETHNCTY_CD) > 0 and char_length(p.BIRTH_DT) > 0 and RACE_ETHNCTY_CD <> 0   and LENGTH(SEX_CD)> 0 
    and up.PSEUDO_ID not in ( select PSEUDO_ID FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_bday`)
    and up.PSEUDO_ID not in ( select PSEUDO_ID FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_sex`)
    and up.PSEUDO_ID not in ( select PSEUDO_ID FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_race_values`)
),

xwalk_person as (
   SELECT DISTINCT /* 1% - 1.2k -- cleaned person  */
        p.PSEUDO_ID as medicaid_person_id
        , p.BIRTH_DT as birth_date  --in ddMMMNyyyy format 
        , MONTH(to_date(BIRTH_DT, 'ddMMMyyyy')) as month_of_birth
        , DAYOFMONTH(to_date(BIRTH_DT, 'ddMMMyyyy')) as day_of_birth
        , YEAR(to_date(BIRTH_DT, 'ddMMMyyyy')) as year_of_birth
        , CAST(to_date(BIRTH_DT, 'ddMMMyyyy') as timestamp) as birth_datetime
        --gender -- sex code  concept id 
        , CAST( gender.target_concept_id as int) as gender_concept_id
        , CAST(gender.source_concept_id AS int) AS gender_source_concept_id 
        , cast( SEX_CD as string) as gender_source_value
        --race -- if multi race value exist in the data then set it to null ---------------------------------------------------
        ---, case when cardinality (p.mutiracevals) > 0 then cast(null as int)
        , COALESCE(cast(race.target_concept_id AS int), CAST(0 AS int) )as race_concept_id
        , CAST( race.source_concept_id AS int) as race_source_concept_id
        , CAST( race.src_code AS string) as race_source_value
        ---    end as race_concept_id
        -- ethnicity - if race cd is 5 then hispanic =38003563  otherwise not hispanic-------------------------------
        , CASE WHEN RACE_ETHNCTY_CD = 0 THEN 0 -- 0 is unknown
            WHEN RACE_ETHNCTY_CD in ('1','2','3','4','5','6' ) THEN 38003564 ----'Not Hispanic or Latino'--38003564
            WHEN RACE_ETHNCTY_CD = '7' THEN 38003563  ---Hispanic, all races
        ELSE 38003564
        END as ethnicity_concept_id
        , CAST(0 AS int) as ethnicity_source_concept_id
        , CAST(RACE_ETHNCTY_CD AS string) as ethnicity_source_value
        --
        , CAST(l.location_id AS long) as location_id
        , CAST( NULL AS long) as provider_id
        , CAST( NULL AS long) as care_site_id
        , CAST(p.PSEUDO_ID AS string) as person_source_value 
        -- , CASE
        --         WHEN RACE_ETHNCTY_CD = 0 THEN 'Unknown'--0
        --         WHEN RACE_ETHNCTY_CD = 1 THEN 'Not Hispanic or Latino'--38003564
        --         WHEN RACE_ETHNCTY_CD = 2 THEN 'Not Hispanic or Latino'--38003564
        --         WHEN RACE_ETHNCTY_CD = 3 THEN 'Not Hispanic or Latino'--38003564
        --         WHEN RACE_ETHNCTY_CD = 4 THEN 'Not Hispanic or Latino'--38003564
        --         WHEN RACE_ETHNCTY_CD = 5 THEN 'Not Hispanic or Latino'--38003564
        --         WHEN RACE_ETHNCTY_CD = 6 THEN 'Not Hispanic or Latino'--38003564
        --         WHEN RACE_ETHNCTY_CD = 7 THEN 'Hispanic or Latino'--38003563
        --         ELSE 'Unknown'
        --     END as eth_concept_name
     -- FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` p
     FROM filtered_person p
      --INNER JOIN unique_peson_id_without_multirace_values up on p.PSEUDO_ID = up.PSEUDO_ID
      --Left join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/care_site` c
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/medicaid_valueset_xwalk` gender on gender.column_name = 'SEX_CD' AND p.SEX_CD = gender.src_code 
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/medicaid_valueset_xwalk` race on race.column_name = 'RACE_ETHNCTY_CD' AND trim(RACE_ETHNCTY_CD) = race.src_code 
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location_latest` l on l.PSEUDO_ID = p.PSEUDO_ID
      --LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location_history` l on l.PSEUDO_ID = p.PSEUDO_ID
      --LEFT JOIN `ri.foundry.main.dataset.cc4b1d3b-38fd-4e99-9fcb-84ab7e9c9ebd` l on l.PSEUDO_ID = p.PSEUDO_ID
      WHERE char_length(p.RACE_ETHNCTY_CD) > 0 and char_length(p.BIRTH_DT) > 0 and RACE_ETHNCTY_CD <> 0 ---rows with null or empty string race values are causing dup person id issue, therefore filter them out
      and char_length(race.target_concept_id) > 0 and race.target_concept_id is not null --- race and ethnicity is overloaded in RACE_ETHNCTY_CD we only want to add non-null race value 7 is ethnicity code   
     ---- add person with multiple race or ethnicity separately in the following step. 
      and p.PSEUDO_ID not in ( select distinct PSEUDO_ID from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_eth_values` )
      and p.PSEUDO_ID not in ( select distinct PSEUDO_ID from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_race_values` )
      and p.PSEUDO_ID not in ( select distinct PSEUDO_ID from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_demographic_values` )
    ), 

update_race_and_ethnicity_values as (
        select distinct 
            -- person with sigle race and ethnicity values
            ---add person with multi race values
            ---add person with multi ethnicity  values
        medicaid_person_id
        , birth_date  --in ddMMMNyyyy format 
        , month_of_birth
        , day_of_birth
        , year_of_birth
        , birth_datetime
        --gender -- sex code  concept id 
        , gender_concept_id
        , gender_source_concept_id 
        , gender_source_value
        --race
        , race_concept_id
        , race_source_concept_id
        , race_source_value
        ---    end as race_concept_id
        -- ethnicity - if race cd is 5 then hispanic =38003563  otherwise not hispanic-------------------------------
        , ethnicity_concept_id
        , ethnicity_source_concept_id
        , ethnicity_source_value
        -- other
        , location_id
        , provider_id
        , care_site_id
        , person_source_value 
        FROM xwalk_person p
        -- add person with multiple race-----------------------------------------
        union 
        SELECT 
        p.PSEUDO_ID as medicaid_person
        , p.BIRTH_DT as birth_date  --in ddMMMNyyyy format 
        , MONTH(to_date(BIRTH_DT, 'ddMMMyyyy')) as month_of_birth
        , DAYOFMONTH(to_date(BIRTH_DT, 'ddMMMyyyy')) as day_of_birth
        , YEAR(to_date(BIRTH_DT, 'ddMMMyyyy')) as year_of_birth
        , CAST(to_date(BIRTH_DT, 'ddMMMyyyy') as timestamp) as birth_datetime
        --gender -- sex code  concept id 
        , CAST( gender.target_concept_id as int) as gender_concept_id
        , CAST(gender.source_concept_id AS int) AS gender_source_concept_id 
        , cast( SEX_CD as string) as gender_source_value
        --race
        , CAST (0 as int)  as race_concept_id
        , CAST( 0 as int) as race_source_concept_id
        , CAST( cardinality(multirace.multiracevals) as string) as race_source_value
        ---    end as race_concept_id
        -- ethnicity - if race cd is 5 then hispanic =38003563  otherwise not hispanic-------------------------------
        , CASE WHEN RACE_ETHNCTY_CD = 0 THEN 0 -- 0 is unknown
            WHEN RACE_ETHNCTY_CD in ('1','2','3','4','5','6' ) THEN 38003564 ----'Not Hispanic or Latino'--38003564
            WHEN RACE_ETHNCTY_CD = '7' THEN 38003563  ---Hispanic, all races
        ELSE 38003564
        END as ethnicity_concept_id
        , CAST(0 AS int) as ethnicity_source_concept_id
        , CAST(RACE_ETHNCTY_CD AS string) as ethnicity_source_value
        -- other
        , location_id
        , CAST( NULL AS long) as provider_id
        , CAST( NULL AS long) as care_site_id
        , p.PSEUDO_ID as person_source_value 
        FROM filtered_person p
        INNER JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_race_values` multirace ON p.PSEUDO_ID = multirace.PSEUDO_ID
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/medicaid_valueset_xwalk` gender on gender.column_name = 'SEX_CD' AND p.SEX_CD = gender.src_code 
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location_latest` l on l.PSEUDO_ID = p.PSEUDO_ID
        where p.RACE_ETHNCTY_CD !=7 --- we are only adding person with multiple race codes not ethnicity codes,
       UNION
        -- person with multi eth values------------------------------------------------------------------
        SELECT 
        p.PSEUDO_ID as medicaid_person
        , p.BIRTH_DT as birth_date  --in ddMMMNyyyy format 
        , MONTH(to_date(BIRTH_DT, 'ddMMMyyyy')) as month_of_birth
        , DAYOFMONTH(to_date(BIRTH_DT, 'ddMMMyyyy')) as day_of_birth
        , YEAR(to_date(BIRTH_DT, 'ddMMMyyyy')) as year_of_birth
        , CAST(to_date(BIRTH_DT, 'ddMMMyyyy') as timestamp) as birth_datetime
        --gender -- sex code  concept id 
        , CAST( gender.target_concept_id as int) as gender_concept_id
        , CAST(gender.source_concept_id AS int) AS gender_source_concept_id 
        , cast( SEX_CD as string) as gender_source_value
        --race
        , CAST (race.target_concept_id as int)  as race_concept_id
        , CAST( 0 as int) as race_source_concept_id
        , CAST( RACE_ETHNCTY_CD AS string ) as race_source_value
        ---    end as race_concept_id
        -- ethnicity - if race cd is 5 then hispanic =38003563  otherwise not hispanic-------------------------------
        , CAST( 0 AS int) as ethnicity_concept_id
        , CAST( 0 AS int) as ethnicity_source_concept_id
        , CAST(cardinality(multi_eth.multi_ethnicityvals ) AS string) as ethnicity_source_value
        -- other
        , location_id
        , CAST( NULL AS long) as provider_id
        , CAST( NULL AS long) as care_site_id
        , p.PSEUDO_ID as person_source_value 
        FROM filtered_person p
        INNER JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_eth_values`  multi_eth ON p.PSEUDO_ID = multi_eth.PSEUDO_ID
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/medicaid_valueset_xwalk` gender on gender.column_name = 'SEX_CD' AND p.SEX_CD = gender.src_code 
         LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/medicaid_valueset_xwalk` race on race.column_name = 'RACE_ETHNCTY_CD' AND RACE_ETHNCTY_CD = race.src_code
        LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/location_latest` l on l.PSEUDO_ID = p.PSEUDO_ID
        where char_length(race.target_concept_id) > 0 and race.target_concept_id is not null --- race and ethnicity is overloaded in RACE_ETHNCTY_CD we only want to add non-null race value
    )

Select distinct 
       md5(concat_ws(
              ';'
        , COALESCE(p.medicaid_person_id, '')
        , COALESCE(p.birth_date, '')
        , COALESCE(p.gender_concept_id, '')
        , COALESCE(p.race_concept_id, '')
        , COALESCE(p.ethnicity_concept_id, '')
        ---, COALESCE(p.location_id, '')
        )) as person_id,
    * 
    from update_race_and_ethnicity_values p
    