CREATE TABLE `ri.foundry.main.dataset.cc97e403-dc1b-477c-b240-62497d067cc8` AS
    -- 1 = white, 2 = black, 3= others, 4=Asian, 5 hispanic, 0= unknown, 6=North American Native
    -- SELECT 
    --         BID as person_id
    --       , BENE_BIRTH_DT
    --       , MONTH(to_date(BENE_BIRTH_DT, 'ddMMMyyyy')) as month_of_birth
    --       , DAYOFMONTH(to_date(BENE_BIRTH_DT, 'ddMMMyyyy')) as day_of_birth
    --       , YEAR(to_date(BENE_BIRTH_DT, 'ddMMMyyyy')) as year_of_birth
    --       , SEX_IDENT_CD as gender_source_value
    --       , CASE 
    --             WHEN SEX_IDENT_CD = 1 THEN 8507
    --             WHEN SEX_IDENT_CD = 2 THEN 8532
    --         END as gender_concept_id
    --       , BENE_RACE_CD as race_source_value
    --       , CASE 
    --             WHEN BENE_RACE_CD = 1 THEN 8527
    --             WHEN BENE_RACE_CD = 2 THEN 8516
    --             WHEN BENE_RACE_CD = 3 THEN 0
    --             WHEN BENE_RACE_CD = 5 THEN 0
    --             ELSE 0
    --         END as race_concept_id
    --       , CASE
    --             WHEN BENE_RACE_CD = 1 THEN 38003564
    --             WHEN BENE_RACE_CD = 2 THEN 38003564
    --             WHEN BENE_RACE_CD = 3 THEN 38003564
    --             WHEN BENE_RACE_CD = 5 THEN 38003563
    --             ELSE 0
    --         END as ethnicity_concept_id
    -- FROM `ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d`
    /* cms2omop_valueset_xwalk is built using mbsf dataset */
    SELECT DISTINCT
        CAST(p.BID as long ) as person_id
        , BENE_BIRTH_DT
        , MONTH(to_date(BENE_BIRTH_DT, 'ddMMMyyyy')) as month_of_birth
        , DAYOFMONTH(to_date(BENE_BIRTH_DT, 'ddMMMyyyy')) as day_of_birth
        , YEAR(to_date(BENE_BIRTH_DT, 'ddMMMyyyy')) as year_of_birth
        , CAST(to_date(BENE_BIRTH_DT, 'ddMMMyyyy') as timestamp) as birth_datetime
        --race
        , CAST( race.target_concept_id AS int) as race_concept_id
        -- ethnicity - if race cd is 5 then hispanic =38003563 otherwise not hispanic
        , CASE
                WHEN BENE_RACE_CD = 0 THEN 0 -- 0 is unknown
                WHEN BENE_RACE_CD = 1 THEN 38003564
                WHEN BENE_RACE_CD = 2 THEN 38003564
                WHEN BENE_RACE_CD = 3 THEN 38003564
                WHEN BENE_RACE_CD = 4 THEN 38003564
                WHEN BENE_RACE_CD = 6 THEN 38003564
                WHEN BENE_RACE_CD = 5 THEN 38003563
                ELSE 0
            END as ethnicity_concept_id
        , CAST( pkey AS long) as location_id
        , CAST( NULL AS long) as provider_id
        , CAST( NULL AS long) as care_site_id
        , CAST(p.BID AS string) as person_source_value 
        , SEX_IDENT_CD as gender_source_value
        --gender concept id 
        , CAST( gender.target_concept_id as int) as gender_concept_id
        , CAST(NULL AS int) AS gender_source_concept_id

        , CAST(BENE_RACE_CD as string) as race_source_value
        ,  CAST(NULL AS int) as race_source_concept_id

        , CAST(BENE_RACE_CD as string) as ethnicity_source_value
        , CAST(NULL AS int) as ethnicity_source_concept_id
        , CASE
                WHEN BENE_RACE_CD = 0 THEN 'Unknown'--0
                WHEN BENE_RACE_CD = 1 THEN 'Not Hispanic or Latino'--38003564
                WHEN BENE_RACE_CD = 2 THEN 'Not Hispanic or Latino'--38003564
                WHEN BENE_RACE_CD = 3 THEN 'Not Hispanic or Latino'--38003564
                WHEN BENE_RACE_CD = 4 THEN 'Not Hispanic or Latino'--38003564
                WHEN BENE_RACE_CD = 6 THEN 'Not Hispanic or Latino'--38003564
                WHEN BENE_RACE_CD = 5 THEN 'Hispanic or Latino'--38003563
                ELSE 'Unknown'
            END as eth_concept_name
      FROM `ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d` p
      --Left join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/care_site` c
      LEFT JOIN `ri.foundry.main.dataset.48e954fe-58fe-4e94-ae5f-a93ba1f74382` gender on gender.column_name = 'SEX_IDENT_CD' AND SEX_IDENT_CD = gender.src_code 
      LEFT JOIN `ri.foundry.main.dataset.48e954fe-58fe-4e94-ae5f-a93ba1f74382` race on race.column_name = 'BENE_RACE_CD' AND BENE_RACE_CD = race.src_code 
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/location` l on l.location_id = p.pkey
 
      