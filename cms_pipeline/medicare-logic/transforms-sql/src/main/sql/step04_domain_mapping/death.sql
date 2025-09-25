CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/death` AS
    with death as (
      SELECT DISTINCT 
          BID as person_id
        , to_date(BENE_BIRTH_DT, 'ddMMMyyyy') as birth_date
        , to_date(BENE_DEATH_DT, 'ddMMMyyyy') as death_date
        , SEX_IDENT_CD
        , BENE_RACE_CD
        , STATE_CODE
        , COUNTY_CD
        , ZIP_CD
        , 32506 as death_type_concept_id
    FROM `ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d`
    WHERE BENE_DEATH_DT IS NOT NULL and BENE_DEATH_DT != '' and trim(length(BENE_DEATH_DT )) >0
    )

    SELECT DISTINCT
         CAST (person_id AS long ) as person_id
        , CAST (death_date AS DATE) AS death_date
        , CAST(death_date as timestamp) as death_datetime
        , CAST( 32810 AS int ) as death_type_concept_id
        , CAST(null as int) as cause_concept_id
        , CAST(null as string) as cause_source_value
        , CAST(NULL as int) as cause_source_concept_id
    FROM death
    WHERE death_date IS NOT NULL
   