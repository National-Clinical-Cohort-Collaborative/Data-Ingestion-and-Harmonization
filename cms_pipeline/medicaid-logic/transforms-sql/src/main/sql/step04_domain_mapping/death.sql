CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/death` AS
    with death as (
      SELECT DISTINCT 
        --pkey
         PSEUDO_ID as medicaid_person_id
        , to_date(BIRTH_DT, 'ddMMMyyyy') as birth_date
        , to_date(DEATH_DT, 'ddMMMyyyy') as death_date
        , SEX_CD
        , RACE_ETHNCTY_CD
        , BENE_STATE_CD
        , BENE_CNTY_CD
        , BENE_ZIP_CD
        , 32506 as death_type_concept_id -- 32506-	Payer enrollment status "Deceased"
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` p
    WHERE DEATH_DT IS NOT NULL and DEATH_DT != '' and trim(length(DEATH_DT )) >0
    )

    SELECT DISTINCT
       -- death.pkey
        death.medicaid_person_id
        , p.person_id as person_id 
        , CAST (death_date AS DATE) AS death_date
        , CAST(death_date as timestamp) as death_datetime
        , CAST( 32810 AS int ) as death_type_concept_id -- 32810-claim type 
        , CAST(null as int) as cause_concept_id
        , CAST(null as string) as cause_source_value
        , CAST(NULL as int) as cause_source_concept_id
    FROM death
    inner JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/person` p on p.medicaid_person_id = death.medicaid_person_id
    WHERE death_date IS NOT NULL
   