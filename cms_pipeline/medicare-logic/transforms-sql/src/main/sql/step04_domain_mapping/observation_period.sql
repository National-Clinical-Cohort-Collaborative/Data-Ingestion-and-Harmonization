CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/observation_period` AS
    --logic
    -- Need to check the following columns for all the months the bid was covered
    ---MONTH = any of these variables: BENE_HI_CVRAGE_TOT_MONS, BENE_SMI_CVRAGE_TOT_MONS, BENE_HMO_CVRAGE_TOT_MONS, PLAN_CVRG_MONS_NUM
    --MDCR_ENTLMT_BUYIN_IND_01 - 12 
    --BENE_STATE_BUYIN_TOT_MONS
    --BENE_HI_CVRAGE_TOT_MONS/BENE_HI_CVRAGE_TOT_MONS/BENE_SMI_CVRAGE_TOT_MONS/BENE_HMO_CVRAGE_TOT_MONS/PTD_PLAN_CVRG_MONS
    --PTD_PLAN_CVRG_MONS-This variable is the number of months during the year that the beneficiary had Medicare Part D coverage. 
    --              CCW derives this variable by counting the number of months where the beneficiary had Part D coverage.
-- ,BENE_HI_CVRAGE_TOT_MONS
-- ,BENE_SMI_CVRAGE_TOT_MONS
-- ,BENE_HMO_CVRAGE_TOT_MONS
-- ,PTD_PLAN_CVRG_MONS
 ---We would have to build the separate enrollment period dataset for Tom who requested additional columns. 
 ---merging his request to this OMOP dataset is causing dup issue to the observation_period 
 ---simplyfying the code and commenting out most of this. 

    -- -- ------------------- for full year coverage    
    -- with full_year as (
    -- SELECT distinct 
    --     a.BID as person_id
    -- , a.min_observation_period_yr
    -- , b.max_observation_period_yr
    -- FROM
    -- (
    --     select distinct BID
    --     , PTD_PLAN_CVRG_MONS
    --     ,to_date( MIN (BENE_ENROLLMT_REF_YR ), 'yyyy'  )as min_observation_period_yr
    --     from`ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d`
    --     where BENE_ENROLLMT_REF_YR is not null and PTD_PLAN_CVRG_MONS = '12'
    --     group by BID, PTD_PLAN_CVRG_MONS
    -- ) a 
    -- LEFT JOIN (
    --     select BID
    --     , PTD_PLAN_CVRG_MONS
    --     , to_date( MAX (BENE_ENROLLMT_REF_YR ), 'yyyy'  )as max_observation_period_yr
    --     from`ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d`
    --     where BENE_ENROLLMT_REF_YR is not null and PTD_PLAN_CVRG_MONS = '12'
    --     group by BID, PTD_PLAN_CVRG_MONS
    -- ) b 
    -- on a.BID = b.BID
    -- ), 

    -- may_include_partial_yr as (
    -- SELECT distinct 
    --     a.BID as person_id
    -- , a.min_observation_period_yr
    -- , b.max_observation_period_yr
    -- FROM
    -- (
    --      select distinct BID
    --     , PTD_PLAN_CVRG_MONS
    --     ,to_date( MIN (BENE_ENROLLMT_REF_YR ), 'yyyy'  )as min_observation_period_yr
    --     from`ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d`
    --     where BENE_ENROLLMT_REF_YR is not null 
    --     group by BID, PTD_PLAN_CVRG_MONS
    -- ) a 
    -- LEFT JOIN (
    --     select BID
    --     , PTD_PLAN_CVRG_MONS
    --     , to_date( MAX (BENE_ENROLLMT_REF_YR ), 'yyyy'  )as max_observation_period_yr
    --     from`ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d`
    --     where BENE_ENROLLMT_REF_YR is not null
    --     group by BID, PTD_PLAN_CVRG_MONS
    -- ) b 
    -- on a.BID = b.BID
    -- ), 

    -- partial_full as (
    -- select distinct
    -- f.person_id 
    -- ,f.min_observation_period_yr as full_year_min
    -- ,f.max_observation_period_yr as full_year_max
    -- ,p.min_observation_period_yr as partial_year_min
    -- ,p.max_observation_period_yr as partial_year_max
    -- from full_year f
    -- LEFT join may_include_partial_yr p 
    -- on f.person_id = p.person_id and  f.min_observation_period_yr = p.min_observation_period_yr
    -- ), 

    -- one_min_max as (
    -- select distinct 
    -- m.person_id
    -- ,m.observation_start_date
    -- ,x.observation_end_date
    -- from(
    --     select 
    --     person_id
    --     --min of the partial or full year
    --     , case when partial_year_min < full_year_min then partial_year_min
    --     else full_year_min
    --     end as observation_start_date 
    --     from partial_full 
    -- ) m
    -- left join (
    --     select
    --     person_id
    --     , case when partial_year_max > full_year_max then partial_year_max
    --          when full_year_max > partial_year_max then full_year_max
    --     end as observation_end_date
    --     from partial_full 
    -- ) x
    -- on m.person_id = x.person_id
    -- ),
     --, to_date(COVSTART, 'ddMMMyyyy') as coverage_start
    --, PTD_PLAN_CVRG_MONS
    --, MDCR_STATUS_CODE_01, MDCR_STATUS_CODE_02, MDCR_STATUS_CODE_03, MDCR_STATUS_CODE_04, MDCR_STATUS_CODE_05, MDCR_STATUS_CODE_06
    --,  MDCR_STATUS_CODE_07, MDCR_STATUS_CODE_08, MDCR_STATUS_CODE_09, MDCR_STATUS_CODE_10, MDCR_STATUS_CODE_11, MDCR_STATUS_CODE_12
    with min_year as ( 
        SELECT distinct BID
        ,to_date( MIN (BENE_ENROLLMT_REF_YR ), 'yyyy'  )as min_observation_period_yr
       FROM `ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d`
        where BENE_ENROLLMT_REF_YR is not null 
        group by BID
    ), 
    max_year as (
        SELECT distinct BID
        ,to_date( MAX (BENE_ENROLLMT_REF_YR ), 'yyyy'  )as max_observation_period_yr
        FROM `ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d`
        where BENE_ENROLLMT_REF_YR is not null 
        group by BID
    ),
    obs_period as(
        SELECT distinct BID, min_observation_period_yr, max_observation_period_yr
        from 
        (
        SELECT a.BID, min_observation_period_yr, max_observation_period_yr
        FROM min_year a
        left join max_year b 
        on a.BID = b.BID
        )
    ),
    final_obs_period as (
    select distinct 
     cast( o.BID as long) as person_id
    , cast( min_observation_period_yr as date) as observation_period_start_date
    , cast( max_observation_period_yr as date) as  observation_period_end_date
    , CAST( 32813 as int) as period_type_concept_id 
    from obs_period o
    )
    
    --generate observation_period_id
     SELECT distinct 
      *
      , cast(conv(substr( obs_period_hashed_id, 1, 15), 16, 10) as bigint) as observation_period_id
      FROM 
      (
        SELECT DISTINCT 
        *, 
        md5(concat_ws(
              ';'
        , COALESCE(person_id, '')
        , COALESCE(observation_period_start_date, '')
        , COALESCE(observation_period_end_date, '')
        , COALESCE('cms', '')
        )) AS obs_period_hashed_id
        FROM final_obs_period
      )
   