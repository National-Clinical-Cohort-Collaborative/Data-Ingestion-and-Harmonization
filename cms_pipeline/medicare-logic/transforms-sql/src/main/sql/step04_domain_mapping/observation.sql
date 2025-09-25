 CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/observation` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, EXECUTOR_CORES_EXTRA_LARGE, DRIVER_MEMORY_EXTRA_EXTRA_LARGE') AS

    --condition should be sourced from the following source files:   opl, ip, pb, sn, hh, dm, hs   
    --
    with ipdx AS (
    SELECT DISTINCT
        BID
--      , CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(DSCHRGDT, 'ddMMMyyyy') as observation_date
      , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , 'IP' as source_domain
    FROM `ri.foundry.main.dataset.87d51e8c-0893-46d0-a2b4-f3f73db44cf6` ip
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON ip.DX = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Observation'
    where  ip.DX is not null and DSCHRGDT is not null ----and cxwalk.src_code_type = 'dx'
    ),
    opldx as (
     SELECT DISTINCT     
        op.BID
      --, op.CLAIM_ID
      , op.PROVIDER as care_site_id
      , to_date(op.REV_DT, 'ddMMMyyyy') as observation_date
      , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , 'OPL' as source_domain
    FROM `ri.foundry.main.dataset.4925b4a7-9b22-41e6-b33c-707a09d0d81e` op
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON op.DX = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Observation'
    where op.DX is not NULL and REV_DT is not null 
    ), 
    hhdx as (
    SELECT DISTINCT
        BID
      --, CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(THRU_DT, 'ddMMMyyyy') as observation_date
      , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , 'HH' as source_domain
    FROM `ri.foundry.main.dataset.8ae606e0-96dc-4b39-ae61-f3fa9f5fd947` hh
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON hh.DX = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Observation'
    where hh.DX is not null and THRU_DT is not null 
    ),
    pbdx as (
      SELECT DISTINCT
        BID
      --, CLAIM_ID
      , BLGNPI as care_site_id
      , to_date(EXPNSDT2 , 'ddMMMyyyy') as observation_date
      , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , vxwalk.target_concept_id as visit_concept_id
      , 'PB' as source_domain
    FROM `ri.foundry.main.dataset.84a03b0f-f74a-404f-a839-9f1457f4379c` pb
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON pb.DX = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Observation'
    LEFT JOIN  `ri.foundry.main.dataset.bea9f90c-f90a-4c25-a5f6-8a9d7c87fe4b` vxwalk on 
    vxwalk.source_concept_code = pb.PLCSRVC  
    where pb.DX is not null and EXPNSDT2 is not null 
    ),
    sndx as (
      SELECT DISTINCT
        BID
      --, CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(THRU_DT , 'ddMMMyyyy') as observation_date
      , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , 'SN' as source_domain
    FROM `ri.foundry.main.dataset.77e379a1-61af-4303-825a-cbc9e4c9cb9f` sn
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON sn.DX = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Observation'
    where sn.DX is not null and sn.THRU_DT is not null
    ), 
    snhipps as ( -- SN HIPPS from HCPSCD
      SELECT DISTINCT
        BID
      --, CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(THRU_DT , 'ddMMMyyyy') as observation_date
      , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , 'SN' as source_domain
    FROM `ri.foundry.main.dataset.79489370-9e8d-4743-b0b6-da040f1f2f02` snl
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
      ON snl.HCPSCD = cxwalk.cms_src_code  
        and cxwalk.target_domain_id = 'Observation'
        and cxwalk.src_vocab_code = 'HIPPS' -- HIPPS because they will only be source-side, not mapped to OMOP concept.
    WHERE snl.HCPSCD is not null and snl.THRU_DT is not null
    ), 
    snobs as ( -- SN other from HCPSCD
      SELECT DISTINCT
        BID
      --, CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(THRU_DT , 'ddMMMyyyy') as observation_date
      , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , 'SN' as source_domain
    FROM `ri.foundry.main.dataset.79489370-9e8d-4743-b0b6-da040f1f2f02` snl
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
      ON snl.HCPSCD = cxwalk.cms_src_code  
        and cxwalk.target_domain_id = 'Observation'
        and cxwalk.src_vocab_code != 'HIPPS' -- NOT HIPPS
    where snl.HCPSCD is not null and snl.THRU_DT is not null
    ), 
    dmdx as (
      SELECT DISTINCT
        BID
     -- , CLAIM_ID
      , TAX_NUM_ID as care_site_id
      , to_date(EXPNSDT1 , 'ddMMMyyyy') as observation_date
      , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , vxwalk.target_concept_id as visit_concept_id
      , 'DM' as source_domain
    FROM `ri.foundry.main.dataset.fc107626-b36b-4e0b-b036-dcf2f548d73d` dm
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON dm.DX = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Observation'
    LEFT JOIN  `ri.foundry.main.dataset.bea9f90c-f90a-4c25-a5f6-8a9d7c87fe4b` vxwalk 
    on vxwalk.source_concept_code = dm.PLCSRVC 
    where dm.DX is not null and dm.EXPNSDT1 is not null 
    ),
    hsdx as (
      SELECT DISTINCT
        BID
     -- , CLAIM_ID
      , PROVIDER as care_site_id
      , to_date(THRU_DT , 'ddMMMyyyy') as observation_date
        , cxwalk.target_concept_id as observation_concept_id
      , cxwalk.source_concept_id as observation_source_concept_id
      , cxwalk.source_concept_code as observation_source_value
      , 'HS' as source_domain
    FROM `ri.foundry.main.dataset.8f878ba2-acae-41d2-b5cc-bf15e26f5861` hs
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxwalk
    ON hs.DX = cxwalk.cms_src_code  and cxwalk.target_domain_id = 'Observation'
    where hs.DX is not null and THRU_DT is not null 
    ),
    -------visit link use visit end date to link with observation date which uses the thru date -----------
    ipdx_visit as (
      SELECT DISTINCT
      ipdx.BID
    , ipdx.care_site_id
    , ipdx.observation_date
    , ipdx.observation_concept_id
    , ipdx.observation_source_concept_id
    , ipdx.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , null as visit_detail_id
    , ipdx.source_domain
    from ipdx
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
      on CAST(ipdx.BID AS long) = v.person_id
      and ipdx.observation_date = v.visit_end_date
      and ipdx.care_site_id = v.care_site_id 
      and v.source_domain = 'IP'
      and v.visit_concept_id = 9201 
    ), 
    iperdx_visit as (
      SELECT DISTINCT
      ipdx.BID
    , ipdx.care_site_id
    , ipdx.observation_date
    , ipdx.observation_concept_id
    , ipdx.observation_source_concept_id
    , ipdx.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , null as visit_detail_id
    , ipdx.source_domain
    from ipdx
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
      on CAST(ipdx.BID AS long) = v.person_id
      and ipdx.observation_date = v.visit_end_date
      and ipdx.care_site_id = v.care_site_id 
      and v.source_domain = 'IPER'
      and v.visit_concept_id = 9203
    ), 
    operdx_visit as (
      SELECT DISTINCT
      d.BID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , null as visit_detail_id
    , 'OPLER' as source_domain
    from opldx d
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
      on CAST(d.BID as long) = v.person_id
      and d.observation_date = v.visit_end_date
      and d.care_site_id = v.care_site_id 
      and v.source_domain = 'OPLER'
      where v.visit_concept_id = 9203
    ),
    opdx_visit as (
      SELECT DISTINCT
      d.BID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , null as visit_detail_id
    , 'OPL' as source_domain
    from opldx d
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
      on CAST(d.BID as long) = v.person_id
      and d.observation_date = v.visit_end_date
      and d.care_site_id = v.care_site_id 
      and v.source_domain = 'OPL'
      where v.visit_concept_id = 9202
    ),
    hhdx_visit as (
    SELECT DISTINCT
      d.BID
    --, d.CLAIM_ID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , null as visit_detail_id
    , d.source_domain
    from hhdx d
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
      on CAST(d.BID as long) = v.person_id
      and d.observation_date = v.visit_end_date
      and d.care_site_id = v.care_site_id 
      and v.visit_concept_id = 581476
      and v.source_domain = 'HH'
    ),
    pbdx_visit as (
      SELECT DISTINCT
      d.BID
    --, d.CLAIM_ID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , null as visit_detail_id
    , d.source_domain
    from pbdx d
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
      on CAST(d.BID as long) = v.person_id
      and d.observation_date = v.visit_end_date
      and d.care_site_id = v.care_site_id 
      and d.visit_concept_id = v.visit_concept_id
      and v.source_domain = 'PB'
    ),
    sndx_visit as (
      SELECT DISTINCT
      d.BID
    --, d.CLAIM_ID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , vd.visit_occurrence_id 
    , vd.visit_detail_concept_id as visit_concept_id
    , vd.visit_detail_id
    , vd.source_domain
    from sndx d
     JOIN `ri.foundry.main.dataset.57c52f98-102f-487a-94c6-9d2c792a8fcb` vd
      on CAST(d.BID as long) = vd.person_id
      and d.observation_date = vd.visit_detail_end_date
      and d.care_site_id = vd.care_site_id 
      and vd.visit_detail_concept_id in (42898160, 9203)
       and vd.source_domain = 'SN'
    ),
    snhipps_visit as (
      SELECT DISTINCT
      d.BID
    --, d.CLAIM_ID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , vd.visit_occurrence_id 
    , vd.visit_detail_concept_id as visit_concept_id
    , vd.visit_detail_id
    , vd.source_domain

    from snhipps d
  JOIN `ri.foundry.main.dataset.57c52f98-102f-487a-94c6-9d2c792a8fcb` vd
      on CAST(d.BID as long) = vd.person_id
      and d.observation_date = vd.visit_detail_end_date
      and d.care_site_id = vd.care_site_id 
      and vd.visit_detail_concept_id in (42898160, 9203)
      and vd.source_domain = 'SN'
    ),
    snobs_visit as (
      SELECT DISTINCT
     d.BID
    --, d.CLAIM_ID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , vd.visit_occurrence_id 
    , vd.visit_detail_concept_id as visit_concept_id
    , vd.visit_detail_id
    , vd.source_domain
    from snobs d
JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_detail` vd
      on CAST(d.BID as long) = vd.person_id
      and d.observation_date = vd.visit_detail_end_date
      and d.care_site_id = vd.care_site_id 
      and vd.visit_detail_concept_id in (42898160, 9203)
      and vd.source_domain = 'SN'
    ),
    dmdx_visit as (
    SELECT DISTINCT
      d.BID
    --, d.CLAIM_ID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , null as visit_detail_id
    , d.source_domain
    from dmdx d
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
      on CAST(d.BID as long) = v.person_id
      and d.observation_date = v.visit_start_date
      and d.observation_date = v.visit_end_date
      and d.care_site_id = v.care_site_id 
      and d.visit_concept_id = v.visit_concept_id
      and v.source_domain = 'DM'
    ),
    hsdx_visit as (
    SELECT DISTINCT
      d.BID
  --  , d.CLAIM_ID
    , d.care_site_id
    , d.observation_date
    , d.observation_concept_id
    , d.observation_source_concept_id
    , d.observation_source_value
    , v.visit_occurrence_id 
    , v.visit_concept_id
    , null as visit_detail_id
    , d.source_domain
    from hsdx d
    JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
      on CAST(d.BID as long) = v.person_id
      and d.observation_date = v.visit_end_date
      and d.care_site_id = v.care_site_id 
      and v.visit_concept_id = 8546
      and v.source_domain = 'HS'
    ),

    final as (
    select distinct * from ipdx_visit
    union
    select distinct * from iperdx_visit
    union
    select distinct * from opdx_visit
    union 
    select distinct * from operdx_visit
    union
    select distinct * from hhdx_visit
    union 
    select distinct * from pbdx_visit
    union
    select distinct * from sndx_visit
    union
    select distinct * from snhipps_visit
    union
    select distinct * from snobs_visit
    union
    select distinct * from dmdx_visit
    union
    select distinct * from hsdx_visit
    ),

    cms_observation as (
    select distinct
     md5(concat_ws(
              ';'
        , COALESCE(f.BID, '')
        , COALESCE(f.care_site_id, '')
        , COALESCE(f.observation_date, '')
        , COALESCE(f.observation_concept_id, '')
        , COALESCE(f.observation_source_concept_id, '')
        , COALESCE(f.observation_source_value, '')
        , COALESCE(f.source_domain, '')
        , COALESCE(visit_occurrence_id, '')
        , COALESCE(visit_detail_id, '') -- we don't have procedure_end_date to distinguish nearly identical rows, so use this.
        , COALESCE('cms', '')
        )) as cms_hashed_observation_id
        ---, COALESCE(co.source_domain, ''), co.visit_concept_id ) as observation_id -- should also add the visit_concept_id----------TODO
        , CAST(f.BID as long) AS person_id
            , observation_concept_id
            , observation_date
            , cast( observation_date as timestamp ) as observation_datetime
            , 32810 as observation_type_concept_id
            , cast( null as float ) as value_as_number
            , cast( null as string ) as value_as_string
            , cast( null as int) as value_as_concept_id
            , cast( null as int) as qualifier_concept_id
            , cast( null as int) as unit_concept_id
            , cast( null as long) as provider_id
            , cast( f.care_site_id as long) as care_site_id
            , visit_occurrence_id 
            , cast(visit_detail_id as bigint) as visit_detail_id -- CR 8/23/2023 (was long)
            , observation_source_value
            , observation_source_concept_id
            , cast( null as string) as unit_source_value
            , cast( null as string) as qualifier_source_value
            , f.source_domain
  from final f
  where observation_concept_id is not null
  --left join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/person` p
  --on f.BID = p.person_id
  -- match visit on bid, provider_id, visit start_date and end_date, visit_concept_id( that are all unique based on the source domain, 32810= visit_concept_id is used for claims data.  
    )
    SELECT
      *
    --, cast(conv(substr(cms_hashed_observation_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id
    , cast(conv(substr(cms_hashed_observation_id, 1, 15), 16, 10) as bigint) as observation_id
    FROM cms_observation

 
    
    