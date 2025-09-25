CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/ip_visit` AS
    WITH ip as ( 
      SELECT 
          BID
        , TYPE_ADM
        , CLAIM_ID
        , to_date(ADMSN_DT, 'ddMMMyyyy') as start_date
        , PROVIDER
        , to_date(DSCHRGDT, 'ddMMMyyyy') as end_date
        , RANK() OVER (PARTITION BY BID, ADMSN_DT, PROVIDER ORDER BY THRU_DT DESC) as idx
      FROM `ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6`
    ),
    -- visit_concept_id
    -- choose idx= 1, you might have multiple claims with same admission date, so choose the latest THRU_DT
    ip_visit as (
       SELECT 
        'IP' as source_domain
      , ip.BID
      , ip.CLAIM_ID
      , ip.PROVIDER
      , ip.start_date 
      , ip.end_date 
      , ipv.visit_concept_id
      --, 44818517 as visit_type_concept_id /* (Visit derived from encounter on claim) */ non-standard
      , 32021 as visit_type_concept_id 
      , cast(ipv.visit_concept_id as string) as visit_source_value
    FROM ip
    INNER JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/ip_visits` ipv
    on ip.BID = ipv.BID and ip.CLAIM_ID = ipv.CLAIM_ID
    where ip.idx = 1
   )

    SELECT DISTINCT
      'IP' as source_domain
      , ip.BID 
      , ip.CLAIM_ID
      , ip.PROVIDER as provider_id
      , start_date as visit_start_date
      , end_date as visit_end_date
      , v.visit_occurrence_id
      , ip.visit_concept_id as visit_concept_id
      --, 44818517 as visit_type_concept_id /* (Visit derived from encounter on claim) */ non-standard
      , 32021 as visit_type_concept_id 
      , cast(v.visit_concept_id as string) as visit_source_value
    FROM ip_visit ip
    INNER JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` v
    ON ip.BID = v.person_id
    AND ip.start_date = v.visit_start_date
    and ip.PROVIDER = v.provider_id
    AND (v.visit_concept_id = 9201 OR v.visit_concept_id = 262)
    AND v.source_domain = 'IP'

