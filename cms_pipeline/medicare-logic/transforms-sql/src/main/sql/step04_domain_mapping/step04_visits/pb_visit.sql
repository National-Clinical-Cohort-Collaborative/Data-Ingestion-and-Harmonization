CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/04 - visits/pb_visit` AS
--pb visit for visit_occurrence
--
with pb as (--64.1m
      SELECT DISTINCT
        BID
        , CLAIM_ID
        , SGMT_NUM
        , BLGNPI as PROVIDER
      , to_date(EXPNSDT1 , 'ddMMMyyyy') as start_date
      , to_date(EXPNSDT2 , 'ddMMMyyyy') as end_date
      , PLCSRVC
      --, PRFNPI as performing_npi
      , RANK() OVER (PARTITION BY BID, EXPNSDT1, BLGNPI, PLCSRVC ORDER BY EXPNSDT2 DESC) as idx
    FROM `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
    WHERE PMT_AMT >= 0 
    ),

    pb_visit as ( ---65.5m-- distinct => 57.1m
       SELECT DISTINCT 
        'PB' as source_domain
      , pb.BID
      --, pb.CLAIM_ID --same claim no are used with different start and end date, so we can ignore the claim no for encounter based on dates
      , pb.PROVIDER
      , pb.start_date as visit_start_date
      , pb.end_date as visit_end_date
      --, 44818517 as visit_type_concept_id /* (Visit derived from encounter on claim) */ non-standard 44818517
      , 32021 as visit_type_concept_id 
      , cast(vxw.target_concept_id as string) as visit_concept_id
      , CAST( vxw.source_concept_code as string) as visit_source_value
    FROM pb
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxw ON pb.PLCSRVC = vxw.source_concept_code
    where pb.idx= 1 and start_date is not null and end_date is not null and PLCSRVC is not null ---and vxw.target_concept_code <> 'OMOP5117446'
    order by BID, PROVIDER
    )
    select * from pb_visit