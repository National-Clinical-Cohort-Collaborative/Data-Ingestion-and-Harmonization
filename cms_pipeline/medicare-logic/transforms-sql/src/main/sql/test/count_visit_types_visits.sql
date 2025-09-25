CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/logic/test/count_visit_types_visits` AS

with sn_prelim as (
      SELECT DISTINCT
          BID
       -- , CLAIM_ID
        --, to_date(ADMSN_DT, 'ddMMMyyyy') as ADMSN_DT
        , to_date(FROM_DT, 'ddMMMyyyy') as FROM_DT
        , PROVIDER
--        , collect_set(RVCNTR)
        , to_date(THRU_DT, 'ddMMMyyyy') as THRU_DT 
        --, ORGNPINM as care_site_npi
        , case when size(array_intersect( collect_set(RVCNTR), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0  
               then 9203 --er
               else 42898160 -- nhiv
            END as visit_concept_id
         -- https://resdac.org/sites/datadocumentation.resdac.org/files/Revenue%20Center%20Code%20Code%20Table%20FFS.txt
      FROM `ri.foundry.main.dataset.79489370-9e8d-4743-b0b6-da040f1f2f02` sn_long
      WHERE PMT_AMT >= 0  -- PMT_AMT must > 0 to complete the stay 
      --group by BID, ADMSN_DT, PROVIDER, THRU_DT
      group by BID, FROM_DT, PROVIDER, THRU_DT
),
sn_CTE as (
        --select BID, ADMSN_DT, PROVIDER, THRU_DT, visit_concept_id, -- RVCNTR,
        select BID, FROM_DT, PROVIDER, THRU_DT, visit_concept_id, -- RVCNTR,
             --RANK() OVER (PARTITION BY BID, PROVIDER, ADMSN_DT, THRU_DT ORDER BY visit_concept_id ASC)  as idx
             RANK() OVER (PARTITION BY BID, PROVIDER, FROM_DT, THRU_DT ORDER BY visit_concept_id ASC)  as idx
        from sn_prelim
), 
OMOP as (
select "OMOP" as title, count(distinct visit_occurrence_id) as omop_ct, visit_concept_id, source_domain 
from  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06_5 - add concept names/visit_occurrence` vo 
group by visit_concept_id, source_domain
), 
CMS as (
      select "CMS" as title , count(distinct BID, ADMSN_DT, PROVIDER) as cms_ct, 262 as concept_id, "IP" as source_domain
      from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/ip` ip
                 join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = ip.BID
            where PMT_AMT > 0 and TYPE_ADM in ('1', '5')
      union
      select "CMS" as title, count(distinct BID,  ADMSN_DT, PROVIDER) as cms_ct, 9201 as concept_id, "IP" as source_domain
            from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/ip` ip
            join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
              on cast(sp.cms_person_id as string) = ip.BID
            where PMT_AMT > 0 and TYPE_ADM not in ('1', '5')
      union 

      select "CMS" as title, count(distinct BID, REV_DT, PROVIDER) as cms_ct, 9202 as concept_id, "OPL" as source_domain
      FROM `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a`
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = BID
      WHERE REV_CNTR NOT IN ('0450', '0451', '0452', '0456', '0459', '0981') 
            AND REVPMT > 0 AND REV_DT is not NULL
      union

      select "CMS" as title, count(distinct BID, REV_DT, PROVIDER) as cms_ct, 9203 as concept_id, "OPL"  as source_domain
      FROM `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a`
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = BID
      WHERE REV_CNTR IN ('0450', '0451', '0452', '0456', '0459', '0981') 
            AND REVPMT > 0 AND REV_DT is not NULL
      union

      select "CMS", count(distinct BID, TAX_NUM, EXPNSDT1) as cms_ct, cpt.concept_id as concept_id, 'DM' as source_domain
      FROM `ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0` dm
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = dm.BID
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
      ON dm.PLCSRVC = vxwalk.source_concept_code
      join `/N3C Export Area/OMOP Vocabularies/concept` cpt on cpt.concept_code = vxwalk.target_concept_code and cpt.vocabulary_id = 'CMS Place of Service'
      GROUP BY concept_id
      UNION

      select "CMS", count(distinct BID, BLGNPI, EXPNSDT1) as cms_ct, cpt.concept_id as concept_id, 'PB' as source_domain
      FROM `ri.foundry.main.dataset.b1477e1c-43ee-4a06-8090-6ab2f605d18f` pb
            join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = pb.BID
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
      ON pb.PLCSRVC = vxwalk.source_concept_code
          join `/N3C Export Area/OMOP Vocabularies/concept` cpt on cpt.concept_code = vxwalk.target_concept_code and cpt.vocabulary_id = 'CMS Place of Service'
      GROUP BY concept_id
      union

      select "CMS" as title, count(distinct BID, PROVIDER, FROM_DT) as cms_ct, 581476 as concept_id, "HH"  as source_domain
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/hh` hh
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = hh.BID
      WHERE PMT_AMT > 0
      union

      select "CMS" as title, count(distinct BID, PRESCRIBER_ID, RX_DOS_DT) as cms_ct, 581458 as concept_id, "PDE" as source_domain
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/pde` pde
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = pde.BID
      WHERE RX_DOS_DT is not null and DAYS_SUPPLY is not NULL and FILL_NUM is not NULL
      union

      --SELECT 'CMS from CTE  thru-date' as title, count(distinct BID, PROVIDER, ADMSN_DT, THRU_DT) as cms_ct, visit_concept_id as concept_id, 'SN' as source_domain
      SELECT 'CMS from CTE  thru-date' as title, count(distinct BID, PROVIDER, FROM_DT, THRU_DT) as cms_ct, visit_concept_id as concept_id, 'SN' as source_domain
      FROM sn_CTE where idx=1
      GROUP BY visit_concept_id
      union

      select "CMS" as title, count(distinct BID, PROVIDER, FROM_DT) as cms_ct, 8546 as concept_id, "HS"  as source_domain
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/hs` hs
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = hs.BID
      WHERE PMT_AMT > 0 and FROM_DT is not NULL

)

select c.cms_ct, o.omop_ct, (c.cms_ct / o.omop_ct) as frac,
   cpt.concept_name, c.concept_id as cms_concept_id, o.visit_concept_id as omop_concept_id, c.source_domain
from OMOP o
FULL OUTER JOIN CMS c on o.visit_concept_id = c.concept_id and o.source_domain = c.source_domain
join `/N3C Export Area/OMOP Vocabularies/concept` cpt on cpt.concept_id = c.concept_id
   