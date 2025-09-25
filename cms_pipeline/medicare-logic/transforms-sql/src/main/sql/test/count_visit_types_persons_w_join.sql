CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/logic/test/count_visit_types_persons_w_join` AS

-- tricky b/c you can't compare CMS sources with visit types. Some sources route to many visit types, so you have to query, split and combine.
-- It's a many-to-many mapping.
-- Ex. SN goes to 9203 and 42898160 (NHIV) and DM and PB also populate NHIV. The crosswalk does this routing and must be accounted for.

/***
 select "OMOP 4-p" as title, count(distinct vo.person_id) as p_ct, visit_concept_id,  concept_name, source_domain
      from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` vo      
      left join `/N3C Export Area/OMOP Vocabularies/concept` c on c.concept_id = vo.visit_concept_id -- note left join in case of mistyped ids XXXXXXXXXXX
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.person_id = vo.person_id
    group by visit_concept_id, concept_name, source_domain
union
***/ 
      select "OMOP 4" as title, count(distinct vo.person_id) as p_ct, visit_concept_id,  concept_name, source_domain
      from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence` vo      
      left join `/N3C Export Area/OMOP Vocabularies/concept` c on c.concept_id = vo.visit_concept_id -- note left join in case of mistyped ids XXXXXXXXXXX
      group by visit_concept_id, concept_name, source_domain 

union select "CMS" as title , count(distinct BID) as p_ct, 262, "ER-IP" as concept_name,'ip' 
      from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/ip` ip
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = ip.BID
      where TYPE_ADM in ('1', '5') and PMT_AMT >= 0
union select "CMS" as title, count(distinct BID) as p_ct, 9201, "IP" as concept_name,'ip' 
      from  `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/ip` ip
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = ip.BID
      where TYPE_ADM not in ('1', '5') and PMT_AMT >= 0
union select "CMS" as title, count(distinct BID) as p_ct, 9202, "OP" as concept_name, 'opl' 
      FROM `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = opl.BID
      WHERE (REV_CNTR is NULL 
            OR REV_CNTR is not NULL and REV_CNTR NOT IN ('0450', '0451', '0452', '0456', '0459', '0981') )
      AND REVPMT >= 0
union select "CMS" as title, count(distinct BID) as p_ct, 9203, "ER" as concept_name, 'opl' 
      FROM `ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a` opl
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = opl.BID
      WHERE REV_CNTR IN ('0450', '0451', '0452', '0456', '0459', '0981') AND REVPMT >= 0 

union select "CMS" as title, count(distinct BID) as p_ct, 581476, "Home Visit" as concept_name, 'hh'
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/hh` hh
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = hh.BID
      WHERE PMT_AMT >= 0

      union select "CMS" as title, count(distinct BID) as p_ct, 581458, "Pharmacy visit" as concept_name, 'pde'
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/pde` pde
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = pde.BID
      WHERE RX_DOS_DT is not null and DAYS_SUPPLY is not NULL and FILL_NUM is not NULL

      -- use sn_hcpcs_long to get to REVCNTR more easily to find the split between 9203 and 42898160
      union select "CMS" as title, count(distinct BID) as p_ct, 9203, "ER" as concept_name, 'sn'
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/sn_hcpscd_long` snl
-- no person join            
      WHERE PMT_AMT >= 0 AND 
           snl.RVCNTR in ('0450', '0451', '0452', '0456', '0459', '0981')

      union select "CMS" as title, count(distinct BID) as p_ct, 42898160, "Non-hospital institution Visit" as concept_name, 'sn'
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/sn_hcpscd_long` snl
      WHERE PMT_AMT >= 0  AND (snl.RVCNTR is null OR (snl.RVCNTR is not Null AND snl.RVCNTR not in  ('0450', '0451', '0452', '0456', '0459', '0981')  ) ) 
-- no person join      

      union select "CMS" as title, count(distinct BID) as p_ct, 8546, "Hospice" as concept_name, 'hs'
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/hs` hs
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on cast(sp.cms_person_id as string) = hs.BID
      WHERE PMT_AMT >= 0 and FROM_DT is not NULL

-- DM  
      union select "CMS-dm" as title, count(distinct BID) as p_ct, 5083, "Telehealth"  as concept_name, 'dm' -- telehealth
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/dm` dm
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
        ON dm.PLCSRVC = vxwalk.source_concept_code
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.cms_person_id = cast(dm.BID as long)
      WHERE -- dm.PLCSRVC in (581399, 5083) and
        dm.EXPNSDT2 IS NOT NULL and dm.PLCSRVC is not null
        AND PMT_AMT >= 0 
        AND vxwalk.target_concept_id = 5083 -- and the crosswalk routes here

      union select "CMS-dm" as title, count(distinct BID) as p_ct, 581478, "Ambulance Visit"  as concept_name, 'dm' -- Ambulance Visit
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/dm` dm
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
        ON dm.PLCSRVC = vxwalk.source_concept_code
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.cms_person_id = cast(dm.BID as long)
      WHERE -- dm.PLCSRVC in (581475, 38003619) and
        dm.EXPNSDT2 IS NOT NULL and dm.PLCSRVC is not null
        AND PMT_AMT >= 0  
        AND vxwalk.target_concept_id =  581478 -- and the crosswalk routes here

      union select "CMS-dm" as title, count(distinct BID) as p_ct, 32036, "Laboratory Visit" as concept_name, 'dm' -- Lab Visit
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/dm` dm 
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
        ON dm.PLCSRVC = vxwalk.source_concept_code
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.cms_person_id  = cast(dm.BID as long)
      WHERE -- dm.PLCSRVC = 8809 and
        dm.EXPNSDT2 IS NOT NULL and dm.PLCSRVC is not null
        AND PMT_AMT >= 0
        AND vxwalk.target_concept_id = 32036 -- and the crosswalk routes here

      union select "CMS-dm" as title, count(distinct BID) as p_ct, 42898160, "Non-Hospital Institutional Visit" as concept_name, 'dm' -- NHIV
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/dm` dm 
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
        ON dm.PLCSRVC = vxwalk.source_concept_code
    join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.cms_person_id  = cast(dm.BID as long)
      WHERE -- dm.PLCSRVC = 8809 and
        dm.EXPNSDT2 IS NOT NULL and dm.PLCSRVC is not null
        AND PMT_AMT >= 0
        AND vxwalk.target_concept_id = 42898160 -- and the crosswalk routes to 42898160

 -- PB    
      union select "CMS-pb" as title, count(distinct BID) as p_ct, 5083, "Telehealth"  as concept_name, 'pb' -- telehealth
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/pb` pb
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
        ON pb.PLCSRVC = vxwalk.source_concept_code
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.cms_person_id =  cast(pb.BID as long)
      WHERE ---- pb.PLCSRVC in (581399, 5083) and
        pb.EXPNSDT2 IS NOT NULL and pb.PLCSRVC is not null
        AND PMT_AMT >= 0 
        AND vxwalk.target_concept_id = 5083 -- and the crosswalk routes here

      union select "CMS-pb" as title, count(distinct BID) as p_ct, 581478, "Ambulance Visit"  as concept_name, 'pb'  -- Ambulance Visit
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/pb` pb
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
        ON pb.PLCSRVC = vxwalk.source_concept_code
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.cms_person_id = cast(pb.BID as long)
      WHERE ------ pb.PLCSRVC in (581475, 38003619) and
        pb.EXPNSDT2 IS NOT NULL and pb.PLCSRVC is not null
        AND PMT_AMT >= 0 
        AND vxwalk.target_concept_id = 581478 -- and the crosswalk routes hereÜ*äöp

      union select "CMS-pb" as title, count(distinct BID) as p_ct, 32036, "Laboratory Visit"  as concept_name, 'pb'  -- Lab Visit
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/pb` pb
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
        ON pb.PLCSRVC = vxwalk.source_concept_code
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.cms_person_id = cast( pb.BID as long) 
      WHERE ------ pb.PLCSRVC = 8809 and
        pb.EXPNSDT2 IS NOT NULL and pb.PLCSRVC is not null
        AND PMT_AMT >= 0  
        AND vxwalk.target_concept_id = 32036 -- and the crosswalk routes here

      union select "CMS-pb" as title, count(distinct BID) as p_ct, 42898160, "Non-Hospital Instituional Visit"  as concept_name, 'pb'  -- HIV
      FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/pb` pb
      LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` vxwalk
        ON pb.PLCSRVC = vxwalk.source_concept_code
      join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/05 - safe/person` sp
        on sp.cms_person_id = cast( pb.BID as long) 
      WHERE ------ pb.PLCSRVC = 8809 and
        pb.EXPNSDT2 IS NOT NULL and pb.PLCSRVC is not null
        AND PMT_AMT >= 0     
        AND vxwalk.target_concept_id = 42898160 -- and the crosswalk routes to 42898160

order by visit_concept_id, title
 