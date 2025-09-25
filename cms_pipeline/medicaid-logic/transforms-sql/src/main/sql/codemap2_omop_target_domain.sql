CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/codemap2_omop_target_domain` AS
  
--ICD10CM
    with cms_codes as (
        SELECT DISTINCT 
        'IP' as source_domain,
        ip.src_code as cms_src_code, 
        -- missing the dot
        case when LENGTH(TRIM(ip.src_code)) > 3 and src_vocab_code = 'ICD10CM' THEN concat_ws( '.', SUBSTRING(ip.src_code, 1, 3),  SUBSTRING(src_code, 4, LENGTH(trim(src_code)) )) 
        ELSE ip.src_code 
        END as src_mapped_code, 
        ip.src_vocab_code,
        'ICD10CM' as mapped_code_system
        FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - melted/datasets/ip` ip

        UNION
        -- all 8 length pcs codes
        select DISTINCT 
        'LT' as source_domain,
        lt.src_code as cms_src_code,
         case when LENGTH(TRIM(lt.src_code)) > 3 and src_vocab_code = 'ICD10CM' THEN concat_ws( '.', SUBSTRING(lt.src_code, 1, 3),  SUBSTRING(src_code, 4, LENGTH(trim(src_code)) )) 
              else lt.src_code 
              end as src_mapped_code, 
        lt.src_vocab_code,
        'ICD10CM' as mapped_code_system
        from `ri.foundry.main.dataset.386bbb38-eb8a-49ad-aa49-75580d42ad79` lt
---ICD10PCS codes
        -- from ip
        UNION 
        SELECT DISTINCT
        'IP' as source_domain, 
        p.src_code as cms_src_code,
        mapped_src_vocab_cd as src_mapped_code, 
        cast(PRCDR_CD_SYS as string) as src_vocab_code, 
     'ICD10PCS' as mapped_code_system
        from `ri.foundry.main.dataset.2495c751-de3b-4743-8b93-2c1f903caba1` p
        where p.mapped_src_vocab_cd ='ICD10PCS' -- mapped_src_vocab_cd
        UNION
        -- from ot
        select DISTINCT 
        'OT' as source_domain,
        o.LINE_PRCDR_CD as cms_src_code,
        o.LINE_PRCDR_CD as src_mapped_code, 
        'ICD10PCS' as src_vocab_code, 
        'ICD10PCS' as mapped_code_system
        from `ri.foundry.main.dataset.8e757c3a-ba1e-4f73-8f27-202e5a2a449d` o
        where o.LINE_PRCDR_CD_SYS = '07' -- 07='ICD10PCS'

--hcpcs codes
 --PRCDR_CD_SYS=01=CPT 4
        -- PRCDR_CD_SYS=02 =ICD-9 CM
        -- PRCDR_CD_SYS=06 =HCPCS (Both National and Regional HCPCS)
        -- PRCDR_CD_SYS=07 =ICD-10 - CM PCS
        -- PRCDR_CD_SYS=10-87=Other Systems
        --hcpcs 5 digit hcpcs codes
        --hcpcs code are found in ip and ot, must check the code system column
        -- FROM IP
        UNION
        select distinct 
        'IP' as source_domain, 
        p.src_code as cms_src_code,
        mapped_src_vocab_cd as src_mapped_code, 
       cast(PRCDR_CD_SYS as string) as src_vocab_code,
       'HCPCS' as mapped_code_system
        from `ri.foundry.main.dataset.2495c751-de3b-4743-8b93-2c1f903caba1` p
        where p.mapped_src_vocab_cd ='HCPCS' -- mapped_src_vocab_cd

        UNION 
        -- from ot
        select DISTINCT 
        'OT' as source_domain,
        o.LINE_PRCDR_CD as cms_src_code,
        'HCPCS' as src_mapped_code, 
        LINE_PRCDR_CD_SYS as src_vocab_code,
        'HCPCS' as mapped_code_system
        from `ri.foundry.main.dataset.8e757c3a-ba1e-4f73-8f27-202e5a2a449d` o
        where o.LINE_PRCDR_CD_SYS = '06' or  o.LINE_PRCDR_CD_SYS is null  -- -- 06='HCPCS' or 
--- cpt4 
        UNION
        select DISTINCT 
        'IP' as source_domain, 
        p.src_code as cms_src_code,
        p.src_code as src_mapped_code, 
        'CPT4' as src_vocab_code,
        'CPT4' as mapped_code_system
        from `ri.foundry.main.dataset.b2348fe7-6c66-4aae-9347-10c3b51370c1` p
        where p.mapped_src_vocab_cd ='CPT4' -- mapped_src_vocab_cd
        -- FROM ot
        UNION
        select DISTINCT 
        'OT' as source_domain,
        o.src_code as cms_src_code,
        o.src_code as src_mapped_code, 
        'CPT4' as src_vocab_code, 
        'CPT4' as mapped_code_system
        from `ri.foundry.main.dataset.8e757c3a-ba1e-4f73-8f27-202e5a2a449d` o
        where o.LINE_PRCDR_CD_SYS = '01' -- 01='CPT4'

---NDC
        /*-- 1% sample-25.6K  */
        -- 11 digits ndc coces
        UNION
       SELECT DISTINCT 
        'RX' as source_domain,
        rx.src_code as cms_src_code,
        rx.src_code as src_mapped_code,
        rx.src_vocab_code as src_vocab_code, /*-- 1% sample-25.6K  */ -- 'NDC' as src_vocab_code
        -- 11 digits ndc coces
        'NDC' as mapped_code_system
        from `ri.foundry.main.dataset.a7ce3f8c-4347-4ec6-80be-3f17b3b92141` rx
        UNION
         SELECT DISTINCT 
        'IP' as source_domain, 
        ip.NDC as cms_src_code,
        ip.NDC as src_mapped_code,
        'NDC' as src_vocab_code, -- NDC are mapped to RxNorm
        'NDC' as mapped_code_system
        FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip

    ), 

    source_concept_mapping as (
        
        SELECT DISTINCT
        source_domain
        , cms.cms_src_code
        , cms.src_mapped_code
        , cms.src_vocab_code
        , cms.mapped_code_system
        -- Source data --> Source concept mapping:
        , c.concept_code                AS source_concept_code
        , COALESCE(c.concept_id, 0)     AS source_concept_id
        , c.concept_name                AS source_code_description
        , c.vocabulary_id               AS source_vocabulary_id
        , c.domain_id                   AS source_domain_id
        , c.concept_class_id            AS source_concept_class_id
        , c.valid_start_date            AS source_valid_start_date
        , c.valid_end_date              AS source_valid_end_date
        , c.invalid_reason              AS source_invalid_reason
        FROM cms_codes cms
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
            ON trim(c.concept_code) = trim(cms.src_mapped_code) 
            AND (upper(trim(c.vocabulary_id)) = upper(trim(cms.mapped_code_system)) 
                OR
                upper(trim(c.vocabulary_id)) in ('ICD10CM', 'ICD10PCS','CPT4', 'HCPCS', 'NDC', 'RxNorm', 'OMOP Genomic', 'RxNorm Extension', 'SNOMED', 'CVX', 'Cancer Modifier'))
            -- code and vocab_id
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
        where cms.src_mapped_code is not NULL ---add these checks later and c.invalid_reason not in ('D') ----and c.valid_end_date > CAST('2023-01-15' AS date) 

    ), 

    target_concept_mapping as (
         SELECT DISTINCT
         source_concept_mapping.*
        , COALESCE(c2.concept_id, 0)     AS target_concept_id
        , COALESCE(c2.concept_name, 'No matching concept') AS target_concept_name
        , c2.vocabulary_id               AS target_vocabulary_id
        , COALESCE( c2.domain_id, 'Observation')               AS target_domain_id
        , c2.concept_class_id            AS target_concept_class_id
        , c2.valid_start_date            AS target_valid_start_date
        , c2.valid_end_date              AS target_valid_end_date
        , c2.invalid_reason              AS target_invalid_reason
        FROM source_concept_mapping 
        LEFT JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr 
        ON source_concept_mapping.source_concept_id = cr.concept_id_1
        AND cr.invalid_reason IS NULL 
        AND lower(cr.relationship_id) = 'maps to'
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c2
        ON cr.concept_id_2 = c2.concept_id
        AND c2.invalid_reason IS NULL -- invalid records will map to concept_id = 0

    )

    select distinct * from target_concept_mapping