CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/medicare_codemap_xwalk_ut` AS
----CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` AS

 with cms_codes as (
        SELECT DISTINCT 
        'IP' as source_domain,  
        i.src_code as cms_src_code, 
        -- missing the dot
        case when LENGTH(TRIM(i.src_code)) > 3 and src_vocab_code = 'ICD10CM' THEN concat_ws( '.', SUBSTRING(i.src_code, 1, 3),  SUBSTRING(src_code, 4, LENGTH(trim(src_code)) )) 
        ELSE i.src_code 
        END as src_mapped_code, 
        i.src_vocab_code
        FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/datasets/ip` i

        UNION
        -- all 8 length pcs codes
        select DISTINCT 
        'DM' as source_domain,
        d.src_code as cms_src_code,
        d.src_code as src_mapped_code, 
        d.src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/datasets/dm` d

        union 
        --hcpcs 5 digit hcpcs codes
        select DISTINCT 
        'HH' as source_domain,
        h.src_code as cms_src_code,
        h.src_code as src_mapped_code,
        h.src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/datasets/hh` h

        UNION 
        SELECT DISTINCT 
        'HS' as source_domain,
        hs.src_code as cms_src_code, 
        hs.src_code as src_mapped_code,
        hs.src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/datasets/hs` hs
       
        UNION 
        SELECT DISTINCT 
        'OPL' as source_domain, 
        opl.src_code as cms_src_code, 
        opl.src_code as src_mapped_code,
        opl.src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/datasets/opl` opl

        UNION
        SELECT DISTINCT 
        'PB' as source_domain, 
        pb.src_code as cms_src_code, 
        pb.src_code as src_mapped_code,
        pb.src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/datasets/pb` pb

        UNION
        SELECT DISTINCT 
        'SN' as source_domain, 
        sn.src_code as cms_src_code, 
        sn.src_code as src_mapped_code,
        sn.src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/datasets/sn` sn

    ), 

    source_concept_mapping as (
        
        SELECT DISTINCT
        cms.source_domain
        , cms.cms_src_code
        , cms.src_mapped_code
        , cms.src_vocab_code
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
            AND (upper(trim(c.vocabulary_id)) = upper(trim(cms.src_vocab_code)) 
                OR
                upper(trim(c.vocabulary_id)) in ('ICD10CM', 'ICD10PCS','CPT4', 'HCPCS', 'NDC', 'RxNorm', 'OMOP Genomic', 'RxNorm Extension', 'SNOMED', 'CVX', 'Cancer Modifier','Visit'))
            -- code and vocab_id
            AND c.concept_class_id != 'ICD10PCS Hierarchy' -- codes overlap with ICD10CM
        where cms.src_mapped_code is not NULL

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