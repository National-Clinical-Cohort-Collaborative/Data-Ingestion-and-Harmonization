CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms_code_xwalk` AS

 with cms_codes as (
        SELECT DISTINCT 
        d.src_code as cms_src_code, 
        -- missing the dot
        case when LENGTH(TRIM(d.src_code)) > 3 and src_vocab_code = 'ICD10CM' THEN concat_ws( '.', SUBSTRING(d.src_code, 1, 3),  SUBSTRING(src_code, 4, LENGTH(trim(src_code)) )) 
        ELSE d.src_code 
        END as src_mapped_code, 
        d.src_vocab_code
        FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/groups/dx` d

        UNION
        -- all 8 length pcs codes
        select DISTINCT 
        p.src_code as cms_src_code,
        p.src_code as src_mapped_code, 
        p.src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/groups/px` p

        union 
        --hcpcs 5 digit hcpcs codes
        select DISTINCT 
        h.src_code as cms_src_code,
        h.src_code as src_mapped_code,
        h.src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - melted/groups/hcpc` h

        --- TODO: we are missing the hcpcs code from ip - once added we can remove it from here S.Hong-3/10/23 
        UNION 
        SELECT DISTINCT 
        iph.HCPSCD as cms_src_code, 
        iph.HCPSCD as src_mapped_code,
        'HCPCS' as src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/ip_hcpscd_long` iph
        ---------WE CAN REMOVE ONCE THE IP HCPCS CODES ARE ADDED TO THE HCPCS DATASET

        UNION  
        SELECT DISTINCT 
        snl.HCPSCD as cms_src_code, 
        snl.HCPSCD as src_mapped_code,
        'HIPPS' as src_vocab_code 
        from `ri.foundry.main.dataset.79489370-9e8d-4743-b0b6-da040f1f2f02` snl
        WHERE snl.RVCNTR = '0022'
        -- for RVCNTR 0022, they are HIPPS codes and the cxwalk map will return null since we don't have HIPPS
        
        UNION  
        SELECT DISTINCT 
        snl.HCPSCD as cms_src_code, 
        snl.HCPSCD as src_mapped_code,
        c.vocabulary_id as src_vocab_code 
        from  `ri.foundry.main.dataset.79489370-9e8d-4743-b0b6-da040f1f2f02` snl
        join  `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c 
          on c.concept_code = snl.HCPSCD 
        where snl.RVCNTR != '0022' 
        -- inner join, so we don't bring in any null vocabulary_ids.

        /*-- before 22.1K --with ndc 31.7k*/
        -- NDC codes/pde	part D	11-digit NDC	PROD_SERVICE_ID
        -- grouper dataset for pde is missing use the schema applied version
        UNION
        SELECT DISTINCT 
        pde.PROD_SERVICE_ID as cms_src_code,
        PROD_SERVICE_ID as src_mapped_code,
        'NDC' as src_vocab_code
        from `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/pde` pde
    ), 

    source_concept_mapping as (
        
        SELECT DISTINCT
        cms.cms_src_code
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
    where src_mapped_code not in ( select icd10_source_code from `ri.foundry.main.dataset.6514962a-d290-4838-b5fa-38ce6656480e`)