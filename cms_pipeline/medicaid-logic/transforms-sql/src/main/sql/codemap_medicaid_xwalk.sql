CREATE TABLE `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` AS

    with cms_codes as (
        SELECT DISTINCT 
        d.src_code as cms_src_code, 
        -- missing the dot
        case when LENGTH(TRIM(d.src_code)) > 3 and src_vocab_code = 'ICD10CM' THEN concat_ws( '.', SUBSTRING(d.src_code, 1, 3),  SUBSTRING(src_code, 4, LENGTH(trim(src_code)) )) 
        ELSE d.src_code 
        END as src_mapped_code, 
        d.src_vocab_code,
        'ICD10CM' as mapped_code_system
        FROM `ri.foundry.main.dataset.a626f796-e66a-4d01-99f5-a3bad5aab0c7` d
--ICD10PCS
        UNION
        -- all 8 length pcs codes
        -- pcs codes are sourced from ip and ot claims --
        -- ip from PRCDR_CD_1 to PRCDR_CD_6 and ot from LINE_PRCDR_CD.
        -- must reference PRCDR_CD_SYS_1-6 and LINE_PRCDR_CD_SYS to retrieve the code system, repectively 
        -- we are switching to use the proc long prepared dataset from px in order to retrieve the procedure codes with respective code systems
        ---as the melted groups px dataset does not reference the code system columns from ip where the codes can be found in PRCDR_CD_1 to PRCDR_CD_6 therefore it may not be accurate. 
        -- if null code system is found the claim is ip use icd10pcs as the default code system
        --from ip
        --PRCDR_CD_SYS=01=CPT 4
        -- PRCDR_CD_SYS=02 =ICD-9 CM
        -- PRCDR_CD_SYS=06 =HCPCS (Both National and Regional HCPCS)
        -- PRCDR_CD_SYS=07 =ICD-10 - CM PCS
        -- PRCDR_CD_SYS=10-87=Other Systems
        select DISTINCT 
        p.src_code as cms_src_code,
        p.src_code as src_mapped_code, 
        cast( PRCDR_CD_SYS as string) as src_vocab_code,
        'ICD10PCS' as mapped_code_system
        ---mapped_src_vocab_cd as mapped_code_system
        from `ri.foundry.main.dataset.2495c751-de3b-4743-8b93-2c1f903caba1` p -- maps null code system as the icd10pcs in ip_proc_long_prepared
        where p.mapped_src_vocab_cd ='ICD10PCS' -- mapped_src_vocab_cd --
        UNION
        
        -- from ot
        select DISTINCT 
        LINE_PRCDR_CD as cms_src_code,
        o.src_code as src_mapped_code, 
        o.LINE_PRCDR_CD_SYS as src_vocab_code,
        'ICD10PCS' as mapped_code_system
        from `ri.foundry.main.dataset.8e757c3a-ba1e-4f73-8f27-202e5a2a449d` o
        where o.LINE_PRCDR_CD_SYS = '07' -- 07='ICD10PCS'
--HCPCS
        union 
        --hcpcs 5 digit hcpcs codes
        --hcpcs code comes from ip and ot
        -- select DISTINCT -- these code are not purely hcpcs, need to check the code system of the code 
        -- h.src_code as cms_src_code,
        -- h.src_code as src_mapped_code,
        -- h.src_vocab_code,
        -- 'HCPCS' as mapped_code_system
        -- from `ri.foundry.main.dataset.4c49ab3d-3e40-45dc-b0c5-65b519732514` h
        --from ip
        select DISTINCT 
        p.src_code as cms_src_code,
        p.src_code as src_mapped_code, 
       cast( PRCDR_CD_SYS as string) as src_vocab_code,
        'HCPCS' as mapped_code_system
        from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/ip_proc_long_prepared` p
        where p.mapped_src_vocab_cd ='HCPCS' -- mapped_src_vocab_cd 
        -- FROM ot
        UNION
        select DISTINCT 
        LINE_PRCDR_CD as cms_src_code,
        o.src_code as src_mapped_code, 
        o.LINE_PRCDR_CD_SYS as src_vocab_code,
        'HCPCS' as mapped_code_system
        from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/ot_proc_long_prepared` o
        where o.LINE_PRCDR_CD_SYS = '06' or  o.LINE_PRCDR_CD_SYS is null  -- 06='HCPCS' and if the sepcified code system is null then default as null 
--- cpt4 
        UNION
        select DISTINCT 
        p.src_code as cms_src_code,
        p.src_code as src_mapped_code, 
       cast( PRCDR_CD_SYS as string) as src_vocab_code,
        'CPT4' as mapped_code_system
        from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/ip_proc_long_prepared` p
        where p.mapped_src_vocab_cd ='CPT4' -- mapped_src_vocab_cd
        -- FROM ot
        UNION
        select DISTINCT 
        o.src_code as cms_src_code,
        o.src_code as src_mapped_code, 
        LINE_PRCDR_CD_SYS as src_vocab_code,
        'CPT4' as mapped_code_system
        from `ri.foundry.main.dataset.8e757c3a-ba1e-4f73-8f27-202e5a2a449d` o
        where o.LINE_PRCDR_CD_SYS = '01' -- 01='CPT4'
---NDC
        /*-- 1% sample-25.6K  */
        -- 11 digits ndc codes
        UNION
       SELECT DISTINCT 
        rx.NDC as cms_src_code,
        rx.NDC as src_mapped_code,
        'NDC' as src_vocab_code, -- NDC are mapped to RxNorm
        'NDC' as mapped_code_system
        from `ri.foundry.main.dataset.292106b5-cd0c-4a32-bc2d-6f9369ffac0b` rx
        UNION
        /* make sure the NDC from IP are included, not sure if melted datasets are including these. */
         -- 11 digits ndc codes
        SELECT DISTINCT 
        ip.NDC as cms_src_code,
        ip.NDC as src_mapped_code,
        'NDC' as src_vocab_code, -- NDC are mapped to RxNorm
        'NDC' as mapped_code_system
        FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea` ip
    ), 

    source_concept_mapping as (
        
        SELECT DISTINCT
        cms.cms_src_code
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
    where src_mapped_code not in ( select icd10_source_code from `ri.foundry.main.dataset.6514962a-d290-4838-b5fa-38ce6656480e`)