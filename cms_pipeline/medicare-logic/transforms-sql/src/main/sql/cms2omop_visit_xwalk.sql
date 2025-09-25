CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk` AS
   
    with plcsrvc_codes as (
        select
              concept_id as source_concept_id
            , concept_code as source_concept_code
            , concept_name as source_concept_name
            , standard_concept
        from `/N3C Export Area/OMOP Vocabularies/concept` c
        ---where c.vocabulary_id in ('CMS Place of Service', 'Medicare Specialty') -----was 59 rows, update to include 'Medicare Specialty' and it is now 91 rows
        where c.vocabulary_id in ('CMS Place of Service') ----adding e 'Medicare Specialty' causes dual maps
    ),

    standard_plcsrvc_codes as (
        select * from plcsrvc_codes
        where standard_concept = 'S'
    ),

    nonstandard_plcsrvc_codes as (
        select * from plcsrvc_codes
        where ISNULL(standard_concept)
    ),

    -- take non-standard CMS Place of Service and map to standard
    mapped_nonstandard_plcsrvc_codes as (
        select
              nonstandard_plcsrvc_codes.*
            , c.concept_id as mapped_concept_id
        from nonstandard_plcsrvc_codes
        INNER JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr
        ON nonstandard_plcsrvc_codes.source_concept_id == cr.concept_id_1
        AND cr.relationship_id = 'Maps to'
        INNER JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
        ON cr.concept_id_2 = c.concept_id
    ),

    -- join standard codes on concept_ancestor table to find their ancestors in the visit hierarchy
    ancestor_standard_plcsrvc_codes as (
        select
              standard_plcsrvc_codes.*
            , ca.ancestor_concept_id as target_concept_id
            , ca.min_levels_of_separation
            , ca.max_levels_of_separation
            , max(max_levels_of_separation) OVER (PARTITION BY source_concept_id) as level_rank
        from standard_plcsrvc_codes
        LEFT JOIN `ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c` ca
        ON standard_plcsrvc_codes.source_concept_id = ca.descendant_concept_id
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
        ON ca.ancestor_concept_id = c.concept_id
        WHERE c.domain_id = 'Visit'
    ),

    -- join mapped non-standard codes on concept_ancestor table to find their ancestors in the visit hierarchy
    ancestor_nonstandard_plcsrvc_codes as (
        select
              mapped_nonstandard_plcsrvc_codes.*
            , ca.ancestor_concept_id as target_concept_id
            , ca.min_levels_of_separation
            , ca.max_levels_of_separation
            , max(max_levels_of_separation) OVER (PARTITION BY source_concept_id) as level_rank
        from mapped_nonstandard_plcsrvc_codes
        LEFT JOIN `ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c` ca
        ON mapped_nonstandard_plcsrvc_codes.mapped_concept_id = ca.descendant_concept_id
        LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
        ON ca.ancestor_concept_id = c.concept_id
        WHERE c.domain_id = 'Visit'
    ),

    -- filter to the ancestor at top most level of the hierarchy and join on concept table to get details
    final_standard_plcsrvc_codes as (
        select 
              ancestor_standard_plcsrvc_codes.source_concept_id
            , ancestor_standard_plcsrvc_codes.source_concept_code
            , ancestor_standard_plcsrvc_codes.source_concept_name
            , c.concept_id as target_concept_id
            , c.concept_code as target_concept_code
            , c.concept_name as target_concept_name
        from ancestor_standard_plcsrvc_codes
        LEFT JOIN  `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
        ON ancestor_standard_plcsrvc_codes.target_concept_id = c.concept_id
        WHERE ancestor_standard_plcsrvc_codes.level_rank = ancestor_standard_plcsrvc_codes.max_levels_of_separation
    ),

    -- filter to the ancestor at top most level of the hierarchy and join on concept table to get details
    final_nonstandard_plcsrvc_codes as (
        select 
              ancestor_nonstandard_plcsrvc_codes.source_concept_id
            , ancestor_nonstandard_plcsrvc_codes.source_concept_code
            , ancestor_nonstandard_plcsrvc_codes.source_concept_name
            , c.concept_id as target_concept_id
            , c.concept_code as target_concept_code
            , c.concept_name as target_concept_name
        from ancestor_nonstandard_plcsrvc_codes
        LEFT JOIN  `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
        ON ancestor_nonstandard_plcsrvc_codes.target_concept_id = c.concept_id
        WHERE ancestor_nonstandard_plcsrvc_codes.level_rank = ancestor_nonstandard_plcsrvc_codes.max_levels_of_separation
    )

select * from final_standard_plcsrvc_codes 

UNION ALL 

SELECT * FROM final_nonstandard_plcsrvc_codes





