CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/macro_visit_with_micro_visit` AS

    with macro_visit as (

    select     
    v.visit_occurrence_id
    , v.care_site_id
    , v.visit_concept_id
    , v.visit_start_date
    , v.visit_end_date
    , m.macrovisit_id
    , m.macrovisit_start_date
    , m.macrovisit_end_date
    , md5(concat_ws(
              ';'
        , COALESCE(visit_occurrence_id, '')     
        , COALESCE(macrovisit_id, '')
        , COALESCE(visit_concept_id, '')
        , COALESCE('medicaid', ''))) AS macrovisit_hashed_id
    FROM `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    LEFT JOIN `ri.foundry.main.dataset.eb1d9b95-6744-4e6e-bd19-5abf55655f2c` m on v.care_site_id = m.person_id
    AND v.visit_start_date >= m.macrovisit_start_date
    AND v.visit_start_date <= m.macrovisit_end_date
    order by care_site_id, macrovisit_start_date
    )

    SELECT 
    m.* 
    , cast(conv(substr( macrovisit_hashed_id, 1, 15) , 16, 10) as bigint) as macro_visit_long_id
    from macro_visit m

    