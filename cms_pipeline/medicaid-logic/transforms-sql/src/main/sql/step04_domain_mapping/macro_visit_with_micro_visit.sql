CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit_with_micro_visit` AS

    with macro_visit as (

    select     
    v.visit_occurrence_id
    , v.person_id
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
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` v
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit` m on v.person_id = m.person_id
    AND v.visit_start_date >= m.macrovisit_start_date
    AND v.visit_start_date <= m.macrovisit_end_date
    order by person_id, macrovisit_start_date
    )

    SELECT 
    m.* 
    , cast(conv(substr( macrovisit_hashed_id, 1, 15) , 16, 10) as bigint) as macro_visit_long_id
    from macro_visit m

    
  
