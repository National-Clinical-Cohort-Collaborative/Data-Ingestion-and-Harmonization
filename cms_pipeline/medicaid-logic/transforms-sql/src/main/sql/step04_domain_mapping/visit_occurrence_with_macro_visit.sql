CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence_with_macro_visit` AS

   SELECT  v.*,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
LEFT JOIN  `ri.foundry.main.dataset.e5cfffd4-b6dd-4cb5-a3c6-fd52728ec1df` m
ON v.visit_occurrence_id = m.visit_occurrence_id
