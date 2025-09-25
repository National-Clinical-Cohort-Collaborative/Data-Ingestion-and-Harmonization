CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/visit_occurrence_with_macro_visit` AS
   -- visit_occurrence_id from the visit_occurrence dataset is the primary key on this dataset
   --   this dataset is identical to visit_occurrence with the addition of three macrovisit columns. 
   --   the rowcount will be identical
   SELECT  v.*,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
LEFT JOIN  `ri.foundry.main.dataset.07ca2570-87ca-4e24-ac90-8c4a264e900b` m
ON v.visit_occurrence_id = m.visit_occurrence_id