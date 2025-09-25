CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/rx_prepared` AS
   SELECT DISTINCT 
    rx.*
    --- pharmacy visit
    , cast( 581458 as int) as visit_concept_id 
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/rx` rx
