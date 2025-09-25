CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_sex` AS
    
SELECT distinct
    PSEUDO_ID
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` p
    where LENGTH (SEX_CD) > 0 
    group by PSEUDO_ID
    having count( DISTINCT SEX_CD) > 1