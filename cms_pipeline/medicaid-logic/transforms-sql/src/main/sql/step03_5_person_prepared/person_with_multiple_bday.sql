CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - person prepared/person_with_multiple_bday` AS
    
SELECT distinct
    PSEUDO_ID
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` p
    where LENGTH (BIRTH_DT) > 0 
    group by PSEUDO_ID
    having count( DISTINCT BIRTH_DT) > 1