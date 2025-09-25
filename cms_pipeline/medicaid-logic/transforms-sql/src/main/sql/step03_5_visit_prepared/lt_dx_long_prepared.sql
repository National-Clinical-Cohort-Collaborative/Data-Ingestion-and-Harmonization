CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/lt_dx_long_prepared` AS
     SELECT DISTINCT 
    lt.*
    -- long term care should receive non-hospital institutional visit assignment
    -- however, if it is home health then it should be outpatient visit 
    -- reference : https://forums.ohdsi.org/t/choosing-visit-occurrence-visit-concept-id-for-certain-visits-in-claim-data/3387
    -- ,case when BILL_TYPE_CD_digit3 = '1' then 9201 
    --       when BILL_TYPE_CD_digit3 = '2' then 9201 
    --       when BILL_TYPE_CD_digit3 = '8' then 9201 
    --       when BILL_TYPE_CD_digit3 = '9' then 0 
    --       else 9202
    --       end as visit_concept_id
    --2nd digit - BILL_TYPE_CD	indicates the Type of Facility
-- BILL_TYPE_CD	1	Hospital
-- BILL_TYPE_CD	2	Skilled Nursing
-- BILL_TYPE_CD	3	Home Health
-- BILL_TYPE_CD	4	Religious Nonmedical (Hospital)
-- BILL_TYPE_CD	5	Reserved for national assignment (discontinued effective 10/1/05).
-- BILL_TYPE_CD	6	Intermediate Care
-- BILL_TYPE_CD	7	Clinic or Hospital Based Renal Dialysis Facility (requires special information in second digit below).
-- BILL_TYPE_CD	8	Special facility or hospital ASC surgery (requires special information in second digit below).
-- BILL_TYPE_CD	9	Reserved for National Assignment
    -- lt= long term care rolls up to non-hospital institution visit, long term care 
    ---https://athena.ohdsi.org/search-terms/terms/42898160
    --SN or Hospice to Long Term Care Visit (42898160)
    -- acumen informed us if BILL_TYPE_CD_digit2 = '3' is then it is home health -- >outpatient
    --  acumen informed us if BILL_TYPE_CD_digit2 = '1' then hospital --> inpatient
    -- all other case to long term care ---> 42898160
    , CASE WHEN BILL_TYPE_CD_digit2 = '3' then 9202  -- 3= home health -> outpatient, all other should be mapped to long term care visit
        when BILL_TYPE_CD_digit2 = '1' then 9201 --> 1 = hospital -->inpatient
        else 42898160 end as visit_concept_id
    --- `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/lt_dx_long_prepared` 
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/lt_dx_long_prepared` lt