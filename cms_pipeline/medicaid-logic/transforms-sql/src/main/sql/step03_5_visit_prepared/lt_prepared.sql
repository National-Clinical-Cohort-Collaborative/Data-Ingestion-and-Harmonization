CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/lt_prepared` AS

/*  LT source file visit concept id
    use BILL_TYPE_CD - based on 1% data this column was well populated compare to REV_CNTR_CD can be enhance with REV_CNT_CD if BILL_TYPE_CD IS missing data. 
    ADMSN_TYPE_CD: not in schema
    REV_CNTR_CD: 840 null/empty
    BILL_TYPE_CD: 530 null/empty 
    MDCR_REIMBRSMT_TYPE_CD: 12.3k null/empty

    the 3rd digit is the visit type classification of the claim line 
*   3rd Digit-Bill Classification (Except Clinics and Special Facilities)
* 9201 = inpatient visit = 1	Inpatient or  2	Inpatient/ == 9201 inpatient visit 
* 9201 = inpatient if BILL_TYPE_CD 3rd digit is =  8	Swing Bed (may be used to indicate billing for SNF level of care in a hospital with an approved swing bed agreement). - is swing bed inpatient visit?
* 9202 = outpatient visit = 3	Outpatient or /4	Other or /5	Intermediate Care - Level I or/ 6	Intermediate Care - Level II / 7	Reserved for national assignment (discontinued effective 10/1/05).
* 9202  = 9	Reserved for National Assignment
*/
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
    ---------------------------------------------------
    --     Not Applicable	2nd Digit-Type of Facility
    -- 1	Hospital --> inpatient 
    -- 2	Skilled Nursing -->42898160
    -- 3	Home Health --> outpatient
    -- lt= long term care rolls up to non-hospital institution visit, long term care 
    ---https://athena.ohdsi.org/search-terms/terms/42898160
    ---https://forums.ohdsi.org/t/choosing-visit-occurrence-visit-concept-id-for-certain-visits-in-claim-data/3387/3
    --SN or Hospice to Long Term Care Visit (42898160)
    -- acumen informed us if BILL_TYPE_CD_digit2 = '3' is then it is home health
    --, CASE WHEN BILL_TYPE_CD_digit2 = '3' then 9202  -- 3= home health -> outpatient, all other should be mapped to long term care visit
    ---all claims in lt should get long term care visit
    , CASE WHEN BILL_TYPE_CD_digit2 = '3' then 9202  -- 3= home health -> outpatient, all other should be mapped to long term care visit
        when BILL_TYPE_CD_digit2 = '1' then 9201 --> 1 = hospital -->inpatient
        else 42898160 end as visit_concept_id
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/lt_prepared` lt