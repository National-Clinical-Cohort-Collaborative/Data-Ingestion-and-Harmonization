CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ot_dx_long_prepared` AS

    SELECT DISTINCT 
    ot.*
    --- use the bill type code to determine type of visit
    -- the 3rd digit is the visit type classification of the claim line 
    -- *   3rd Digit-Bill Classification (Except Clinics and Special Facilities)
-- * 9201 = inpatient visit = 1	Inpatient or  2	Inpatient/ == 9201 inpatient visit 
-- * 9201 = inpatient if BILL_TYPE_CD 3rd digit is =  8	Swing Bed (may be used to indicate billing for SNF level of care in a hospital with an approved swing bed agreement). - is swing bed inpatient visit?
-- * 9202 = outpatient visit = 3	Outpatient or /4	Other or /5	Intermediate Care - Level I or/ 6	Intermediate Care - Level II / 7	Reserved for national assignment (discontinued effective 10/1/05).
-- * 9202  = 9	Reserved for National Assignment
    ---9 is unknown, 1,2 or 8 is inpatient visit, all other outpatient visit
    -- ,case when BILL_TYPE_CD_digit3 = '1' then 9201 
    --       when BILL_TYPE_CD_digit3 = '2' then 9201 
    --       when BILL_TYPE_CD_digit3 = '8' then 9201 
    --       when BILL_TYPE_CD_digit3 = '9' then 0 
    --       else 9202
    --       end as visit_concept_id
    -- ---, as visit_concept_id 
    --may need to find all other non-hospital institutional visits
    --MDCR_REIMBRSMT_TYPE_CD	01	IPPS - Acute Inpatient PPS and POS_CD = '21' is inpatient hospital
    -- , case when MDCR_REIMBRSMT_TYPE_CD='01' and POS_CD = '21' then 9201
    --     else 9202
    --     end as visit_concept_id
    -- , case when (SUBSTRING(BILL_TYPE_CD,2 ,2) = '81' ) or (SUBSTRING(BILL_TYPE_CD,2,2) = '82') then 42898160
    --     else 9202
    --     end as visit_concept_id
    , case when REV_CNTR_CD in ("0450", "0451", "0452", "0456", "0459", "0981" ) then 9203 ----Emergency Room Visit	9203
        else 9202 --Outpatient Visit	9202
        end as visit_concept_id
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/ot_dx_long_prepared` ot