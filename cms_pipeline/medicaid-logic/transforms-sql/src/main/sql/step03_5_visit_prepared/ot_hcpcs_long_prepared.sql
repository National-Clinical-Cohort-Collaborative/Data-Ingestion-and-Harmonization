CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ot_hcpcs_long_prepared` AS
    SELECT DISTINCT 
    ot.*
    --MDCR_REIMBRSMT_TYPE_CD	01	IPPS - Acute Inpatient PPS and POS_CD = '21' is inpatient hospital
    --, case when MDCR_REIMBRSMT_TYPE_CD='01' and POS_CD = '21' then 9201
    --    else 9202
    --    end as visit_concept_id
    -- ,case when BILL_TYPE_CD_digit3 = '1' then 9201 
    --       when BILL_TYPE_CD_digit3 = '2' then 9201 
    --       when BILL_TYPE_CD_digit3 = '8' then 9201 
    --       when BILL_TYPE_CD_digit3 = '9' then 0 
    --       else 9202
    --       end as visit_concept_id
    ---, as visit_concept_id 
--  , case when (SUBSTRING(BILL_TYPE_CD,2 ,2) = '81' ) or (SUBSTRING(BILL_TYPE_CD,2 ,2) = '82') then 42898160
--         else 9202
--         end as visit_concept_id
  , case when REV_CNTR_CD in ("0450", "0451", "0452", "0456", "0459", "0981" ) then 9203 ----Emergency Room Visit	9203
        else 9202 --Outpatient Visit	9202
        end as visit_concept_id
    FROM `ri.foundry.main.dataset.c138564f-868c-4b8d-b15b-cd254783360a` ot