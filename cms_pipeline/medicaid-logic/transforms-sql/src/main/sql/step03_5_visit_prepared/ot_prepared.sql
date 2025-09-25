CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ot_prepared` AS
    SELECT DISTINCT 
    ot.*
    ---9 is unknown, 1,2 or 8 is inpatient visit, all other outpatient visit
    ---POS_CD 21 = inpatient hospital
    -- POS_CD 22 =	Outpatient Hospital is mapped to outpatient visit in OMOP vocabulary
    --shong, TODO: 100% data may provide different data variables
    ---,case when MDCR_REIMBRSMT_TYPE_CD='01' and POS_CD = '21' then 9201
    ---    else 9202
    ---    end as visit_concept_id
    -- ,case when BILL_TYPE_CD_digit3 = '1' then 9201 
    --       when BILL_TYPE_CD_digit3 = '2' then 9201 
    --       when BILL_TYPE_CD_digit3 = '8' then 9201 
    --       when BILL_TYPE_CD_digit3 = '9' then 0 
    --       else 9202
    --       end as visit_concept_id
    ---, as visit_concept_id 
    -- if home health then outpatient ---------------------
    -- if hospice then non-hospital institutional visit.  -------------------------
    -- ot is outpatient and home health is outpatient, however if it is hospice claim then it should have the non-hospital institutional visit. 
    -- NOTE,  home health and hospice visit will be in the OT file
    -- default visit type is outpatien however, if hopise then non-hospital institutional visit. 
    --BILL_TYPE_CD_digit2 = '3' -- hh
    ---BILL_TYPE_CD_digit = 8 and BILL_TYPE_CD_digit2 = 1 or 2 (combined 81 or 82) -- then hospice 
    --BILL_TYPE_CD is 4 digit code xxxx
    -- , case when (SUBSTRING(BILL_TYPE_CD, 2,2) = '81' ) or (SUBSTRING(BILL_TYPE_CD, 2,2) = '82') then 42898160
    --     else 9202
    --     end as visit_concept_id
    , case when REV_CNTR_CD in ("0450", "0451", "0452", "0456", "0459", "0981" ) then 9203 ----Emergency Room Visit	9203
        else 9202 --Outpatient Visit	9202
        end as visit_concept_id_per_row
    FROM `ri.foundry.main.dataset.10f492d9-ad21-477a-9633-80d42000b743` ot