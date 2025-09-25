CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ip_prepared` AS
    /* use REV_CNTR_CD column to determine if the visit concept id should be 262/ERIP 
    Source data is IP = inpatient / REV_CNTR_CD column is mostly populated compared to BILL_TYPE_CD
    However if the REV_CNTR_CD is ER then the visit type id ERIP
    OMOP = 262/ ERIP  visit
    OMOP = 9201/ inpatient visit
    0450 = Emergency room - general classification
    0451 = Emergency room - EMTALA emergency medical screening services
    0452 = Emergency room - ER beyond EMTALA screening
    0456 = Emergency room-urgent care
    0459 = Emergency room-other
    0981 = Professional fees-emergency room
    */
    SELECT DISTINCT 
     ip.*
    ,case when REV_CNTR_CD = '0450' then 262 
          when REV_CNTR_CD = '0451' then 262
          when REV_CNTR_CD = '0452' then 262 
          when REV_CNTR_CD = '0456' then 262
          when REV_CNTR_CD = '0459' then 262
          when REV_CNTR_CD = '0981' then 262 
          else 9201
          end as visit_concept_id
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/ip_prepared` ip


--     pkey,
-- PSEUDO_ID,
-- ADMSN_DT,
-- ADMSN_HR,
-- ADMSN_TYPE_CD,
-- ADMTG_DGNS_CD,
-- BILL_TYPE_CD,
-- BLG_PRVDR_ID,
-- BLG_PRVDR_NPI,
-- BLG_PRVDR_SPCLTY_CD,
-- DGNS_CD_1,
-- DGNS_CD_10,
-- DGNS_CD_11,
-- DGNS_CD_12,
-- DGNS_CD_2,
-- DGNS_CD_3,
-- DGNS_CD_4,
-- DGNS_CD_5,
-- DGNS_CD_6,
-- DGNS_CD_7,
-- DGNS_CD_8,
-- DGNS_CD_9,
-- DGNS_POA_IND_1,
-- DGNS_POA_IND_10,
-- DGNS_POA_IND_11,
-- DGNS_POA_IND_12,
-- DGNS_POA_IND_2,
-- DGNS_POA_IND_3,
-- DGNS_POA_IND_4,
-- DGNS_POA_IND_5,
-- DGNS_POA_IND_6,
-- DGNS_POA_IND_7,
-- DGNS_POA_IND_8,
-- DGNS_POA_IND_9,
-- DGNS_VRSN_CD_1,
-- DGNS_VRSN_CD_10,
-- DGNS_VRSN_CD_11,
-- DGNS_VRSN_CD_12,
-- DGNS_VRSN_CD_2,
-- DGNS_VRSN_CD_3,
-- DGNS_VRSN_CD_4,
-- DGNS_VRSN_CD_5,
-- DGNS_VRSN_CD_6,
-- DGNS_VRSN_CD_7,
-- DGNS_VRSN_CD_8,
-- DGNS_VRSN_CD_9,
-- DSCHRG_DT,
-- DSCHRG_HR,

---NDC,
---NDC_QTY,
---NDC_UOM_CD,
-- PRCDR_CD_1,
-- PRCDR_CD_2,
-- PRCDR_CD_3,
-- PRCDR_CD_4,
-- PRCDR_CD_5,
-- PRCDR_CD_6,
-- PRCDR_CD_DT_1,
-- PRCDR_CD_DT_2,
-- PRCDR_CD_DT_3,
-- PRCDR_CD_DT_4,
-- PRCDR_CD_DT_5,
-- PRCDR_CD_DT_6,
------------REV_CNTR_CD,
--SRVC_BGN_DT,
--SRVC_END_DT,
--SRVC_PRVDR_SPCLTY_CD,
-- BILL_TYPE_CD_digit1,
-- BILL_TYPE_CD_digit2,
-- BILL_TYPE_CD_digit3,
-- BILL_TYPE_CD_digit4,
-- admsn_dt_concat,
-- admission_datetime
