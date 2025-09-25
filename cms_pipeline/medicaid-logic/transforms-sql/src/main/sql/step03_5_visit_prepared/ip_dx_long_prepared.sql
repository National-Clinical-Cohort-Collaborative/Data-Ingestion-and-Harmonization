CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03_5 - visit prepared/ip_dx_long_prepared` AS
    -- ip or erip
    -- compute visit_concept_id

    SELECT DISTINCT --ipx.*, 
      pkey,
      PSEUDO_ID,
      --MSIS_ID,
      --CLM_NUM_ORIG,
      BLG_PRVDR_NPI,
      SRVC_PRVDR_NPI,
      SRVC_PRVDR_ID,
      --REV_CNTR_CD,
      BLG_PRVDR_SPCLTY_CD,
      --MDCR_REIMBRSMT_TYPE_CD,
      ADMSN_DT,
      DSCHRG_DT,
      --BILLED_AMT,
      --CLM_TYPE_CD,
      --YEAR,
      --LINE_NUM,
      MDCD_PD_AMT,
      CLAIMNO,
      --BILL_TYPE_CD,
      --BILL_TYPE_CD_digit2,
      --BILL_TYPE_CD_digit3,
      src_domain,
      src_code_type,
      src_vocab_code,
      index,
      src_column,
      src_code,
      start_date_key,
      start_date_value,
      end_date_key,
      end_date_value,
        --- determine the visit_concept id based on the value in the REV_CNT_CD 
            case when size(array_intersect( collect_set(REV_CNTR_CD), array('0450', '0451', '0452', '0456', '0459', '0981' ))) > 0 then 262
            else 9201
            end as visit_concept_id
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - prepared/ip_dx_long_prepared` ipx
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
