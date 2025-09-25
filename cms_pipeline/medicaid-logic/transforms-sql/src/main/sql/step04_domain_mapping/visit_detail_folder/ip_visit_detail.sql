CREATE TABLE `ri.foundry.main.dataset.b846a542-8c6d-462a-ab44-1e230e488c95` AS
-- CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_detail_folder/ip_visit_detail` A
    SELECT DISTINCT
        CLAIMNO
        , PSEUDO_ID as medicaid_person_id
        , to_date(ADMSN_DT, 'ddMMMyyyy') as start_date
        , to_date(DSCHRG_DT, 'ddMMMyyyy') as end_date
        , admission_datetime
        , ADMSN_TYPE_CD as admitting_source_value--- CMS Place of Service ( 1EMERGENCY=>23Emergency Room - Hospital/ 2URGENT=>20Urgent Care Facility, 3ELECTIVE = 22Outpatient Hospital,4NEWBORN=25BIRTHING ,5, 9 UNKNOWN)
        --, BLG_PRVDR_SPCLTY_CD as place_of_service -- if A0 then it is hospital
        , COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
        , SRVC_PRVDR_NPI as provider_npi  --health care professional who delivers or completes a particular medical service or non-surgical procedure.
        , REV_CNTR_CD
        --, visit_concept_id ------ BASED ON REV_CNTR_CD in step 03_5 determin ip or erip
        ,  case when ipp.REV_CNTR_CD IN ("0450", "0451", "0452", "0456", "0459", "0981" ) then  262 -- iper ERIP Emergency Room and Inpatient Visit
            else 9201 --- inpatient visit
              end as visit_concept_id 
        , 32021 as visit_type_concept_id 
      FROM `ri.foundry.main.dataset.4994c288-97f1-4b26-8867-14311851ef30` ipp
      WHERE MDCD_PD_AMT >= 0 /* Acumen suggest to check for payment > 0 */
      AND CLM_TYPE_CD NOT IN ( 'Z')