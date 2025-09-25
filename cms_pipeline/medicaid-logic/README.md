### Repo for Converting Medicaid Data to OMOP Format
#### Useful links:
[Data Dictionary](https://unite.nih.gov/workspace/fusion/spreadsheet/ri.fusion.main.document.f7c5c097-96f2-40cb-91a5-8e49b99b9e46/spreadsheet/TAF%20Data%20Dictionary)

[NLM Medicaid Mapping](https://github.com/lhncbc/CRI/tree/master/EtlOmopMedicaid)

[TAF info](https://www.medicaid.gov/medicaid/data-systems/macbis/medicaid-chip-research-files/transformed-medicaid-statistical-information-system-t-msis-analytic-files-taf/index.html)


#### Input tables:
* de_base = Demographics and Eligibility (DE) base
* de_dates = Demographics and Eligibility (DE) dates
* de_dsb = Disability
* de_hh_and_spo = Home Health (HH) and State Plan Option (SPO)
* de_mc = Managed Care Plan
* de_mfp = Money Follows the Person plan 
* de_wvr = Waiver plan
* ip = Inpatient (Claims)
* lt = Long-term care (Claims)
* ot = Other services (Claims)
* rx = Prescriptions (Claims)

##### Key Fields
* PSEUDO_ID
* MSIS_ID - A state-assigned unique identification number used to identify a Medicaid/CHIP enrolled beneficiary and any claims submitted to the system. Also referred to as the Medicaid Statistical Information System Identifier (MSIS_ID).
* ADMSN_DT - The date on which the recipient was admitted to a hospital.
* DSCHRG_DT - IF MISSING WHICH DATE TO USE? IF NULL SHOULD WE USE ADMSN_DT?
* PROVIDER : use BLG_PRVDR_NPI and/or  SRVC_PRVDR_NPI may neeed to pull in both for visits
* note, BLG_PRVDR_NPI - IF NULL use SRVC_PRVDR_NPI,  ADMTG_PRVDR_NPI, RFRG_PRVDR_NPI there are other NPIs.
* BILL_TYPE_CD -A data element corresponding with UB-04 form locator FL4 that classifies the claim as to the type of facility (2nd digit), type of care (3rd digit) and the billing record's sequence in the episode of care (4th digit). (Note that the 1st digit is always zero.)


#### Step 3
##### Melting Basic Info
* Dates come in 2 formats: static (start and end date specified) and dynamic (end date is null)
* Melting can be done via column_group (when a column has _1 - _12 for example) and column_list (one column that gets melted)
* Each config file has dictionaries of melted = {groupName: groupSettings}

* dx = icd10cm
* px = icd10pcs
* hcpcs = hcpcs or cpt4

##### 1_initial
* 1_initial step will create {dataset_column} datasets that always have the same schema 
* Not every dataset will have outputs in initial folder 

##### 2_datasets
* 2_datasets step takes in the domain's datasets created in 1_initial and unions them 
* The output from this step is the number of datasets with melting info specified in config

##### 2_groups
* 2_groups step maps {groupName: [datasets that have groupName in their melting config]}
* 2_groups then unions all datasets for a given groupName 
* Output from this step is the number of unique groupNames in the config files

##### 3_all
* 3_all step creates one unioned dataset of all datasets created in 1_initial

##### Prepared
Based on what is done in melting, certain codes (groupNames) will need to be cleaned / filtered / joined to the main data

##### Melting Codes
* IP:
    * ICD10CM - ADMTG_DGNS_CD, DGNS_CD_1-12
    * ICD10PCS - PRCDR_CD_1 - 6
    * dates: ADMSN_DT - DSCHRG_DT
* LT:
    * ICD10CM - ADMTG_DGNS_CD
    * ICD10CM - DGNS_CD_1 - 5
    * dates: ADMSN_DT - DSCHRG_DT
* OT:
    * ICD10CM - DGNS_CD_1 - 2
    * CPT or HCPCS - LINE_PRCDR_CD

    * LINE_PRCDR_CD
    * LINE_PRCDR_CD_DT -- is the date same as SRVC_BGN_DT? if not need to add
    * LINE_PRCDR_CD_SYS
    * LINE_PRCDR_MDFR_CD_1
* LINE_PRCDR_MDFR_CD_2
* LINE_PRCDR_MDFR_CD_3
* LINE_PRCDR_MDFR_CD_4
    * dates: SRVC_BGN_DT - SRVC_END_DT

    * DGNS_POA_IND_1
    * DGNS_POA_IND_2
* RX: 
    * NDC - NDC    
    * dates: PRSCRBD_DT -> None (end date is dynamic)
    * PRSCRBD_DT is the date the doctor issued the prescription but the patient may have picked up the meds until some time after so we will need to use the MDCD_PD_DT date

#### Dataset Details
* codemap xwalk for domain map tranformation
* valueset crosswalk for OMOP concepts ids to use for the enumerated list of permissible values found in the source data. 
* define visits 

##### Base

##### Dates 

##### Disability 

##### Home Health and State Plan Option 

##### Managed Care Plan 

##### Money Follows the Person Plan

##### Waiver Plan 

##### Inpatient 

##### Long-term Care 
See the comments and code in check_lt_steps.sql in the DQ repo: https://unite.nih.gov/workspace/data-integration/code/repos/ri.stemma.main.repository.98023975-eafd-442e-9f68-ead495e2d716/contents/refs%2Fheads%2FCR_count_visit_types/transforms-sql/src/main/sql/input_vs_output_counts/check_LT_steps.sql 


##### Other Services

##### Prescriptions 