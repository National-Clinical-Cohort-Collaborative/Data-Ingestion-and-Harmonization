#### Notes about mapping from step 3 prepared to OMOP 	- Stephanie Hong, shong59@jh.edu
## visit mapping
## visit mapping -- evolving list
### IP visit
        * PSEUDO_ID
        * ADMSN_TYPE_CD
        * REV_CNTR_CD
        * BILL_TYPE_CD
        * MDCR_REIMBRSMT_TYPE_CD
        * BLG_PRVDR_SPCLTY_CD
        * BLG_PRVDR_NPI
        * SRVC_PRVDR_NPI  
        * OPRTG_PRVDR_NPI -- surgical
        * ADMTG_PRVDR_NPI -- admitting
        * RFRG_PRVDR_NPI -- referring_npi
        * --dates
        * ADMSN_DT
        * DSCHRG_DT

### LT visits
        * PSEUDO_ID
        * BILL_TYPE_CD
        * REV_CNTR_CD
        * MDCR_REIMBRSMT_TYPE_CD
        * BLG_PRVDR_SPCLTY_CD
        * BLG_PRVDR_NPI
        * SRVC_PRVDR_NPI
        * -- dates
        * SRVC_BGN_DT
        * max(SRVC_END_DT)  

### OT visits
        * PSEUDO_ID
        * BILL_TYPE_CD_digit3
        * REV_CNTR_CD
        * BILL_TYPE_CD
        * MDCR_REIMBRSMT_TYPE_CD
        * BLG_PRVDR_SPCLTY_CD
        * BLG_PRVDR_NPI
        * SRVC_PRVDR_NPI
        * HLTH_HOME_PRVDR_NPI
        * RFRG_PRVDR_NPI
        * -- dates
        * SRVC_BGN_DT
        * max(SRVC_END_DT)
* ot is outpatient
* from the ot file MDCR_REIMBRSMT_TYPE_CD will be 7,8,1,4
* MDCR_REIMBRSMT_TYPE_CD	01	IPPS - Acute Inpatient PPS
* MDCR_REIMBRSMT_TYPE_CD	04	HHPPS - Home Health PPS
* MDCR_REIMBRSMT_TYPE_CD	07	OPPS - Outpatient PPS
* MDCR_REIMBRSMT_TYPE_CD	08	Fee Schedules (for physicians, DME, ambulance, and clinical lab)

 * BILL_TYPE_CD	1	Hospital
 * BILL_TYPE_CD	2	Skilled Nursing
 * BILL_TYPE_CD	3	Home Health
 * BILL_TYPE_CD	4	Religious Nonmedical (Hospital)
 * BILL_TYPE_CD	5	Reserved for national assignment (discontinued effective 10/1/05).
 * BILL_TYPE_CD	6	Intermediate Care
 * BILL_TYPE_CD	7	Clinic or Hospital Based Renal Dialysis Facility (requires special information in second digit below).
 * BILL_TYPE_CD	8	Special facility or hospital ASC surgery (requires special information in second digit below).
 * BILL_TYPE_CD	9	Reserved for National Assignment
  *  use the bill type code to determine type of visit
  *  the 3rd digit is the visit type classification of the claim line
  *   *   3rd Digit-Bill Classification (Except Clinics and Special Facilities)
 * 9201 = inpatient visit = 1	Inpatient or  2	Inpatient/ == 9201 inpatient visit
 * 9201 = inpatient if BILL_TYPE_CD 3rd digit is =  8	Swing Bed (may be used to indicate billing for SNF level of care in a hospital with an approved swing bed agreement). - is swing bed inpatient visit?
 * 9202 = outpatient visit = 3	Outpatient or /4	Other or /5	Intermediate Care - Level I or/ 6	Intermediate Care - Level II / 7	Reserved for national assignment (discontinued effective 10/1/05).
 * 9202  = 9	Reserved for National Assignment

### IP visit NPIs
* BLG_PRVDR_NPI	- empty 2.3k/10 digits
* The National Provider ID (NPI) of the billing entity responsible for billing a patient for healthcare services.
* The billing provider can also be servicing, referring, or prescribing provider. Can be admitting provider except for Long Term Care.
* -------- When BLG_PRVDR_NPI is null,
* -------- there may be both ADMTG_PRVDR_NPI and the SRVC_PRVDR_NPI --- which one should be used as the provider?
* --- for now we will pull both provider information, admitting npi and medical service provider
* SRVC_PRVDR_NPI - empty 6.9K --The National Provider Identifier (NPI) of the health care professional who delivers or completes a particular medical service or non-surgical procedure.

* OPRTG_PRVDR_NPI - empty 27.4k /10 digits -- The National Provider ID (NPI) of the provider who performed the surgical procedure(s).	Operating Provider NPI

* ADMTG_PRVDR_NPI - empty 23.9k -- The National Provider ID (NPI) of the doctor responsible for admitting a patient to a hospital or other inpatient health facility.

* RFRG_PRVDR_NPI - empty 34.4
* The National Provider Identifier (NPI) assigned to a provider which identifies the physician or other provider who referred the patient.

### IP, ADMSN_TYPE_CD
* ADMSN_TYPE_CD	1	EMERGENCY The patient requires immediate medical intervention as a result of severe, life threatening or potentially disabling conditions. Generally, the patient is admitted through the emergency room.
* ADMSN_TYPE_CD	2	URGENT The patient requires immediate attention for the care and treatment of a physical or mental disorder. Generally, the patient is admitted to the first available and suitable accommodation.
* ADMSN_TYPE_CD	3	ELECTIVE The patients condition permits adequate time to schedule the availability of a suitable accommodation.
* ADMSN_TYPE_CD	4	NEWBORN The patient is a newborn delivered either inside the admitting hospital (UB04 FL 15 value 5 [A baby born inside the admitting hospital] or outside of the hospital (UB04 FL 15 value 6 [A baby born outside the admitting hospital]).
* ADMSN_TYPE_CD	5	TRAUMA The patient visits a trauma center (A trauma center means a facility licensed or designated by the State or local government authority authorized to do so, or as verified by the American College of surgeons and involving a trauma activation.)
* ADMSN_TYPE_CD	9	UNKNOWN Information not available.

### questions to ask 1/27/23
* ---------------can we use the BLG_PRVDR_SPCLTY_CD code ?-----------------for OPRTG_PRVDR_NPI entity
*  SELECT DISTINCT
*        OPRTG_PRVDR_NPI as care_site_npi
*        , BLG_PRVDR_SPCLTY_CD as specialty_cd ---------------can we use the BLG_PRVDR_SPCLTY_CD code ?-----------------
*        ,'OPRTG_PRVDR_NPI' as npi_column
*        ,'IP' as source_domain
*      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
*      WHERE CHAR_LENGTH(trim(OPRTG_PRVDR_NPI)) > 0

#visit types from ( ip, lt, ot, rx)
* Emergency room - ER
* inpatient - IP
* Inpatient/Emergency - IP from ER
* Outpatient
* Long-term care
* rx- pharmacy visits
# CLM_TYPE_CD codes
* CLM_TYPE_CD	1	A Fee-For-Service Medicaid or Medicaid-expansion CHIP Claim
* CLM_TYPE_CD	2	Medicaid or Medicaid-expansion CHIP Capitated Payment
* CLM_TYPE_CD	3	Medicaid or Medicaid-expansion CHIP Managed Care Encounter record that simulates a bill for a service rendered to a patient covered under some form of Capitation Plan.  This includes billing records submitted by providers to non-state entities (e.g., MCOs, health plans) for which the State has no financial liability since the risk entity has already received a capitated payment from the State.
* CLM_TYPE_CD	4	Medicaid or Medicaid-expansion CHIP Service Tracking Claim
* CLM_TYPE_CD	5	Medicaid or Medicaid-expansion CHIP Supplemental Payment (above capitation fee or above negotiated rate) (e.g., FQHC additional reimbursement)
* CLM_TYPE_CD	A	Separate CHIP (Title XXI) claim: A Fee-for-Service Claim
* CLM_TYPE_CD	B	Separate CHIP (Title XXI) claim: Capitated Payment
* CLM_TYPE_CD	C	Separate CHIP (Title XXI) managed care encounter record that simulates a bill for a service or items rendered to a patient covered under some form of Capitation Plan.
*     This includes billing records submitted by providers to non-State entities (e.g., MCOs, health plans) for which a state has no financial liability as the at-risk entity has already received a capitated payment from the state
* CLM_TYPE_CD	D	Separate CHIP (Title XXI) Service Tracking Claim
* CLM_TYPE_CD	E	Separate CHIP (Title XXI) claim for a supplemental payment (above capitation fee or above negotiated rate) (e.g., FQHC additional reimbursement)
* CLM_TYPE_CD	U	Other FFS claim
* CLM_TYPE_CD	V	Other Capitated Payment
* CLM_TYPE_CD	W	Other Managed Care Encounter
* CLM_TYPE_CD	X	Non-Medicaid/CHIP service tracking claims
* CLM_TYPE_CD	Y	Other Supplemental Payment
* CLM_TYPE_CD	Z	Denied claims

# LT source file visit concept
## BILL_TYPE_CD
*        ---- we will need to build out the admission type code, if the third digit of the BILL_TYPE_CD is
*       /*	3rd Digit-Bill Classification (Except Clinics and Special Facilities)
* 9201 = inpatient visit = 1	Inpatient or  2	Inpatient/ == 9201 inpatient visit
* 9201 = inpatient if BILL_TYPE_CD 3rd digit is =  8	Swing Bed (may be used to indicate billing for SNF level of care in a hospital with an approved swing bed agreement). - is swing bed inpatient visit?
* 9202 = outpatient visit = 3	Outpatient or /4	Other or /5	Intermediate Care - Level I or/ 6	Intermediate Care - Level II / 7	Reserved for national assignment (discontinued effective 10/1/05).
* 9202  = 9	Reserved for National Assignment*/


# RX
** PRSCRBD_DT or MDCD_PD_DT - which should be start date?
**

## IP/LT/OT - This code indicates the type of Medicare reimbursement.
* MDCR_REIMBRSMT_TYPE_CD	01	IPPS - Acute Inpatient PPS
* MDCR_REIMBRSMT_TYPE_CD	02	LTCHPPS - Long-term Care Hospital PPS
* MDCR_REIMBRSMT_TYPE_CD	03	SNFPPS - Skilled Nursing Facility PPS
* MDCR_REIMBRSMT_TYPE_CD	04	HHPPS - Home Health PPS
* MDCR_REIMBRSMT_TYPE_CD	05	IRFPPS - Inpatient Rehabilitation Facility PPS
* MDCR_REIMBRSMT_TYPE_CD	06	IPFPPS - Inpatient Psychiatric Facility PPS
* MDCR_REIMBRSMT_TYPE_CD	07	OPPS - Outpatient PPS
* MDCR_REIMBRSMT_TYPE_CD	08	Fee Schedules (for physicians, DME, ambulance, and clinical lab)
* MDCR_REIMBRSMT_TYPE_CD	09	Part C Hierarchical Condition Category Risk Assessment (CMS-HCC RA) Capitation Payment Model



* rx, if MDCD_PD_AMT is 0 what does that mean? do we still add this row?
