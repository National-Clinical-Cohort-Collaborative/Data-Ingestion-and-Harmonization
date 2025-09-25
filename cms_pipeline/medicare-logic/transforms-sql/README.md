
Useful OHDSI links:
https://github.com/OHDSI/ETL-CMS/blob/master/python_etl/CMS_SynPuf_ETL_CDM_v5.py
https://github.com/lhncbc/CRI/tree/master/EtlOmopMedicaid

General questions:
* other ETL references header/line files, any insight into this?

Input tables:

* IP = Inpatient
    * ICD10 and ICD10 PCS
* OP = Outpatient
    * ICD10 and ICD10 PCS
* Part D Drug Event
    * NDC
* HH = Home Health
* HS = Hospice
* DM = Durable Medical Equipment (e.g. Prosthetics, device_exposure is likely destination, some drugs in NDC field)
* PB = Part B = Carrier file
* SN = Skilled Nursing

General notes:
* BID is beneficiary primary key


## Care Site
* use provider number to build the care_site_id
 -- place of service ip inpatient/ erip or ip/ opl has 
  -- INPATIENT_CLAIMS:  8717-Inpatient Hospital/ Inpatient Facility
  -- OUTPATIENT_CLAIMS:  8756-Outpatient Hospital/ Outpatient Facility
  -- pb - CARRIER_CLAIMS:  8940-Office
   --hs - 8546 /Hospice Facility 
   -- sn 8863 / Skilled Nursing Facility
   -- hh 581476 / Home Visit
## Provider 
* one NPI can be associated with multiple specialty code and specialty descriptions
* npi	nnn	2021	C1	Centralized Flu Biller
* npi	nnn	2021	A5	Pharmacy
* npi	nnn	2021	69	Independent Clinical Laboratory

## Location

What is the granularity of location? - zip state and county

## Visit Occurrence

Acumen advise that the below approach is not how they would do it.
They do:

* IP
    * BID, ADMSN_DT, PROVIDER → then look for the latest THRU_DT (you might have multiple claims with same admission date, so choose the latest THRU_DT)
    * Identifying ER visits
        * Need to look at RVCNTRXX columns 
        * RVCNTRXX in 0450 0451 0452 0456 0459 0981
        * then take RVDTBLXX → but this seems to be 0 or null
* OP
    * doesn’t have an ADMSN_DT, do you use FROM_DT?
        * REV_DT → is 10% null, but then you shouldn’t trust these
    * Identifying ER visits: 
        * REV_CTR in 0450 0451 0452 0456 0459 0981
        * REV_PMT > 0
* PB
    * need to look in here for non-facility visits (I think this means to say a primary provider or a eye clinic or something)
    * has PLCSRVC
    * how do we deduplicate with IP/OP?


From: https://github.com/OHDSI/ETL-CMS/blob/master/python_etl/CMS_SynPuf_ETL_CDM_v5.py

* Inpatient:
    * unique FROM_DT, THRU_DT, PROVIDER(?), BID
* Outpatient
    * unique FROM_DT, THRU_DT, PROVIDER(?), BID
* Carrier claims
    * Not sure where this is

Where else do FROM_DT, THRU_DT appear?

* SN
* HH
* HS
* DM
* PB


## Person

From CMS_to_OMOP spreadsheet and SynPuf mapping we can conclude:

* BID → person_id?
* BENE_BIRTH_DT → 
    * month_of_birth = BENE_BIRTH_DT[0:4]
    * day_of_birth = BENE_BIRTH_DT[4:6]
    * year_of_birth = BENE_BIRTH_DT[6:8]
    * birth_datetime
* SEX_IDENT_CD → gender_source_value
    * 1 = MALE, 2 = FEMALE
* BENE_RACE_CD → race_source_value
    *     if int(yd.BENE_RACE_CD) == 1:    #White # race_concept_id and ethnicity_concept_id
                person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_WHITE))
                person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
            elif int(yd.BENE_RACE_CD) == 2:  #Black
                person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_BLACK))
                person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
            elif int(yd.BENE_RACE_CD) == 3:  #Others
                person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_OTHER))
                person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
            elif int(yd.BENE_RACE_CD) == 5:  #Hispanic
                person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_NON_WHITE))
                person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_HISPANIC))
* RTI_RACE_CD → race_source_value


What is missing?

* care_site_id
* location_id

## Payer plan period

## Location

## Drug Exposure

Column mappings from Part D Drug Event to drug_exposure:

* NDC_CD → drug_exposure_concept_id
* PROD_SERVICE_ID → drug_exposure_concept_id
    * this seems to be the equivalent to the SynPuf PROD_SRVC_ID
* X_DOS_DT → drug_exposure_start_date
* QUANTITY_DISPENSED → quantity
* DAYS_SUPPLY → days_supply
* drug_type_concept_id is constant: DRUG_TYPE_PRESCRIPTION = "38000175"



* Where do you get route information?
* What is the difference between NDC_CD and PROD_SERVICE_ID?
* *How do you derive drug_exposure_end_date?*
    * start_date + days_supply - 1? is days_supply always completed?

## Condition Occurrence


Notes:

* Condition records could come from any of IP/OP/HH/HS/SN? DM? PB?
* Target table of condition based on target concept id domain of ICD/HCPCS codes
* Which columns should we be looking in for diagnosis codes?
    * PDGNS_CD? This doesn’t seem to exist in the ‘blown out’ version
    * DGNSCD01-25
    * do we need to use E codes? doesn’t seem necessary


Column mapping:

* condition_occurrence_id
    * Generated
* condition_end_date = THRU_DT
* condition_end_datetime
* condition_start_date = FROM_DT
* condition_start_datetime
* person_id = BIC?
* provider_id
* stop_reason
* visit_detail_id
* visit_occurrence_id = Join on FROM_DT, THRU_DT, PROVIDER, BIC
* condition_source_value = diagnosis code columns
* condition_concept_id = Join on concept_relationship from diagnosis code columns
* condition_source_concept_id = Join on concept table from diagnosis code columns
* condition_status_source_value
* condition_status_concept_id
    * Is it possible to know this? We do have a “Claim Admitting Diagnosis Code (AD_DGNS)” column in the IP/SN tables
    * Admitting diagnosis: use concept_id 4203942
    * Preliminary diagnosis: use concept_id 4033240
    * Final diagnosis: use concept_id 4230359 – should also be used for ‘Discharge diagnosis’
* condition_type_concept_id
    * Seems to be related to primary/secondary diagnoses? Perhaps we can complete based on “Claim Principal Diagnosis Code (PDGNS_CD)” column


## Part B - Carrier file 

https://www.cms.gov/files/document/2019-part-b-carrier-readme-file.pdf

* Do we have claims from the same 'visit' in Part B + other files, e.g. IP or OP? If so, how do we identify this? How do we link them?
    * OHDSI mapping file uses TAX_NUM to identify unique visits - this seems wrong to me since multiple providers could bill separately for the same visit
