CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/medicaid_valueset_xwalk` AS
    
   with gender_xwalk as (
     SELECT distinct 
    'de_base' as domain,
    'SEX_CD' as column_name, 
    base.SEX_CD as src_code,
    case when base.SEX_CD = 'M' then 'Male'
         when base.SEX_CD = 'F' then 'Female'
         else 0
         end as src_code_name,
    'Gender' as src_vocab_id,  
        0 as source_concept_id,
    case when base.SEX_CD = 'M' then 8507 --male
         when base.SEX_CD = 'F' then 8532 --female
         else 0
         end as target_concept_id
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` base
   ),
   race_xwalk as (
       --RACE_ETHNCTY_CD
/* RACE_ETHNCTY_CD	1	White, non-Hispanic
RACE_ETHNCTY_CD	2	Black, non-Hispanic
RACE_ETHNCTY_CD	3	Asian, non-Hispanic
RACE_ETHNCTY_CD	4	American Indian and Alaska Native (AIAN), non-Hispanic
RACE_ETHNCTY_CD	5	Hawaiian/Pacific Islander
RACE_ETHNCTY_CD	6	Multiracial, non-Hispanic
RACE_ETHNCTY_CD	7	Hispanic, all races
----*/
 SELECT distinct 
    'de_base' as domain,
    'RACE_ETHNCTY_CD' as column_name, 
    base.RACE_ETHNCTY_CD as src_code,
    case when base.RACE_ETHNCTY_CD = 1 then 'White'
         when base.RACE_ETHNCTY_CD = 2 then 'Black'
         when base.RACE_ETHNCTY_CD = 0 then 'Unknown'
         when base.RACE_ETHNCTY_CD = 3 then 'Asian'
         when base.RACE_ETHNCTY_CD = 4 then 'American Indian and Alaska Native (AIAN)'
         when base.RACE_ETHNCTY_CD = 5 then 'Hawaiian/Pacific Islander'
         when base.RACE_ETHNCTY_CD = 6 then 'multi-racial' 
           when base.RACE_ETHNCTY_CD = 7 then cast( null as string) 
         else cast(null as string)
         end as src_code_name,
    'Race' as src_vocab_id,  
    0 as source_concept_id,
   case when base.RACE_ETHNCTY_CD = 1 then 8527 --white
         when base.RACE_ETHNCTY_CD = 2 then 8516 --black
         when base.RACE_ETHNCTY_CD = 0 then 0 --Unknown
         when base.RACE_ETHNCTY_CD = 3 then 8515 -- asian
         when base.RACE_ETHNCTY_CD = 4 then 8657 --8657/American Indian and Alaska Native (AIAN), non-Hispanic
         when base.RACE_ETHNCTY_CD = 5 then 8557 --5/ Hawaiian/Pacific Islander
         when base.RACE_ETHNCTY_CD = 6 then 4212311	--multi-racial 
           when base.RACE_ETHNCTY_CD = 7 then cast( null as string) --not a race code
         else cast( null as string)
         end as target_concept_id 

    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/de_base` base

   ), 
   /*
    ADMSN_TYPE_CD	1	EMERGENCY The patient requires immediate medical intervention as a result of severe, life threatening or potentially disabling conditions. Generally, the patient is admitted through the emergency room.
    ADMSN_TYPE_CD	2	URGENT The patient requires immediate attention for the care and treatment of a physical or mental disorder. Generally, the patient is admitted to the first available and suitable accommodation.
    ADMSN_TYPE_CD	3	ELECTIVE The patients condition permits adequate time to schedule the availability of a suitable accommodation.
    ADMSN_TYPE_CD	4	NEWBORN The patient is a newborn delivered either inside the admitting hospital (UB04 FL 15 value 5 [A baby born inside the admitting hospital] or outside of the hospital (UB04 FL 15 value 6 [A baby born outside the admitting hospital]).
    ADMSN_TYPE_CD	5	TRAUMA The patient visits a trauma center (A trauma center means a facility licensed or designated by the State or local government authority authorized to do so, or as verified by the American College of surgeons and involving a trauma activation.)
    ADMSN_TYPE_CD	9	UNKNOWN Information not available. 
   */
--- CMS Place of Service ( 1EMERGENCY=>23Emergency Room - Hospital/ 2URGENT=>20Urgent Care Facility, 3ELECTIVE = 22Outpatient Hospital,4NEWBORN=25BIRTHING ,5, 9 UNKNOWN)
   admission_type_xwalk as (
    SELECT distinct 
    'ip' as domain,
    'ADMSN_TYPE_CD' as column_name, 
    ip.ADMSN_TYPE_CD as src_code,
    case when ip.ADMSN_TYPE_CD = 1 then 'Emergency' 
         when ip.ADMSN_TYPE_CD = 2 then 'Urgent care facility'
         when ip.ADMSN_TYPE_CD = 3 then 'Elective visit/ outpatient visit'
         when ip.ADMSN_TYPE_CD = 4 then 'new born visit = newborn delived in admitting hospital inpatient'
         when ip.ADMSN_TYPE_CD = 5 then 'trauma/ emergency visit'
         when ip.ADMSN_TYPE_CD = 9 then 'unknown /0 visit type'
         else 0
         end as src_code_name,
    'medicaid admission type code' as src_vocab_id,  
    0 as source_concept_id,
    case when ip.ADMSN_TYPE_CD = 1 then 8870-- Emergency 
         when ip.ADMSN_TYPE_CD = 2 then 8782 --Urgent Care Facility
         when ip.ADMSN_TYPE_CD = 3 then 8756 --elective visit/ outpatient visit, Outpatient Hospital
         when ip.ADMSN_TYPE_CD = 4 then 8650 --Birthing Center new born visit = newborn delived in admitting hospital inpatient
         when ip.ADMSN_TYPE_CD = 5 then 581381 -- 5=trauma/ emergency -Emergency Room Critical Care Facility
         when ip.ADMSN_TYPE_CD = 9 then 0 -- 9=unknown /0 visit type
         else 0
         end as target_concept_id 
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/ip` ip
   ),
   medicare_reimbursement_type_code as 
   (
     SELECT distinct 
    'ip' as domain,
    'MDCR_REIMBRSMT_TYPE_CD' as column_name, 
    ip.MDCR_REIMBRSMT_TYPE_CD as src_code,
    case when ip.MDCR_REIMBRSMT_TYPE_CD = '01' then 'IPPS - Acute Inpatient PPS'-- emergency 
         when ip.MDCR_REIMBRSMT_TYPE_CD = '02' then 'LTCHPPS - Long-term Care Hospital PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '03' then  'SNFPPS - Skilled Nursing Facility PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '04' then 'HHPPS - Home Health PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '05' then 'IRFPPS - Inpatient Rehabilitation Facility PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '06' then 'IPFPPS - Inpatient Psychiatric Facility PPS' 
         when ip.MDCR_REIMBRSMT_TYPE_CD = '07' then 'OPPS - Outpatient PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '08' then 'Fee Schedules (for physicians, DME, ambulance, and clinical lab)' 
         when ip.MDCR_REIMBRSMT_TYPE_CD = '09' then 'Part C Hierarchical Condition Category Risk Assessment (CMS-HCC RA) Capitation Payment Model' 
         else 0
         end as src_code_name,
    'MDCR_REIMBRSMT_TYPE_CD' as src_vocab_id,  
    0 as source_concept_id,
   case when ip.MDCR_REIMBRSMT_TYPE_CD = '01' then 9201 ---'IPPS - Acute Inpatient PPS'-- emergency 
         when ip.MDCR_REIMBRSMT_TYPE_CD = '02' then 4137296 ---'LTCHPPS - Long-term Care Hospital PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '03' then 4164912 --- 'SNFPPS - Skilled Nursing Facility PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '04' then 38004519 ---'HHPPS - Home Health PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '05' then 8920 ---'IRFPPS - Inpatient Rehabilitation Facility PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '06' then 8971 ---'IPFPPS - Inpatient Psychiatric Facility PPS' 
         when ip.MDCR_REIMBRSMT_TYPE_CD = '07' then 8756 ---'OPPS - Outpatient PPS'
         when ip.MDCR_REIMBRSMT_TYPE_CD = '08' then 0 ---can be multiple including void -'Fee Schedules (for physicians( other = 32271) , DME(if UB04 type of 32392 then it can be a cancel claim), ambulance= 8668, and clinical lab = 38004294 or 32036)' 
         when ip.MDCR_REIMBRSMT_TYPE_CD = '09' then 0 ---'Part C Hierarchical Condition Category Risk Assessment (CMS-HCC RA) Capitation Payment Model' 
         else 0
         end as target_concept_id 
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/ip` ip

   )


   SELECT * FROM gender_xwalk
   union 
   SELECT * FROM race_xwalk
   union 
   select * from admission_type_xwalk
   union
   select * from medicare_reimbursement_type_code
   



