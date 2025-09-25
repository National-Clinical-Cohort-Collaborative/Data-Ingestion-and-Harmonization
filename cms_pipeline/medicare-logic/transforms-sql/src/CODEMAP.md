Useful OHDSI links:
https://github.com/OHDSI/ETL-CMS/blob/master/python_etl/CMS_SynPuf_ETL_CDM_v5.py
https://github.com/lhncbc/CRI/tree/master/EtlOmopMedicaid

General questions:
* other ETL references header/line files, any insight into this? acument.xls file and data dictionary.xls

code and code system from the following Input tables:

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

**ip dx codes
    ** ip PDGNS_CD -- Principal dx codes
    ** ip DGNS_E -- separate fields for external injury, only used in special cases.
    ** ip ADMSN_DT - admission date, required for inpatient claims
    ** AD_DGNS -- admitting dx code
    ** DSCHRGDT -- discharge date
    ** DGNSCD01-25 -- diagnosis code
    ** Parts A&B-DM, HH, HS, IP, OP, PB, SN	DGNSIND	NCH Diagnosis Trailer Indicator Code
    ** Parts A&B-DM, HH, HS, IP, OP, PB, SN	DVRSNCD	Claim Diagnosis Version Code	"Effective with Version 'J', the code used to indicate if the diagnosis code is ICD-9 or ICD-10.
    *** NOTE: With 5010, the diagnosis and procedure codes have been expanded to accommodate ICD-10, even though ICD-10 is not scheduled for implementation until 10/2014."
    ** Parts A&B-DM, HH, HS, IP, OP, PB, SN	DGNS_CD	Claim Diagnosis Code

** hcpcs codes
** Parts A&B-DM, HH, HS, IP, OP, PB, SN	HCPCS_CD	Line HCPCS Code	"The Health Care Common Procedure Coding System (HCPCS) is a collection of codes that represent procedures, supplies, products and services which may be provided to Medicare beneficiaries and to individuals enrolled in private health insurance programs. The codes are divided into three levels, or groups as described below:
** COMMENTS: Prior to Version H this line item field was named: HCPCS_CD. With Version H, a prefix was added to denote the location of this field on each claim type (institutional: REV_CNTR and noninstitutional: LINE).

** Level I Codes and descriptors copyrighted by the American Medical Association's Current Procedural Terminology, Fourth Edition (CPT-4). These are 5 position numeric codes representing physician and nonphysician services.

** NOTE: CPT-4 codes including both long and short descriptions shall be used in accordance with the
**  CMS/AMA agreement. Any other use violates the AMA copyright.

** Level II Includes codes and descriptors copyrighted by the American Dental Association's Current Dental 
** Terminology, Fifth Edition (CDT-5). These are 5 position alpha-numeric codes comprising the D series. All other level II codes and descriptors are approved and maintained jointly by the alpha-numeric editorial panel (consisting of CMS, the Health Insurance Association of America, and the Blue Cross and Blue Shield 
** Association). These are 5 position alphanumeric codes representing primarily items and nonphysician services that are not represented in the level I codes.

** Level III Codes and descriptors developed by Medicare carriers for use at the local (carrier) level. These are 5 position alpha-numeric codes in the W, X, Y or Z series representing physician and nonphysician services that are not represented in the level I or level II codes."
** Parts A&B-DM, HH, HS, IP, OP, PB, SN	MDFR_CD1	Line HCPCS Initial Modifier Code	A first modifier to the HCPCS procedure code to enable a more specific procedure identification for the line item service on the noninstitutional claim. 
** Parts A&B-DM, HH, HS, IP, OP, PB, SN	MDFR_CD2	Line HCPCS Second Modifier Code	A second modifier to the HCPCS procedure code to make it more specific than the first modifier code to identify the line item procedures for this claim. 
** Parts A&B-DM, HH, HS, IP, OP, PB, SN	MDFR_CD3	Line HCPCS Third Modifier Code	Prior to Version H this field was named: HCPCS_3RD_MDFR_CD.
** Parts A&B-DM, HH, HS, IP, OP, PB, SN	MDFR_CD4	Line HCPCS Fourth Modifier Code	Prior to Version H this field was named: HCPCS_4TH_MDFR_CD.
** DM
** Parts A&B-DM, PB	LINEDGNS	Line Diagnosis Code	The code indicating the diagnosis supporting this line item procedure/service on the noninstitutional claim.

** RVCNTR codes: https://resdac.org/sites/datadocumentation.resdac.org/files/Revenue%20Center%20Code%20Code%20Table%20FFS.txt 
* from Daniel Moran @ Acumen  5/5/2023
Chris, for all revenue code arrays, the basis of the array is formed around the revenue code. This provides the basic information of the service provided. 
The HCPCS may provide additional information about the service and the revenue date may provide information about the specific timing of the service. 
So certain revenue codes are expected to have a revenue date and a HCPCS service while others would likely have neither. This also varies by setting. 
Outpatient will regularly have HCPCS and revenue dates because its paid closer to the line level. SNF services are more general and span the entire claim 
so often miss both HCPCS and date. A few examples:
0001 is an accounting line for total charges and won’t have a HCPCS or revenue date
0022 marks the field as including a HIPPS code for SNF. The HIPPS code will be in the HCPCS field and the revenue date the initiation of that HIPPS level
0120 represents supplying a room for the claim. This service needs neither a revenue date nor HCPCS to provide additional information
0450 represents emergency room services in, for example, outpatient services. This is expected to have HCPCS and revenue date indicating the specific 
      service and date
So I would not exclude lines missing a HCPCS or revenue date. They can contain important information even if they lack specifics. If you want to identify 
specific pieces such as HIPPS, I would identify them through the revenue codes. A full list of codes can be found here:
https://resdac.org/cms-data/variables/revenue-center-code-ffs

