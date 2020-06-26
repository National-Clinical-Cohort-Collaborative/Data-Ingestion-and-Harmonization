/*******************************************************************************************************************************
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong, Tanner Zhang, Richard Zhu, Sandeep Naredla
Description : Stored Procedure to insert PCORnet PROCEDURES into staging table
Stored Procedure: SP_P2O_SRC_DEATH
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER, RECORDCOUNT OUT NUMBER
Edit History:
0.1 6/18/2020 Stephanie Hong - removed the not null requirement on the OMOP Death table in order to insert death records without DOD. 

*********************************************************************************************************************************/
CREATE PROCEDURE              CDMH_STAGING.SP_P2O_SRC_DEATH 

(
DATAPARTNERID IN NUMBER
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS
death_recordCount number;
BEGIN
   --execute immediate 'truncate table CDMH_STAGING.ST_OMOP53_DEATH';
   INSERT INTO CDMH_STAGING.ST_OMOP53_DEATH ( DATA_PARTNER_ID,
  MANIFEST_ID,
  PERSON_ID,
  DEATH_DATE,
  DEATH_DATETIME,
  DEATH_TYPE_CONCEPT_ID,
  CAUSE_CONCEPT_ID,
  CAUSE_SOURCE_VALUE,
  CAUSE_SOURCE_CONCEPT_ID,
  DOMAIN_SOURCE
)--9 items

   SELECT
   DATAPARTNERID as DATA_PARTNER_ID,
   MANIFESTID as MANIFEST_ID,
   p.N3cds_Domain_Map_Id AS PERSON_ID,
   d.DEATH_DATE as DEATH_DATE,
   null as DEATH_DATETIME,-- should this be the same value as d.DEATH_DATE
   --d.DEATH_DATE as DEATH_DATETIME,
   dt.TARGET_CONCEPT_ID as DEATH_TYPE_CONCEPT_ID,
   cs.TARGET_CONCEPT_ID as CAUSE_CONCEPT_ID,  -- this field is number, ICD codes don't fit
   dc.DEATH_CAUSE as CAUSE_SOURCE_VALUE,--put raw ICD10 codes here, it fits the datatype -VARCHAR, and is useful for downstream analytics
   null as CAUSE_SOURCE_CONCEPT_ID -- this field is number, ICD codes don't fit
   ,'PCORNET_DEATH' AS DOMAIN_SOURCE

FROM NATIVE_PCORNET51_CDM.Death d

JOIN CDMH_STAGING.N3cds_Domain_Map p on p.Source_Id=d.PATID AND p.Domain_Name='PERSON' AND p.DATA_PARTNER_ID=DATAPARTNERID 
LEFT JOIN NATIVE_PCORNET51_CDM.DEATH_CAUSE dc on dc.PATID=d.PATID
LEFT JOIN CDMH_STAGING.p2o_death_term_xwalk dt on dt.cdm_tbl='DEATH' AND dt.cdm_column_name='DEATH_SOURCE' AND dt.src_code=d.DEATH_SOURCE
LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard cs on cs.cdm_tbl ='DEATH_CAUSE' and dc.DEATH_CAUSE_CODE = cs.SRC_CODE_TYPE and dc.DEATH_CAUSE = cs.SOURCE_CODE
;
death_recordCount:=SQL%ROWCOUNT;
commit;
RECORDCOUNT:=death_recordCount;
DBMS_OUTPUT.put_line(RECORDCOUNT || ' PCORnet DEATH source data inserted to staging table, ST_OMOP53_DEATH, successfully.'); 
                                       
END SP_P2O_SRC_DEATH;
