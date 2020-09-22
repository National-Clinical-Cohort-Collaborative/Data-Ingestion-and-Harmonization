
CREATE PROCEDURE                           CDMH_STAGING.SP_P2O_SRC_DEATH 

(
DATAPARTNERID IN NUMBER
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS
/********************************************************************************************************
project : N3C DI&H
     Name:      SP_P2O_SRC_DEATH
     Purpose:    Loading The NATIVE_PCORNET51_CDM.DEATH  Table into ST_OMOP53_DEATH
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1         5/16/2020 SHONG Initial Version
******************************************************************************************************/
death_recordCount number;
BEGIN
   DELETE FROM CDMH_STAGING.ST_OMOP53_DEATH WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_DEATH';
   COMMIT;
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
JOIN CDMH_STAGING.PERSON_CLEAN pc on d.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
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
