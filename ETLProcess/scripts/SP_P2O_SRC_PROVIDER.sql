/***********************************************************************************************************
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
FILE:           SP_P2O_SRC_PROVIDER.sql
Description :   Loading NATIVE_PCORNET51_CDM.PROVIDER table into stging table SP_P2O_SRC_PROVIDER
Procedure:      SP_P2O_SRC_PROVIDER
Edit History:
     Ver       Date         Author          Description
     0.1       5/16/2020    SHONG           Initial version
 
*************************************************************************************************************/

CREATE PROCEDURE CDMH_STAGING.SP_P2O_SRC_PROVIDER 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 
BEGIN
  
  INSERT INTO CDMH_STAGING.ST_OMOP53_PROVIDER
  ( DATA_PARTNER_ID,
  MANIFEST_ID,
  PROVIDER_ID,
  PROVIDER_NAME,
  NPI,
  DEA,
  SPECIALTY_CONCEPT_ID,
  CARE_SITE_ID,
  YEAR_OF_BIRTH,
  GENDER_CONCEPT_ID,
  PROVIDER_SOURCE_VALUE,
  SPECIALTY_SOURCE_VALUE,
  SPECIALTY_SOURCE_CONCEPT_ID,
  GENDER_SOURCE_VALUE,
  GENDER_SOURCE_CONCEPT_ID,
  DOMAIN_SOURCE
)

  SELECT 
  DATAPARTNERID AS DATA_PARTNER_ID,
  MANIFESTID AS MANIFEST_ID,
  mp.N3cds_Domain_Map_Id AS PROVIDER_ID, 
  NULL AS PROVIDER_NAME,
  NULL AS NPI,
  NULL AS DEA,
  NULL AS SPECIALTY_CONCEPT_ID,
  NULL AS CARE_SITE_ID,
  NULL AS YEAR_OF_BIRTH,
  gx.TARGET_CONCEPT_ID AS gender_concept_id, 
  NULL AS PROVIDER_SOURCE_VALUE,
  PROVIDER_SPECIALTY_PRIMARY AS specialty_source_value ,
  NULL AS SPECIALTY_SOURCE_CONCEPT_ID,
  PROVIDER_SEX AS gender_source_value, 
  NULL AS GENDER_SOURCE_CONCEPT_ID,
  'PCORNET-Provider' AS DOMAIN_SOURCE

  FROM "NATIVE_PCORNET51_CDM"."PROVIDER" 
  JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id=PROVIDER.PROVIDERID 
                                          AND Mp.Domain_Name='PROVIDER'
                                          AND mp.DATA_PARTNER_ID=DATAPARTNERID
  LEFT JOIN CDMH_STAGING.Gender_Xwalk gx on gx.CDM_TBL='DEMOGRAPHIC'
                                          AND Gx.Src_Gender=PROVIDER.PROVIDER_SEX
                                          ;
  RECORDCOUNT  := sql%rowcount;
    
  DBMS_OUTPUT.put_line(RECORDCOUNT || ' PCORnet PROVIDER source data inserted to staging table, ST_OMOP53_PROVIDER, successfully.'); 
--


  commit ;                                        
END SP_P2O_SRC_PROVIDER;
