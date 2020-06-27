/***********************************************************************************************************
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
FILE:           SP_P2O_SRC_PRO_CM.sql
Description :   Loading NATIVE_PCORNET51_CDM.pro_cm table into stging table 
Procedure:      SP_P2O_SRC_PRO_CM
Edit History:
     Ver       Date         Author          Description
     0.1       5/16/2020    SHONG           Initial version
 
*************************************************************************************************************/

CREATE PROCEDURE CDMH_STAGING.SP_P2O_SRC_PRO_CM 

(

  DATAPARTNERID IN NUMBER,

  MANIFESTID IN NUMBER,

  RECORDCOUNT OUT NUMBER

) AS

BEGIN

 

--death/death_cause/ with latest encounter using data and time and insert to OMOP observation

 

    RECORDCOUNT  := 0;

--    DBMS_OUTPUT.put_line(RECORDCOUNT || '  PCORnet DISPENSING source data inserted to xxx staging table, ST_OMOP53_xxx, successfully.');

 

 

END SP_P2O_SRC_PRO_CM;
