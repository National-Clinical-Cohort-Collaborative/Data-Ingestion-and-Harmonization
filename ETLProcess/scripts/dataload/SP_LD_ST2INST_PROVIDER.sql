/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_LD_ST2INST_PROVIDER

Description : Load data from staging to instance

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_LD_ST2INST_PROVIDER 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER 
) AS 
/********************************************************************************************************
     Name:      SP_LOAD_ST2INST_PROVIDER
     Purpose:    Loading The CDMH_STAGING.ST_OMOP53_PROVIDER Table into N3C_omop531_Instance.PROVIDER
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1        6/29/2020     SHONG             Initial Version
*********************************************************************************************************/
/******************************************************
*  CONSTANTs
******************************************************/
COMMIT_LIMIT CONSTANT NUMBER := 10000;
loop_count NUMBER;
insert_rec_count NUMBER;
/**************************************************************
*  Cursor for selecting table
**************************************************************/
CURSOR Provider_Cursor IS
SELECT 
PROVIDER_ID
,PROVIDER_NAME
,NPI
,DEA
,SPECIALTY_CONCEPT_ID
,CARE_SITE_ID
,YEAR_OF_BIRTH
,GENDER_CONCEPT_ID
,PROVIDER_SOURCE_VALUE
,SPECIALTY_SOURCE_VALUE
,SPECIALTY_SOURCE_CONCEPT_ID
,GENDER_SOURCE_VALUE
,GENDER_SOURCE_CONCEPT_ID
FROM CDMH_STAGING.ST_OMOP53_PROVIDER
WHERE DATA_PARTNER_ID=DATAPARTNERID AND MANIFEST_ID=MANIFESTID;
TYPE l_val_cur IS TABLE OF Provider_Cursor%ROWTYPE;
values_rec l_val_cur;

BEGIN

/**************************************************************
_  VARIABLES:
*  loop_count - counts loop iterations for COMMIT_LIMIT
**************************************************************/
   loop_count := 0;
   insert_rec_count := 0;
/******************************************************
* Beginning of loop on each record in cursor.
******************************************************/
open Provider_Cursor;
  LOOP
    FETCH Provider_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO N3C_OMOP531_INSTANCE.provider VALUES values_rec(i);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close Provider_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);


END SP_LD_ST2INST_PROVIDER;
