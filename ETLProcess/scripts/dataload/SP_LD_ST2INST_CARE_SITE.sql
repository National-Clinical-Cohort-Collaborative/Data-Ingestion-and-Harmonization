/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_LD_ST2INST_CARE_SITE

Description : Load data from staging to instance

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_LD_ST2INST_CARE_SITE 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/********************************************************************************************************
     Name:      SP_LD_ST2INST_CARE_SITE
     Purpose:    Loading The CDMH_STAGING.CARE_SITE Table into N3C_Instance.CARE_SITE
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1        6/29/2020     SHONG            Initial Version
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
CURSOR CARE_SITE_Cursor IS
SELECT 
CARE_SITE_ID
,CARE_SITE_NAME
,PLACE_OF_SERVICE_CONCEPT_ID
,LOCATION_ID
,CARE_SITE_SOURCE_VALUE
,PLACE_OF_SERVICE_SOURCE_VALUE

FROM CDMH_STAGING.ST_OMOP53_CARE_SITE
WHERE DATA_PARTNER_ID=DATAPARTNERID AND MANIFEST_ID=MANIFESTID;
TYPE l_val_cur IS TABLE OF CARE_SITE_Cursor%ROWTYPE;
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
open CARE_SITE_Cursor;
  LOOP
    FETCH CARE_SITE_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO N3C_OMOP531_INSTANCE.CARE_SITE VALUES values_rec(i);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;

COMMIT;
Close CARE_SITE_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);

END SP_LD_ST2INST_CARE_SITE;
