/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_LD_ST2INST_CONDITION_ERA

Description : Load data from staging to instance

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_LD_ST2INST_CONDITION_ERA 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER 
) AS 
/********************************************************************************************************
     Name:      SP_LOAD_ST2INST_CONDITION_ERA
     Purpose:    Loading The CDMH_STAGING.ST_OMOP53_Condition_era Table into N3C_omop531_Instance.Condition_era
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
CURSOR Condition_era_Cursor IS
SELECT 
CONDITION_ERA_ID
,PERSON_ID
,CONDITION_CONCEPT_ID
,CONDITION_ERA_START_DATE
,CONDITION_ERA_END_DATE
,CONDITION_OCCURRENCE_COUNT
FROM CDMH_STAGING.ST_OMOP53_Condition_era
WHERE DATA_PARTNER_ID=DATAPARTNERID AND MANIFEST_ID=MANIFESTID;
TYPE l_val_cur IS TABLE OF Condition_era_Cursor%ROWTYPE;
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
open Condition_era_Cursor;
  LOOP
    FETCH Condition_era_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO N3C_OMOP531_INSTANCE.condition_era VALUES values_rec(i);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close Condition_era_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);

END SP_LD_ST2INST_CONDITION_ERA;
