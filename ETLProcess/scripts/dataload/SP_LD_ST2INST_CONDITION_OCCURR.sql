/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_LD_ST2INST_CONDITION_OCCURR

Description : Load data from staging to instance

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_LD_ST2INST_CONDITION_OCCURR 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/********************************************************************************************************
     Name:      SP_LD_ST2INST_CONDITION_OCCURR
     Purpose:    Loading The CDMH_STAGING.st_omop53_condition_occurrence Table into N3C_Instance.condition_occurrence
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
CURSOR condition_occurrence_Cursor IS
SELECT 
CONDITION_OCCURRENCE_ID
,PERSON_ID
,CONDITION_CONCEPT_ID
,CONDITION_START_DATE
,CONDITION_START_DATETIME
,CONDITION_END_DATE
,CONDITION_END_DATETIME
,CONDITION_TYPE_CONCEPT_ID
,STOP_REASON
,PROVIDER_ID
,VISIT_OCCURRENCE_ID
,VISIT_DETAIL_ID
,CONDITION_SOURCE_VALUE
,CONDITION_SOURCE_CONCEPT_ID
,CONDITION_STATUS_SOURCE_VALUE
,CONDITION_STATUS_CONCEPT_ID
FROM CDMH_STAGING.ST_OMOP53_CONDITION_OCCURRENCE
WHERE DATA_PARTNER_ID=DATAPARTNERID AND MANIFEST_ID=MANIFESTID;
TYPE l_val_cur IS TABLE OF condition_occurrence_Cursor%ROWTYPE;
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
open condition_occurrence_Cursor;
  LOOP
    FETCH condition_occurrence_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO N3C_OMOP531_INSTANCE.condition_occurrence VALUES values_rec(i);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close condition_occurrence_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);


END SP_LD_ST2INST_CONDITION_OCCURR;
