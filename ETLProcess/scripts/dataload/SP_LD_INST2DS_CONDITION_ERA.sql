/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_LD_INST2DS_CONDITION_ERA

Description : sp to load data from instance to DS

*************************************************************/

-- Unable to render PROCEDURE DDL for object CDMH_STAGING.SP_LD_INST2DS_CONDITION_ERA with DBMS_METADATA attempting internal generator.
CREATE PROCEDURE CDMH_STAGING.SP_LD_INST2DS_CONDITION_ERA 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/********************************************************************************************************
     Name:      SP_LD_INST2DS_CONDITION_ERA
     Purpose:    Loading The N3C_Instance.CONDITION_ERA Table into N3C_DS.CONDITION_ERA
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1        6/29/2020     SNAREDLA             Initial Version
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
CURSOR CE_Cursor IS
SELECT 
  CONDITION_ERA_ID,
  PERSON_ID,
  CONDITION_CONCEPT_ID,
  CONDITION_ERA_START_DATE,
  CONDITION_ERA_END_DATE,
  CONDITION_OCCURRENCE_COUNT,
  DATAPARTNERID AS DATA_PARTNER_ID
FROM N3C_OMOP531_Instance.CONDITION_ERA ;
TYPE l_val_cur IS TABLE OF CE_Cursor%ROWTYPE;
values_rec l_val_cur;

BEGIN
/******************************************************
* DELETE PREVIOUS RUN DATA
******************************************************/
DELETE FROM N3C_OMOP531_DS.CONDITION_ERA WHERE DATA_PARTNER_ID=DATAPARTNERID;
COMMIT;
/**************************************************************
_  VARIABLES:
*  loop_count - counts loop iterations for COMMIT_LIMIT
**************************************************************/
   loop_count := 0;
   insert_rec_count := 0;
/******************************************************
* Beginning of loop on each record in cursor.
******************************************************/
open CE_Cursor;
  LOOP
    FETCH CE_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO N3C_OMOP531_DS.CONDITION_ERA VALUES values_rec(i);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close CE_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_LD_INST2DS_CONDITION_ERA;
