/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_LD_INST2DS_PAYER_PLAN_PD

Description : Load data from instance to DS

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_LD_INST2DS_PAYER_PLAN_PD 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/********************************************************************************************************
     Name:      SP_LD_INST2DS_PAYER_PLAN_PD
     Purpose:    Loading The N3C_Instance.PAYER_PLAN_PERIOD Table into N3C_DS.PAYER_PLAN_PERIOD
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
CURSOR PayerP_Cursor IS
SELECT PAYER_PLAN_PERIOD_ID,
  PERSON_ID,
  PAYER_PLAN_PERIOD_START_DATE,
  PAYER_PLAN_PERIOD_END_DATE,
  PAYER_CONCEPT_ID,
  PAYER_SOURCE_VALUE,
  PAYER_SOURCE_CONCEPT_ID,
  PLAN_CONCEPT_ID,
  PLAN_SOURCE_VALUE,
  PLAN_SOURCE_CONCEPT_ID,
  SPONSOR_CONCEPT_ID,
  SPONSOR_SOURCE_VALUE,
  SPONSOR_SOURCE_CONCEPT_ID,
  FAMILY_SOURCE_VALUE,
  STOP_REASON_CONCEPT_ID,
  STOP_REASON_SOURCE_VALUE,
  STOP_REASON_SOURCE_CONCEPT_ID,
  DATAPARTNERID AS DATA_PARTNER_ID
FROM N3C_OMOP531_Instance.PAYER_PLAN_PERIOD ;
TYPE l_val_cur IS TABLE OF PayerP_Cursor%ROWTYPE;
values_rec l_val_cur;

BEGIN
/******************************************************
* DELETE PREVIOUS RUN DATA
******************************************************/
DELETE FROM N3C_OMOP531_DS.PAYER_PLAN_PERIOD WHERE DATA_PARTNER_ID=DATAPARTNERID;
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
open PayerP_Cursor;
  LOOP
    FETCH PayerP_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO N3C_OMOP531_DS.PAYER_PLAN_PERIOD VALUES values_rec(i);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close PayerP_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_LD_INST2DS_PAYER_PLAN_PD;
