/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_LD_INST2DS_PROCEDURE_OCCURR

Description : Load data from instance to DS

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_LD_INST2DS_PROCEDURE_OCCURR 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/********************************************************************************************************
     Name:      SP_LD_INST2DS_PROCEDURE_OCCURR 
     Purpose:    Loading The N3C_Instance.Procedure_occurrence Table into N3C_DS.Procedure_occurrence
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
CURSOR PO_Cursor IS
SELECT 
  PROCEDURE_OCCURRENCE_ID
  ,PERSON_ID
  ,PROCEDURE_CONCEPT_ID
  ,PROCEDURE_DATE
  ,PROCEDURE_DATETIME
  ,PROCEDURE_TYPE_CONCEPT_ID
  ,MODIFIER_CONCEPT_ID
  ,QUANTITY
  ,PROVIDER_ID
  ,VISIT_OCCURRENCE_ID
  ,VISIT_DETAIL_ID
  ,PROCEDURE_SOURCE_VALUE
  ,PROCEDURE_SOURCE_CONCEPT_ID
  ,MODIFIER_SOURCE_VALUE,
  DATAPARTNERID AS DATA_PARTNER_ID
FROM N3C_OMOP531_Instance.Procedure_occurrence ;
TYPE l_val_cur IS TABLE OF PO_Cursor%ROWTYPE;
values_rec l_val_cur;

BEGIN
/******************************************************
* DELETE PREVIOUS RUN DATA
******************************************************/
DELETE FROM N3C_OMOP531_DS.Procedure_occurrence WHERE DATA_PARTNER_ID=DATAPARTNERID;
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
open PO_Cursor;
  LOOP
    FETCH PO_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO N3C_OMOP531_DS.Procedure_occurrence VALUES values_rec(i);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close PO_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_LD_INST2DS_PROCEDURE_OCCURR ;
