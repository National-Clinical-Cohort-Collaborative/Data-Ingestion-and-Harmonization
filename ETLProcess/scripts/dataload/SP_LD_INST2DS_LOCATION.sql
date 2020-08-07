/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_LD_INST2DS_LOCATION

Description : Load data from instance to DS

*************************************************************/
-- Unable to render PROCEDURE DDL for object CDMH_STAGING.SP_LD_INST2DS_LOCATION with DBMS_METADATA attempting internal generator.
CREATE PROCEDURE CDMH_STAGING.SP_LD_INST2DS_LOCATION 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/********************************************************************************************************
     Name:      SP_LD_INST2DS_LOCATION
     Purpose:    Loading The N3C_Instance.LOCATION Table into N3C_DS.LOCATION
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
CURSOR Location_Cursor IS
SELECT 
  LOCATION_ID
  ,ADDRESS_1
  ,ADDRESS_2
  ,CITY
  ,STATE
  ,ZIP
  ,COUNTY
  ,LOCATION_SOURCE_VALUE,
  DATAPARTNERID AS DATA_PARTNER_ID
FROM N3C_OMOP531_Instance.LOCATION ;
TYPE l_val_cur IS TABLE OF Location_Cursor%ROWTYPE;
values_rec l_val_cur;

BEGIN
/******************************************************
* DELETE PREVIOUS RUN DATA
******************************************************/
DELETE FROM N3C_OMOP531_DS.LOCATION WHERE DATA_PARTNER_ID=DATAPARTNERID;
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
open Location_Cursor;
  LOOP
    FETCH Location_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO N3C_OMOP531_DS.LOCATION VALUES values_rec(i);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close Location_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_LD_INST2DS_LOCATION;
