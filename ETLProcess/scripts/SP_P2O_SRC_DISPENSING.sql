
/**********************************************************************************
project : N3C DI&H
Date: 6/16/2020
Author: Richard Zhu / Tanner Zhang/ Stephanie Hong / Sandeep Naredla
Description : Stored Procedure to insert PCORnet Condition into staging table
Stored Procedure: SP_P2O_SRC_DISPENSING:
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER, RECORDCOUNT OUT NUMBER
Edit History::
6/16/2020 	SHONG Initial Version

***********************************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_P2O_SRC_DISPENSING 

(

  DATAPARTNERID IN NUMBER,

  MANIFESTID IN NUMBER,

  RECORDCOUNT OUT NUMBER

) AS

BEGIN

 

--death/death_cause/ with latest encounter using data and time and insert to OMOP observation

 

    RECORDCOUNT  := 0;

--    DBMS_OUTPUT.put_line(RECORDCOUNT || '  PCORnet DISPENSING source data inserted to xxx staging table, ST_OMOP53_xxx, successfully.');

 

 

END SP_P2O_SRC_DISPENSING;
