/*******************************************************************************************************
project : N3C DI&H
Date: 6/16/2020
Author: Stephanie Hong / Sandeep Naredla / Richard Zhu / Tanner Zhang
Description : Stored Procedure to insert PCORnet Condition into staging table
Stored Procedure: SP_P2O_SRC_DEATH_CAUSE:
Parameters: DATAPARTNERID IN NUMBER, MANIFESTID IN NUMBER, RECORDCOUNT OUT NUMBER
********************************************************************************************************/

CREATE PROCEDURE CDMH_STAGING.SP_P2O_SRC_DEATH_CAUSE 

(

  DATAPARTNERID IN NUMBER,

  MANIFESTID IN NUMBER,

  RECORDCOUNT OUT NUMBER

) AS

BEGIN

 

--death/death_cause/ with latest encounter using data and time and insert to OMOP observation

 

    RECORDCOUNT  := 0;

    DBMS_OUTPUT.put_line(RECORDCOUNT || '  PCORnet DEATH_CAUSE source data inserted to observation staging table, ST_OMOP53_Observation, successfully.');

 

 

END SP_P2O_SRC_DEATH_CAUSE;
