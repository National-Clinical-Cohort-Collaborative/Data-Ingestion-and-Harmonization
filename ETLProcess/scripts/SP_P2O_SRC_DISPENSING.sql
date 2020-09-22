---------------------------------------------------------------------------
--  File : CDMH_STAGING.SP_P2O_SRC_DISPENSING  - Tuesday-September-22-2020   
---------------------------------------------------------------------------

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
