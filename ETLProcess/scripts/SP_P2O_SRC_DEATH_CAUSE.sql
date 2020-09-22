
CREATE PROCEDURE              CDMH_STAGING.SP_P2O_SRC_DEATH_CAUSE 

(

  DATAPARTNERID IN NUMBER,

  MANIFESTID IN NUMBER,

  RECORDCOUNT OUT NUMBER

) AS
/********************************************************************************************************
project : N3C DI&H
     Name:      SP_P2O_SRC_DEATH_CAUSE
     Purpose:    
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1         5/16/2020 SHONG Initial Version
********************************************************************************************************/
BEGIN

 

--death/death_cause/ with latest encounter using data and time and insert to OMOP observation

 

    RECORDCOUNT  := 0;

    DBMS_OUTPUT.put_line(RECORDCOUNT || '  PCORnet DEATH_CAUSE source data inserted to observation staging table, ST_OMOP53_Observation, successfully.');

 

 

END SP_P2O_SRC_DEATH_CAUSE;
