--------------------------------------------------------
-- PROJECT : N3C
-- 
-- CDM data source: PCORnet 
-- Description: insert to PCORnet obs_gen to target domain staging table 
-- File: SP_P2O_SRC_OBS_GEN.sql 
-- PROCEDURE: SP_P2O_SRC_OBS_GEN
--
-- Author: Stephanie Hong
-- Edit History: 
-- Date: 	Author: 	Description:
-- 6/1/2020 Shong	    Initial Version
--
--------------------------------------------------------


CREATE PROCEDURE CDMH_STAGING.SP_P2O_SRC_OBS_GEN 

(

  DATAPARTNERID IN NUMBER,

  MANIFESTID IN NUMBER,

  RECORDCOUNT OUT NUMBER

) AS

BEGIN

 

--death/death_cause/ with latest encounter using data and time and insert to OMOP observation

 

    RECORDCOUNT  := 0;

--    DBMS_OUTPUT.put_line(RECORDCOUNT || '  PCORnet DISPENSING source data inserted to xxx staging table, ST_OMOP53_xxx, successfully.');

 

 

END SP_P2O_SRC_OBS_GEN;
