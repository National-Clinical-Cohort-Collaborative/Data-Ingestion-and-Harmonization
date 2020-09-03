/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_DM_PCORNET_DEMOGRAPHIC

Description : insert to N3CDS_DOMAIN_MAP

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_DM_PCORNET_DEMOGRAPHIC 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/******************************************************
*  CONSTANTs
******************************************************/
COMMIT_LIMIT CONSTANT NUMBER := 10000;
loop_count NUMBER;
insert_rec_count NUMBER;
/**************************************************************
*  Cursor for selecting table
**************************************************************/
CURSOR ResultSet_Cursor IS
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
PERSON_ID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
'PERSON' AS DOMAIN_NAME, 
NULL AS TARGET_DOMAIN_ID, 
NULL AS TARGET_CONCEPT_ID 
FROM "CDMH_STAGING"."PERSON_CLEAN"   
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on PERSON_CLEAN.PERSON_ID=Mp.Source_Id  
                                    AND mp.DOMAIN_NAME='PERSON' 
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID    
WHERE PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID AND
mp.N3CDS_DOMAIN_MAP_ID IS NULL
UNION ALL 
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
PERSON_ID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
'DEMOGRAPHIC' AS DOMAIN_NAME, 
'Observation' AS TARGET_DOMAIN_ID, 
4152283 AS TARGET_CONCEPT_ID 
FROM "CDMH_STAGING"."PERSON_CLEAN"   
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on PERSON_CLEAN.PERSON_ID=Mp.Source_Id  
                                    AND mp.DOMAIN_NAME='DEMOGRAPHIC' 
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID   
                                    AND mp.TARGET_DOMAIN_ID='Observation' 
                                    AND Mp.Target_Concept_Id=4152283 
WHERE PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID AND
mp.N3CDS_DOMAIN_MAP_ID IS NULL 
UNION ALL 
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
PERSON_ID AS SOURCE_ID,
SYSDATE AS CREATE_DATE,
'DEMOGRAPHIC' AS DOMAIN_NAME, 
'Observation' AS TARGET_DOMAIN_ID, 
4283657 AS TARGET_CONCEPT_ID 
FROM "CDMH_STAGING"."PERSON_CLEAN"   
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on PERSON_CLEAN.PERSON_ID=Mp.Source_Id  
                                    AND mp.DOMAIN_NAME='DEMOGRAPHIC' 
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID   
                                    AND mp.TARGET_DOMAIN_ID='Observation' 
                                    AND Mp.Target_Concept_Id=4283657 
WHERE PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID AND
mp.N3CDS_DOMAIN_MAP_ID IS NULL 
UNION ALL 
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
PERSON_ID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
'DEMOGRAPHIC' AS DOMAIN_NAME, 
'Observation' AS TARGET_DOMAIN_ID, 
4110772 AS TARGET_CONCEPT_ID 
FROM "CDMH_STAGING"."PERSON_CLEAN"   
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on PERSON_CLEAN.PERSON_ID=Mp.Source_Id  
                                    AND mp.DOMAIN_NAME='DEMOGRAPHIC' 
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID   
                                    AND mp.TARGET_DOMAIN_ID='Observation' 
                                    AND Mp.Target_Concept_Id=4110772 
WHERE PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID AND
mp.N3CDS_DOMAIN_MAP_ID IS NULL

UNION ALL
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
PERSON_ID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
'DEMOGRAPHIC' AS DOMAIN_NAME, 
'OBSERVATION_PERIOD' AS TARGET_DOMAIN_ID, 
44814724 AS TARGET_CONCEPT_ID 
FROM "CDMH_STAGING"."PERSON_CLEAN"   
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on PERSON_CLEAN.PERSON_ID=Mp.Source_Id  
                                    AND mp.DOMAIN_NAME='DEMOGRAPHIC' 
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID   
                                    AND mp.TARGET_DOMAIN_ID='OBSERVATION_PERIOD' 
                                    AND mp.TARGET_CONCEPT_ID=44814724
WHERE PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID AND
mp.N3CDS_DOMAIN_MAP_ID IS NULL
;
TYPE l_val_cur IS TABLE OF ResultSet_Cursor%ROWTYPE;
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
open ResultSet_Cursor;
  LOOP
    FETCH ResultSet_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (values_rec(i).DATA_PARTNER_ID,values_rec(i).DOMAIN_NAME,values_rec(i).SOURCE_ID,values_rec(i).CREATE_DATE,values_rec(i).TARGET_DOMAIN_ID,values_rec(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close ResultSet_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_DM_PCORNET_DEMOGRAPHIC;
