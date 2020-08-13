/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_DM_PCORNET_OBS_CLIN

Description : insert to N3CDS_DOMAIN_MAP

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_DM_PCORNET_OBS_CLIN 
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
CURSOR OC_Cursor IS
SELECT distinct 
DATAPARTNERID AS DATA_PARTNER_ID,
'OBS_CLIN' AS DOMAIN_NAME,
OBSCLINID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
x.target_domain_id, 
x.target_concept_id 
FROM "NATIVE_PCORNET51_CDM"."OBS_CLIN"  
JOIN CDMH_STAGING.PERSON_CLEAN ON OBS_CLIN.PATID=PERSON_CLEAN.PERSON_ID 
                                        AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID   
JOIN CDMH_STAGING.P2o_Code_Xwalk_Standard x ON x.src_code=OBS_CLIN.obsclin_code 
                                        AND X.Cdm_Tbl='OBS_CLIN'  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on OBS_CLIN.OBSCLINID=Mp.Source_Id AND mp.DOMAIN_NAME='OBS_CLIN'  
                                        AND Mp.target_domain_id=x.target_domain_id 
                                        AND mp.target_concept_id=x.target_concept_id 
                                        AND mp.DATA_PARTNER_ID=DATAPARTNERID   
;
TYPE l_val_cur IS TABLE OF OC_Cursor%ROWTYPE;
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
open OC_Cursor;
  LOOP
    FETCH OC_Cursor bulk collect into values_rec limit 10000;
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
Close OC_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_DM_PCORNET_OBS_CLIN;
