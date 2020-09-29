/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_DM_OMOP_DOSE_ERA

Description : insert to N3CDS_DOMAIN_MAP

*************************************************************/
CREATE PROCEDURE              CDMH_STAGING.SP_DM_OMOP_DRUG_ERA 
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
CURSOR Drug_Cursor IS
SELECT   /*+ index(mp,IDX_DOMAIN_MAP_DATAPARTNERID) */ distinct DRUG_ERA_ID 
FROM "NATIVE_OMOP531_CDM"."DRUG_ERA" 
JOIN CDMH_STAGING.PERSON_CLEAN ON DRUG_ERA.PERSON_ID=PERSON_CLEAN.PERSON_ID 
                                AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on DRUG_ERA.DRUG_ERA_ID=Mp.Source_Id 
                                AND mp.DOMAIN_NAME='DRUG_ERA' AND mp.DATA_PARTNER_ID=DATAPARTNERID
                                WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL
                                ;
                                
TYPE l_val_cur IS TABLE OF NATIVE_OMOP531_CDM.DRUG_ERA.DRUG_ERA_ID%TYPE;
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
open Drug_Cursor;
  LOOP
    FETCH Drug_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (DATAPARTNERID,'DRUG_ERA',values_rec(i),sysdate,null,null);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close Drug_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_DM_OMOP_DRUG_ERA;
