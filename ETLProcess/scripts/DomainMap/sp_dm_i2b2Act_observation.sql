/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_DM_I2B2ACT_OBSERVATION

Description : insert to N3CDS_DOMAIN_MAP

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_DM_I2B2ACT_OBSERVATION 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/********************************************************************************************************
     Name:      SP_DM_I2B2ACT_OBSERVATION
     Purpose:    Generate domain map id  for i2b2Act observation fact table. 
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1        7/20/2020     SHONG             Initial Version
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
CURSOR ResultSet_Cursor IS
SELECT distinct 
DATAPARTNERID AS DATA_PARTNER_ID,
'OBSERVATION_FACT' AS DOMAIN_NAME,
OBSERVATION_fact_ID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
x.target_domain_id as target_domain_id, 
x.target_concept_id as target_concept_id
FROM NATIVE_I2B2ACT_CDM.OBSERVATION_FACT ob
JOIN CDMH_STAGING.person_clean pc ON ob.patient_num=pc.PERSON_ID AND pc.data_partner_id=DATAPARTNERID
join CDMH_STAGING.a2o_code_xwalk_standard x on 
         x.src_code_type||':'||x.src_code =ob.concept_cd
--        x.src_code= substr(ob.concept_cd, instr(ob.concept_cd, ':')+1, length(ob.concept_cd)) 
--        and x.src_code_type = substr(concept_cd, 0,instr(concept_cd, ':')-1) --src_code_type, SHong match on both type and code 7/20/20
        and x.cdm_tbl = 'OBSERVATION_FACT'
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on ob.OBSERVATION_FACT_ID=Mp.Source_Id AND mp.DOMAIN_NAME='OBSERVATION_FACT' 
            and mp.target_domain_id = x.target_domain_id 
            and mp.target_concept_id = x.target_concept_id 
            AND mp.DATA_PARTNER_ID=DATAPARTNERID  
WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL
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
     VALUES (values_rec(i).DATA_PARTNER_ID,
     values_rec(i).DOMAIN_NAME,
     values_rec(i).SOURCE_ID,
     values_rec(i).CREATE_DATE,
     values_rec(i).TARGET_DOMAIN_ID,
     values_rec(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
RECORDCOUNT :=insert_rec_count;
COMMIT;
Close ResultSet_Cursor;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);

END SP_DM_I2B2ACT_OBSERVATION;
