/************************************************************
project : N3C DI&H
Date: 5/16/2020
Authors: 
Stephanie Hong, Sandeep Naredla, Richard Zhu, Tanner Zhang
Stored Procedure : SP_DM_PCORNET_VITAL

Description : insert to N3CDS_DOMAIN_MAP

*************************************************************/
CREATE PROCEDURE CDMH_STAGING.SP_DM_PCORNET_VITAL 
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
CURSOR ResultSet_Cursor1 IS
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
VITALID AS SOURCE_ID,  
SYSDATE AS CREATE_DATE,
'VITAL' AS DOMAIN_NAME,
'Measurement' AS TARGET_DOMAIN_ID, 
4177340 as TARGET_CONCEPT_ID  
FROM "NATIVE_PCORNET51_CDM"."VITAL"  
JOIN CDMH_STAGING.PERSON_CLEAN ON VITAL.PATID=PERSON_CLEAN.PERSON_ID 
                                  AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID  
                                  AND HT!=0  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on VITAL.VITALID=Mp.Source_Id 
                                  AND mp.DOMAIN_NAME='VITAL'   
                                  AND TARGET_DOMAIN_ID='Measurement' 
                                  AND TARGET_CONCEPT_ID=4177340  
                                  AND mp.DATA_PARTNER_ID=DATAPARTNERID   
WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL;
TYPE l_val_cur1 IS TABLE OF ResultSet_Cursor1%ROWTYPE;
values_rec1 l_val_cur1;

CURSOR ResultSet_Cursor2 IS
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
VITALID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
'VITAL' AS DOMAIN_NAME,
'Measurement' AS TARGET_DOMAIN_ID, 
4099154 as TARGET_CONCEPT_ID  
FROM "NATIVE_PCORNET51_CDM"."VITAL"  
JOIN CDMH_STAGING.PERSON_CLEAN ON VITAL.PATID=PERSON_CLEAN.PERSON_ID 
                                  AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID 
                                  AND WT!=0  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on VITAL.VITALID=Mp.Source_Id 
                                  AND mp.DOMAIN_NAME='VITAL'   
                                  AND TARGET_DOMAIN_ID='Measurement' 
                                  AND TARGET_CONCEPT_ID=4099154  
                                  AND mp.DATA_PARTNER_ID=DATAPARTNERID    
WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL;
TYPE l_val_cur2 IS TABLE OF ResultSet_Cursor2%ROWTYPE;
values_rec2 l_val_cur2;

CURSOR ResultSet_Cursor3 IS  
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
VITALID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
'VITAL' AS DOMAIN_NAME,
'Measurement' AS TARGET_DOMAIN_ID, 
4245997 as TARGET_CONCEPT_ID  
FROM "NATIVE_PCORNET51_CDM"."VITAL"  
JOIN CDMH_STAGING.PERSON_CLEAN ON VITAL.PATID=PERSON_CLEAN.PERSON_ID 
                                    AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID 
                                    AND ORIGINAL_BMI!=0  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on VITAL.VITALID=Mp.Source_Id 
                                    AND mp.DOMAIN_NAME='VITAL'   
                                    AND TARGET_DOMAIN_ID='Measurement' 
                                    AND TARGET_CONCEPT_ID=4245997  
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID    
WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL ;
TYPE l_val_cur3 IS TABLE OF ResultSet_Cursor3%ROWTYPE;
values_rec3 l_val_cur3;

CURSOR ResultSet_Cursor4 IS  
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
VITALID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
'VITAL' AS DOMAIN_NAME,
'Measurement' AS TARGET_DOMAIN_ID,
DIASTOLIC.TARGET_CONCEPT_ID  
FROM "NATIVE_PCORNET51_CDM"."VITAL"  
JOIN CDMH_STAGING.PERSON_CLEAN ON VITAL.PATID=PERSON_CLEAN.PERSON_ID 
                                    AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID 
                                    AND VITAL.DIASTOLIC != 0  
JOIN CDMH_STAGING.P2O_VITAL_TERM_XWALK DIASTOLIC on DIASTOLIC.SRC_CODE=VITAL.BP_POSITION 
                                    AND Diastolic.Src_Cdm_Column='DIASTOLIC_BP_POSITION'  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on VITAL.VITALID=Mp.Source_Id 
                                    AND mp.DOMAIN_NAME='VITAL'                                              
                                    AND mp.TARGET_DOMAIN_ID='Measurement'                                             
                                    AND mp.TARGET_CONCEPT_ID=DIASTOLIC.TARGET_CONCEPT_ID
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID    
WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL;  
TYPE l_val_cur4 IS TABLE OF ResultSet_Cursor4%ROWTYPE;
values_rec4 l_val_cur4;

CURSOR ResultSet_Cursor5 IS   
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
VITALID AS SOURCE_ID, 
SYSDATE AS CREATE_DATE,
'VITAL' AS DOMAIN_NAME,
'Measurement' AS TARGET_DOMAIN_ID, 
SYSTOLIC.TARGET_CONCEPT_ID  
FROM "NATIVE_PCORNET51_CDM"."VITAL"  
JOIN CDMH_STAGING.PERSON_CLEAN ON VITAL.PATID=PERSON_CLEAN.PERSON_ID 
                                    AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID 
                                    AND VITAL.SYSTOLIC != 0  
JOIN CDMH_STAGING.P2O_VITAL_TERM_XWALK SYSTOLIC on SYSTOLIC.SRC_CODE=VITAL.BP_POSITION 
                                    AND SYSTOLIC.Src_Cdm_Column='SYSTOLIC_BP_POSITION'  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on VITAL.VITALID=Mp.Source_Id 
                                    AND mp.DOMAIN_NAME='VITAL'                                              
                                    AND mp.TARGET_DOMAIN_ID='Measurement'                                             
                                    AND mp.TARGET_CONCEPT_ID=SYSTOLIC.TARGET_CONCEPT_ID                                             
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID    
WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL ;
TYPE l_val_cur5 IS TABLE OF ResultSet_Cursor5%ROWTYPE;
values_rec5 l_val_cur5;

CURSOR ResultSet_Cursor6 IS  
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
VITALID AS SOURCE_ID,
SYSDATE AS CREATE_DATE,
'VITAL' AS DOMAIN_NAME,
'Observation' AS TARGET_DOMAIN_ID, 
43054909 AS TARGET_CONCEPT_ID  
FROM "NATIVE_PCORNET51_CDM"."VITAL"  
JOIN CDMH_STAGING.PERSON_CLEAN ON VITAL.PATID=PERSON_CLEAN.PERSON_ID 
                                    AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID 
                                    AND SMOKING IS NOT NULL  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on VITAL.VITALID=Mp.Source_Id 
                                    AND mp.DOMAIN_NAME='VITAL' 
                                    AND TARGET_DOMAIN_ID='Observation'  
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID  
                                    AND mp.TARGET_CONCEPT_ID=43054909  
WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL ; 
TYPE l_val_cur6 IS TABLE OF ResultSet_Cursor6%ROWTYPE;
values_rec6 l_val_cur6;

CURSOR ResultSet_Cursor7 IS  
SELECT DISTINCT 
DATAPARTNERID AS DATA_PARTNER_ID,
VITALID AS SOURCE_ID,
SYSDATE AS CREATE_DATE,
'VITAL' AS DOMAIN_NAME,
'Observation' AS TARGET_DOMAIN_ID, 
36305168 AS TARGET_CONCEPT_ID  
FROM "NATIVE_PCORNET51_CDM"."VITAL"  
JOIN CDMH_STAGING.PERSON_CLEAN ON VITAL.PATID=PERSON_CLEAN.PERSON_ID 
                                    AND PERSON_CLEAN.DATA_PARTNER_ID=DATAPARTNERID 
                                    AND TOBACCO IS NOT NULL  
LEFT JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on VITAL.VITALID=Mp.Source_Id 
                                    AND mp.DOMAIN_NAME='VITAL' AND TARGET_DOMAIN_ID='Observation'  
                                    AND mp.DATA_PARTNER_ID=DATAPARTNERID  
                                    AND mp.TARGET_CONCEPT_ID=36305168  
WHERE mp.N3CDS_DOMAIN_MAP_ID IS NULL
;
TYPE l_val_cur7 IS TABLE OF ResultSet_Cursor7%ROWTYPE;
values_rec7 l_val_cur7;

ht_recordCount number;
wt_recordCount number;
bmi_recordCount number;
diastolic_recordCount number;
systolic_recordCount number;
smoking_recordCount number;
tobacco_recordCount number;

BEGIN

/**************************************************************
_  VARIABLES:
*  loop_count - counts loop iterations for COMMIT_LIMIT
**************************************************************/
   loop_count := 0;
   insert_rec_count := 0;
   ht_recordCount := 0;
   wt_recordCount := 0;
   bmi_recordCount := 0;
   diastolic_recordCount := 0;
   systolic_recordCount := 0;
   smoking_recordCount := 0;
   tobacco_recordCount := 0;
/******************************************************
* Beginning of loop on each record in cursor.
******************************************************/
open ResultSet_Cursor1;
  LOOP
    FETCH ResultSet_Cursor1 bulk collect into values_rec1 limit 10000;
    EXIT WHEN values_rec1.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec1.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (values_rec1(i).DATA_PARTNER_ID,values_rec1(i).DOMAIN_NAME,values_rec1(i).SOURCE_ID,values_rec1(i).CREATE_DATE,values_rec1(i).TARGET_DOMAIN_ID,values_rec1(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         ht_recordCount := ht_recordCount+ values_rec1.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
COMMIT;
Close ResultSet_Cursor1;

open ResultSet_Cursor2;
  LOOP
    FETCH ResultSet_Cursor2 bulk collect into values_rec2 limit 10000;
    EXIT WHEN values_rec2.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec2.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (values_rec2(i).DATA_PARTNER_ID,values_rec2(i).DOMAIN_NAME,values_rec2(i).SOURCE_ID,values_rec2(i).CREATE_DATE,values_rec2(i).TARGET_DOMAIN_ID,values_rec2(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         wt_recordCount := wt_recordCount + values_rec2.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
COMMIT;
Close ResultSet_Cursor2;

open ResultSet_Cursor3;
  LOOP
    FETCH ResultSet_Cursor3 bulk collect into values_rec3 limit 10000;
    EXIT WHEN values_rec3.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec3.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (values_rec3(i).DATA_PARTNER_ID,values_rec3(i).DOMAIN_NAME,values_rec3(i).SOURCE_ID,values_rec3(i).CREATE_DATE,values_rec3(i).TARGET_DOMAIN_ID,values_rec3(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         bmi_recordCount := bmi_recordCount+ values_rec3.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
COMMIT;
Close ResultSet_Cursor3;

open ResultSet_Cursor4;
  LOOP
    FETCH ResultSet_Cursor4 bulk collect into values_rec4 limit 10000;
    EXIT WHEN values_rec4.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec4.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (values_rec4(i).DATA_PARTNER_ID,values_rec4(i).DOMAIN_NAME,values_rec4(i).SOURCE_ID,values_rec4(i).CREATE_DATE,values_rec4(i).TARGET_DOMAIN_ID,values_rec4(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         diastolic_recordCount := diastolic_recordCount+ values_rec4.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
COMMIT;
Close ResultSet_Cursor4;

open ResultSet_Cursor5;
  LOOP
    FETCH ResultSet_Cursor5 bulk collect into values_rec5 limit 10000;
    EXIT WHEN values_rec5.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec5.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (values_rec5(i).DATA_PARTNER_ID,values_rec5(i).DOMAIN_NAME,values_rec5(i).SOURCE_ID,values_rec5(i).CREATE_DATE,values_rec5(i).TARGET_DOMAIN_ID,values_rec5(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         systolic_recordCount := systolic_recordCount+ values_rec5.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
COMMIT;
Close ResultSet_Cursor5;

open ResultSet_Cursor6;
  LOOP
    FETCH ResultSet_Cursor6 bulk collect into values_rec6 limit 10000;
    EXIT WHEN values_rec6.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec6.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (values_rec6(i).DATA_PARTNER_ID,values_rec6(i).DOMAIN_NAME,values_rec6(i).SOURCE_ID,values_rec6(i).CREATE_DATE,values_rec6(i).TARGET_DOMAIN_ID,values_rec6(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         smoking_recordCount := smoking_recordCount+ values_rec6.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
COMMIT;
Close ResultSet_Cursor6;

open ResultSet_Cursor7;
  LOOP
    FETCH ResultSet_Cursor7 bulk collect into values_rec7 limit 10000;
    EXIT WHEN values_rec7.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec7.COUNT
	   INSERT INTO CDMH_STAGING.N3CDS_DOMAIN_MAP (DATA_PARTNER_ID,DOMAIN_NAME,SOURCE_ID,CREATE_DATE,TARGET_DOMAIN_ID,TARGET_CONCEPT_ID)
     VALUES (values_rec7(i).DATA_PARTNER_ID,values_rec7(i).DOMAIN_NAME,values_rec7(i).SOURCE_ID,values_rec7(i).CREATE_DATE,values_rec7(i).TARGET_DOMAIN_ID,values_rec7(i).TARGET_CONCEPT_ID);
        COMMIT;
	END;
         tobacco_recordCount := tobacco_recordCount + values_rec7.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
COMMIT;
Close ResultSet_Cursor7;
RECORDCOUNT:=ht_recordCount+wt_recordCount+bmi_recordCount+diastolic_recordCount+systolic_recordCount+smoking_recordCount+tobacco_recordCount;
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_DM_PCORNET_VITAL;
