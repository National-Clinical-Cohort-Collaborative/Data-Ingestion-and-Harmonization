
CREATE PROCEDURE CDMH_STAGING.SP_ST_TRINETX_MEDICATION 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) 
AS
/******************************************************
*  Name:      SP_ST_TRINETX_MEDICATION
*   
*  Stored Proc: SP_ST_TRINETX_MEDICATION
*  Description: add instance_num as there may be multiple medication data for the same patient per same encounter for same mediation on the same date. 
*  ROW_NUMBER() OVER (partition by patient_id, encounter_id, rx_code_system, rx_code, rx_description, start_date,
*  mapped_code_system, mapped_code order by patient_id, encounter_id, rx_code_system, rx_code, rx_description, start_date, mapped_code_system, mapped_code) as instance_num
*
Revisions:
     Ver          Date        Author               Description
     0.1         8/11/20      shong, snaredla                Initial version.
*
******************************************************/
COMMIT_LIMIT CONSTANT NUMBER := 10000;
loop_count NUMBER;
insert_rec_count NUMBER;
/**************************************************************
*  Cursor for selecting table
**************************************************************/
CURSOR ResultSet_Cursor IS
SELECT PATIENT_ID,
ENCOUNTER_ID,
RX_CODE_SYSTEM,
RX_CODE,
RX_DESCRIPTION,
ALT_DRUG_CODE_SYS,
ALT_DRUG_CODE,
START_DATE,
ROUTE_OF_ADMINISTRATION,
UNITS_PER_ADMINISTRATION,
FREQUENCY,
BRAND,
STRENGTH,
FORM,
DURATION,
REFILLS,
RX_SOURCE,
INDICATION_CODE_SYSTEM,
INDICATION_CODE,
INDICATION_DESC,
ALT_DRUG_NAME,
CLINICAL_DRUG,
END_DATE,
QTY_DISPENSED,
DOSE_AMOUNT,
DOSE_UNIT,
ORPHAN_FLAG,
ORPHAN_REASON,
MAPPED_CODE_SYSTEM,
MAPPED_CODE
, ROW_NUMBER() OVER (partition by patient_id, encounter_id, rx_code_system, rx_code, rx_description, start_date,
mapped_code_system, mapped_code order by patient_id, encounter_id, rx_code_system, rx_code, rx_description, start_date,
mapped_code_system, mapped_code) as instance_num
from native_trinetx_cdm.medication 
;
TYPE l_val_cur IS TABLE OF ResultSet_Cursor%ROWTYPE;
values_rec l_val_cur;

BEGIN

execute immediate 'truncate table CDMH_STAGING.st_tnx_medication';
commit ;
open ResultSet_Cursor;
  LOOP
    FETCH ResultSet_Cursor bulk collect into values_rec limit 10000;
    EXIT WHEN values_rec.COUNT=0;
BEGIN
   FORALL i IN 1..values_rec.COUNT
	   INSERT INTO CDMH_STAGING.st_tnx_medication (PATIENT_ID,
ENCOUNTER_ID,
RX_CODE_SYSTEM,
RX_CODE,
RX_DESCRIPTION,
ALT_DRUG_CODE_SYS,
ALT_DRUG_CODE,
START_DATE,
ROUTE_OF_ADMINISTRATION,
UNITS_PER_ADMINISTRATION,
FREQUENCY,
BRAND,
STRENGTH,
FORM,
DURATION,
REFILLS,
RX_SOURCE,
INDICATION_CODE_SYSTEM,
INDICATION_CODE,
INDICATION_DESC,
ALT_DRUG_NAME,
CLINICAL_DRUG,
END_DATE,
QTY_DISPENSED,
DOSE_AMOUNT,
DOSE_UNIT,
ORPHAN_FLAG,
ORPHAN_REASON,
MAPPED_CODE_SYSTEM,
MAPPED_CODE,
INSTANCE_NUM,
MEDICATION_ID
)
     VALUES (values_rec(i).PATIENT_ID,values_rec(i).ENCOUNTER_ID,values_rec(i).RX_CODE_SYSTEM,values_rec(i).RX_CODE,values_rec(i).RX_DESCRIPTION,
     values_rec(i).ALT_DRUG_CODE_SYS,
values_rec(i).ALT_DRUG_CODE,
values_rec(i).START_DATE,
values_rec(i).ROUTE_OF_ADMINISTRATION,
values_rec(i).UNITS_PER_ADMINISTRATION,
values_rec(i).FREQUENCY,
values_rec(i).BRAND,
values_rec(i).STRENGTH,
values_rec(i).FORM,
values_rec(i).DURATION,
values_rec(i).REFILLS,
values_rec(i).RX_SOURCE,
values_rec(i).INDICATION_CODE_SYSTEM,
values_rec(i).INDICATION_CODE,
values_rec(i).INDICATION_DESC,
values_rec(i).ALT_DRUG_NAME,
values_rec(i).CLINICAL_DRUG,
values_rec(i).END_DATE,
values_rec(i).QTY_DISPENSED,
values_rec(i).DOSE_AMOUNT,
values_rec(i).DOSE_UNIT,
values_rec(i).ORPHAN_FLAG,
values_rec(i).ORPHAN_REASON,
values_rec(i).MAPPED_CODE_SYSTEM,
values_rec(i).MAPPED_CODE,
values_rec(i).INSTANCE_NUM,
values_rec(i).patient_id ||'|'|| values_rec(i).encounter_id||'|'|| values_rec(i).rx_code_system||'|'|| values_rec(i).rx_code||'|'|| 
        values_rec(i).rx_description||'|'|| values_rec(i).start_date||'|'|| values_rec(i).mapped_code_system||'|'|| values_rec(i).mapped_code||'|'|| values_rec(i).INSTANCE_NUM
     );
        COMMIT;
	END;
         insert_rec_count := insert_rec_count+ values_rec.COUNT;
--         dbms_output.put_line('Number of records inserted during loop = '||insert_rec_count);
END LOOP;
COMMIT;
Close ResultSet_Cursor;

  SP_DM_TRINETX_MEDICATION(
    DATAPARTNERID => DATAPARTNERID,
    MANIFESTID => MANIFESTID,
    RECORDCOUNT => RECORDCOUNT
  );
dbms_output.put_line('Number of records inserted are = '||RECORDCOUNT);
END SP_ST_TRINETX_medication ;
